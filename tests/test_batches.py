import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from src.domain import ConflictError, PermissionDenied, ValidationError
from src.repository import Repository
from src.rules import STATES
from src.service import Service


class BatchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repository(str(Path(self.tmp.name) / "batch.db"))
        self.service = Service(self.repo)
        self.item = self.service.create_item(
            {"title": "batch item", "description": "recoverable batch", "severity": "warning",
             "quantity": 5, "threshold": 10, "weather": "calm", "external_ref": "B-1",
             "op_id": "B-0"}, "creator", "sensor_operator")

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def _to_warning(self, op_id="B-W"):
        return self.service.transition(
            self.item["id"], "warning", self.item["version"],
            "sensor", "sensor_operator", op_id=op_id)

    def test_same_op_id_reuses_first_result(self):
        before = len(self.service.list_items("viewer"))
        payload = {"title": "idem", "description": "d", "severity": "warning",
                   "quantity": 1, "threshold": 1, "external_ref": "IDEM-1", "op_id": "I-1"}
        first = self.service.create_item(payload, "a", "sensor_operator")
        second = self.service.create_item(payload, "a", "sensor_operator")
        self.assertEqual(first["id"], second["id"])
        self.assertTrue(second["replayed"])
        self.assertEqual(before + 1, len(self.service.list_items("viewer")))
        # 同一操作号提交不同内容必须拒绝
        changed = dict(payload, quantity=9)
        with self.assertRaises(ConflictError):
            self.service.create_item(changed, "a", "sensor_operator")

    def test_notice_required_for_restriction(self):
        w = self._to_warning()
        with self.assertRaises(ValidationError):
            self.service.transition(
                self.item["id"], "restricted", w["version"], "e", "bridge_engineer",
                op_id="B-R", traffic_notice_no=None)
        advanced = self.service.transition(
            self.item["id"], "restricted", w["version"], "e", "bridge_engineer",
            op_id="B-R2", traffic_notice_no="TN-1")
        self.assertEqual("restricted", advanced["status"])
        self.assertEqual("TN-1", advanced["decision_snapshot"]["traffic_notice_no"])
        # 同号重送沿用首次结果，状态版本不变
        replay = self.service.transition(
            self.item["id"], "restricted", w["version"], "e", "bridge_engineer",
            op_id="B-R2", traffic_notice_no="TN-1")
        self.assertTrue(replay["replayed"])
        self.assertEqual(advanced["version"], replay["version"])

    def test_concurrent_advance_winner_and_deferred_note(self):
        w = self._to_warning()
        results = {}

        def duty(op_id, tag):
            results[tag] = self.service.transition(
                self.item["id"], "restricted", w["version"], f"eng-{tag}",
                "bridge_engineer", op_id=op_id, basis_version=1,
                traffic_notice_no=f"TN-{tag}")

        t1 = threading.Thread(target=duty, args=("B-A", "A"))
        t2 = threading.Thread(target=duty, args=("B-B", "B"))
        t1.start(); t2.start(); t1.join(); t2.join()
        outcomes = sorted(results[k]["outcome"] for k in results)
        self.assertEqual(["advanced", "deferred_for_review"], outcomes)
        loser = next(k for k in results if results[k]["outcome"] == "deferred_for_review")
        note_id = results[loser]["pending_review_record_id"]
        note = self.repo.get_record(note_id)
        self.assertEqual("pending_review", note["status"])
        self.assertEqual(0, self.repo.open_record_count(self.item["id"]))
        # 后到者同号重送（同内容）仍是同一条待复核记录
        resent = self.service.transition(
            self.item["id"], "restricted", w["version"], f"eng-{loser}",
            "bridge_engineer", op_id=results[loser]["op_id"], basis_version=1,
            traffic_notice_no=f"TN-{loser}")
        self.assertEqual("deferred_for_review", resent["outcome"])
        self.assertEqual(note_id, resent["pending_review_record_id"])
        # 同号不同内容按冲突处理，防止操作号被挪作他用
        with self.assertRaises(ConflictError):
            self.service.transition(
                self.item["id"], "restricted", w["version"], f"eng-{loser}",
                "bridge_engineer", op_id=results[loser]["op_id"], basis_version=1,
                traffic_notice_no="TN-DIFFERENT")
        # 工程师复核关闭现场记录
        reviewed = self.service.review_pending_record(
            self.item["id"], note_id, {"decision": "accepted", "note": "现场确认"},
            "reviewer", "bridge_engineer")
        self.assertEqual("closed", reviewed["status"])

    def test_stale_version_is_conflict_not_deferred(self):
        w = self._to_warning()
        r = self.service.transition(
            self.item["id"], "restricted", w["version"], "e", "bridge_engineer",
            op_id="B-R", traffic_notice_no="TN-1")
        # expected_version 差距超过一次并发擦肩 -> 冲突
        with self.assertRaises(ConflictError):
            self.service.transition(
                self.item["id"], "closed", 1, "t", "traffic_authority",
                op_id="B-STALE", traffic_notice_no="TN-2")
        self.assertEqual("restricted", r["status"])

    def test_basis_change_recomputes_open_batch_but_freezes_decision(self):
        self._to_warning()
        # 决定前，未完成批次随监测值/天气/通告重算
        b1 = self.service.update_basis(
            self.item["id"], {"quantity": 12, "op_id": "B-B1"}, "s", "sensor_operator")
        self.assertEqual(2, b1["basis_version"])
        self.assertTrue(b1["recomputed_batches"])
        b2 = self.service.update_basis(
            self.item["id"], {"traffic_notice_no": "TN-LATE", "op_id": "B-B2"},
            "t", "traffic_authority")
        open_batch = [b for b in self.service.list_batches(self.item["id"], "viewer")
                      if b["status"] == "open"][-1]
        self.assertEqual(3, open_batch["basis_version"])
        self.assertEqual("TN-LATE", open_batch["snapshot"]["traffic_notice_no"])
        self.assertEqual(2, open_batch["recompute_count"])
        # 按当时依据冻结决定
        decided = self.service.transition(
            self.item["id"], "restricted", b2["version"], "e", "bridge_engineer",
            op_id="B-DEC", basis_version=3, traffic_notice_no="TN-LATE")
        self.assertEqual(3, decided["decision_snapshot"]["basis_version"])
        # 决定后依据再变化，已完成决定快照保留
        self.service.update_basis(
            self.item["id"], {"quantity": 50, "weather": "clear", "op_id": "B-B3"},
            "s", "sensor_operator")
        frozen = [b for b in self.service.list_batches(self.item["id"], "viewer")
                  if b["op_id"] == "B-DEC"][0]
        self.assertEqual("decided", frozen["status"])
        self.assertEqual(12, frozen["snapshot"]["quantity"])
        self.assertEqual("calm", frozen["snapshot"]["weather"])

    def test_basis_change_idempotent(self):
        b1 = self.service.update_basis(
            self.item["id"], {"quantity": 12, "op_id": "B-B1"}, "s", "sensor_operator")
        b1_again = self.service.update_basis(
            self.item["id"], {"quantity": 12, "op_id": "B-B1"}, "s", "sensor_operator")
        self.assertTrue(b1_again["replayed"])
        self.assertEqual(b1["basis_version"], b1_again["basis_version"])

    def test_basis_roles_enforced(self):
        with self.assertRaises(PermissionDenied):
            self.service.update_basis(
                self.item["id"], {"traffic_notice_no": "X"}, "s", "sensor_operator")
        with self.assertRaises(PermissionDenied):
            self.service.update_basis(
                self.item["id"], {"quantity": 9}, "t", "traffic_authority")


class CrashRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "crash.db")
        self.repo = Repository(self.db)
        self.service = Service(self.repo)

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def _reopen(self):
        self.repo.close()
        self.repo = Repository(self.db)
        self.service = Service(self.repo)

    def test_recover_after_item_write(self):
        self.repo.fail_after(1, "K-1")
        with self.assertRaises(RuntimeError):
            self.service.create_item(
                {"title": "K", "description": "d", "severity": "warning",
                 "quantity": 5, "threshold": 10, "external_ref": "K-1", "op_id": "K-1"},
                "a", "sensor_operator")
        self._reopen()
        recovered = self.service.recover_operations("bridge_engineer", "K-1")
        self.assertEqual("created", recovered[0]["outcome"])
        again = self.service.create_item(
            {"title": "K", "description": "d", "severity": "warning",
             "quantity": 5, "threshold": 10, "external_ref": "K-1", "op_id": "K-1"},
            "a", "sensor_operator")
        self.assertTrue(again["replayed"])
        self.assertEqual(1, len(self.service.list_items("viewer")))
        self.assertTrue(self.repo.verify_audit_chain())

    def test_recover_after_status_write_completes_decision(self):
        item = self.service.create_item(
            {"title": "K2", "description": "d", "severity": "warning",
             "quantity": 5, "threshold": 10, "external_ref": "K2", "op_id": "K2-1"},
            "a", "sensor_operator")
        w = self.service.transition(
            item["id"], "warning", 1, "s", "sensor_operator", op_id="K2-W")
        self.repo.fail_after(3, "K2-ADV")
        with self.assertRaises(RuntimeError):
            self.service.transition(
                item["id"], "restricted", w["version"], "e", "bridge_engineer",
                op_id="K2-ADV", traffic_notice_no="TN-K2")
        self._reopen()
        result = self.service.recover_operations("bridge_engineer", "K2-ADV")
        self.assertEqual("advanced", result[0]["outcome"])
        batch = [b for b in self.service.list_batches(item["id"], "viewer")
                 if b["op_id"] == "K2-ADV"][0]
        self.assertEqual("decided", batch["status"])
        self.assertEqual("TN-K2", batch["snapshot"]["traffic_notice_no"])
        # 审计事件不重复
        events = self.service.audit("viewer", item["id"])
        self.assertEqual(1, len([e for e in events
                                 if e["op_id"] == "K2-ADV" and e["action"] == "transition"]))
        self.assertTrue(self.repo.verify_audit_chain())
        # 重放沿用首次结果
        replay = self.service.transition(
            item["id"], "restricted", w["version"], "e", "bridge_engineer",
            op_id="K2-ADV", traffic_notice_no="TN-K2")
        self.assertTrue(replay["replayed"])

    def test_recover_all_pending(self):
        self.repo.fail_after(1, "P-1")
        with self.assertRaises(RuntimeError):
            self.service.create_item(
                {"title": "P", "description": "d", "severity": "watch",
                 "quantity": 1, "threshold": 1, "external_ref": "P-1", "op_id": "P-1"},
                "a", "sensor_operator")
        self._reopen()
        out = self.service.recover_operations("bridge_engineer")
        self.assertEqual("P-1", out[0]["op_id"])
        self.assertEqual(0, len(self.repo.list_pending_operations()))

    def test_recover_basis_after_apply_before_audit(self):
        item = self.service.create_item(
            {"title": "B3", "description": "d", "severity": "watch",
             "quantity": 1, "threshold": 1, "external_ref": "B3", "op_id": "B3-1"},
            "a", "sensor_operator")
        self.repo.fail_after(3, "B3-X")
        with self.assertRaises(RuntimeError):
            self.service.update_basis(
                item["id"], {"quantity": 9, "op_id": "B3-X"}, "s", "sensor_operator")
        self._reopen()
        out = self.service.recover_operations("bridge_engineer", "B3-X")
        self.assertEqual("basis_updated", out[0]["outcome"])
        self.assertEqual(2, self.service.get_item(item["id"], "viewer")["basis_version"])
        events = self.service.audit("viewer", item["id"])
        self.assertEqual(1, len([e for e in events
                                 if e["op_id"] == "B3-X" and e["action"] == "basis_change"]))
        replay = self.service.update_basis(
            item["id"], {"quantity": 9, "op_id": "B3-X"}, "s", "sensor_operator")
        self.assertTrue(replay["replayed"])
        self.assertEqual(2, replay["basis_version"])
        self.assertTrue(self.repo.verify_audit_chain())


class LegacyMigrationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "legacy.db")
        c = sqlite3.connect(self.db)
        c.executescript("""
            CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL,
              description TEXT NOT NULL, severity TEXT, quantity REAL, threshold REAL,
              status TEXT, version INTEGER, external_ref TEXT, created_by TEXT,
              created_at TEXT, updated_at TEXT);
            CREATE TABLE records (id INTEGER PRIMARY KEY AUTOINCREMENT, item_id INTEGER,
              kind TEXT, detail TEXT, status TEXT, external_ref TEXT, created_by TEXT,
              created_at TEXT);
            CREATE TABLE audit_events (id INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT,
              entity_type TEXT, entity_id INTEGER, actor TEXT, detail TEXT,
              previous_hash TEXT, entry_hash TEXT UNIQUE, created_at TEXT);
        """)
        c.execute("INSERT INTO items VALUES(1,'旧告警','历史','warning',8,10,'restricted',3,'OLD',"
                  "'o','2026-09-01T00:00:00+00:00','2026-09-02T00:00:00+00:00')")
        c.execute("INSERT INTO records VALUES(1,1,'sensor','旧读数','closed','E1','o','t')")
        c.execute("INSERT INTO records VALUES(2,1,'inspection','旧巡检','closed',NULL,'o','t')")
        c.execute("INSERT INTO audit_events VALUES(1,'create','桥梁告警',1,'o','{}','GENESIS',?,'t')",
                  ("0" * 64,))
        c.commit(); c.close()

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def test_legacy_records_backfilled_and_usable(self):
        self.repo = Repository(self.db)
        service = Service(self.repo)
        item = service.get_item(1, "viewer")
        self.assertEqual(1, item["basis_version"])
        for record in service.list_records(1, "viewer"):
            self.assertTrue(record["op_id"].startswith("legacy:record:"))
            self.assertIsNotNone(record["batch_id"])
        batch = service.list_batches(1, "viewer")[0]
        self.assertEqual("decided", batch["status"])
        self.assertTrue(batch["snapshot"]["legacy"])
        self.assertTrue(self.repo.verify_audit_chain())
        self.assertEqual(0, len(self.repo.list_pending_operations()))
        # 补全后按新协议继续可用
        b = service.update_basis(1, {"quantity": 20, "op_id": "L-B"}, "s", "sensor_operator")
        closed = service.transition(
            1, "closed", b["version"], "t", "traffic_authority",
            op_id="L-C", basis_version=b["basis_version"], traffic_notice_no="TN-L")
        restored = service.transition(
            1, "restored", closed["version"], "e", "bridge_engineer", op_id="L-R")
        self.assertEqual(STATES[-1], restored["status"])
        # 迁移幂等：再次打开不重复补数据
        batches_before = len(self.repo.list_batches(1))
        self.repo.close()
        self.repo = Repository(self.db)
        self.assertEqual(batches_before, len(self.repo.list_batches(1)))
        self.assertEqual(0, len(self.repo.list_pending_operations()))
        self.assertTrue(self.repo.verify_audit_chain())


if __name__ == "__main__":
    unittest.main()
