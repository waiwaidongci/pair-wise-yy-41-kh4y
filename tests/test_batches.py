import json
import tempfile
import unittest
from pathlib import Path

from src.domain import ConflictError
from src.repository import (OP_COMPLETED, OP_CONFLICT_REVIEW, OP_PENDING, ST_BEGIN,
                            ST_DONE, Repository)
from src.rules import TRANSITION_ROLES
from src.service import Service


class BatchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repository(str(Path(self.tmp.name) / "test.db"))
        self.service = Service(self.repo)

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    # ------------------------------------------------------- 同号重送
    def test_create_idempotent_same_operation_no(self):
        payload = {"title": "idem", "description": "d", "severity": "warning",
                   "quantity": 5, "threshold": 10, "operation_no": "OP-C-1"}
        first = self.service.create_item(payload, "creator", "sensor_operator")
        second = self.service.create_item(payload, "creator", "sensor_operator")
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(len(self.service.list_items("viewer")), 1)
        ops = self.repo.list_operations()
        self.assertEqual(len(ops), 1)
        self.assertEqual(ops[0]["status"], OP_COMPLETED)

    def test_record_idempotent_same_operation_no(self):
        item = self.service.create_item(
            {"title": "r", "description": "d", "severity": "warning", "quantity": 5,
             "threshold": 10}, "creator", "sensor_operator")
        payload = {"kind": "evidence", "detail": "same", "status": "closed",
                   "external_ref": "EV-1", "operation_no": "OP-R-1"}
        first = self.service.add_record(item["id"], payload, "recorder", "sensor_operator")
        second = self.service.add_record(item["id"], payload, "recorder", "sensor_operator")
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(len(self.service.list_records(item["id"], "viewer")), 1)

    def test_transition_idempotent_same_operation_no(self):
        item = self.service.create_item(
            {"title": "t", "description": "d", "severity": "warning", "quantity": 5,
             "threshold": 10}, "creator", "sensor_operator")
        kwargs = dict(target="warning", expected_version=1, actor="reviewer",
                      role=TRANSITION_ROLES["warning"][0], operation_no="OP-T-1",
                      traffic_notice_no="TN-1")
        first = self.service.transition(item["id"], **kwargs)
        second = self.service.transition(item["id"], **kwargs)
        self.assertEqual(first["version"], second["version"])
        self.assertEqual(second["status"], "warning")
        # 版本只自增一次、审计只挂一条。
        self.assertEqual(self.repo.get_item(item["id"])["version"], 2)
        audits = [e for e in self.repo.list_audit(item["id"]) if e["action"] == "transition"]
        self.assertEqual(len(audits), 1)
        self.assertTrue(self.repo.verify_audit_chain())

    # ------------------------------------------------------- 并发推进
    def test_concurrent_transition_first_occupies_second_leaves_scene_record(self):
        item = self.service.create_item(
            {"title": "cc", "description": "d", "severity": "warning", "quantity": 5,
             "threshold": 10}, "creator", "sensor_operator")
        first = self.service.transition(
            item["id"], "warning", 1, "reviewer", TRANSITION_ROLES["warning"][0],
            operation_no="OP-CC-1")
        self.assertEqual(first["status"], "warning")
        with self.assertRaises(ConflictError):
            self.service.transition(
                item["id"], "warning", 1, "reviewer", TRANSITION_ROLES["warning"][0],
                operation_no="OP-CC-2")
        # 后到者留下现场记录待复核。
        records = self.service.list_records(item["id"], "viewer")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["kind"], "conflict_review")
        self.assertEqual(records[0]["status"], "open")
        # 同号重送沿用首次（冲突）结果，不重复写现场记录。
        # 同号重送沿用首次（冲突）结果，不重复写现场记录。
        with self.assertRaises(ConflictError):
            self.service.transition(
                item["id"], "warning", 1, "reviewer", TRANSITION_ROLES["warning"][0],
                operation_no="OP-CC-2")
        self.assertEqual(len(self.service.list_records(item["id"], "viewer")), 1)
        op = self.repo.get_operation("OP-CC-2")
        self.assertEqual(op["status"], OP_CONFLICT_REVIEW)
        self.assertTrue(self.repo.verify_audit_chain())

    # ------------------------------------------------------- 快照与重算
    def test_completed_decision_keeps_snapshot_after_basis_change(self):
        item = self.service.create_item(
            {"title": "snap", "description": "d", "severity": "warning", "quantity": 5,
             "threshold": 10}, "creator", "sensor_operator")
        self.service.transition(
            item["id"], "warning", 1, "reviewer", TRANSITION_ROLES["warning"][0],
            operation_no="OP-S-1", traffic_notice_no="TN-OLD")
        # 依据变化（监测值变大）。
        self.service.update_basis(
            item["id"], {"severity": "critical", "quantity": 20, "threshold": 10},
            "reviewer", "bridge_engineer")
        # 同号重送：沿用首次结果与快照，不按新依据重算。
        stored = self.service.transition(
            item["id"], "warning", 1, "reviewer", TRANSITION_ROLES["warning"][0],
            operation_no="OP-S-1", traffic_notice_no="TN-OLD")
        self.assertEqual(stored["priority"], 8)
        op = self.repo.get_operation("OP-S-1")
        self.assertEqual(op["basis_snapshot"]["traffic_notice_no"], "TN-OLD")
        self.assertEqual(op["basis_snapshot"]["priority"], 8)

    def test_pending_batch_recomputed_on_new_basis(self):
        item = self.service.create_item(
            {"title": "pend", "description": "d", "severity": "warning", "quantity": 5,
             "threshold": 10}, "creator", "sensor_operator")
        # 模拟一次已落检查点、未完成的推进批次。
        self.repo.insert_operation(
            "OP-P-1", item["id"], "transition", 1, None,
            request={"target": "warning", "expected_version": 1}, actor="reviewer")
        # 监测值变化，未完成批次按新依据重算。
        self.service.update_basis(
            item["id"], {"severity": "critical", "quantity": 20, "threshold": 10},
            "reviewer", "bridge_engineer")
        op = self.repo.get_operation("OP-P-1")
        self.assertEqual(op["basis_snapshot"]["priority"], 10)
        self.assertEqual(op["basis_version"], 2)
        # 用新依据继续推进，完成时仍使用新依据快照。
        result = self.service.transition(
            item["id"], "warning", 2, "reviewer", TRANSITION_ROLES["warning"][0],
            operation_no="OP-P-1")
        self.assertEqual(result["priority"], 10)
        self.assertEqual(self.repo.get_operation("OP-P-1")["status"], OP_COMPLETED)

    # ------------------------------------------------------- 检查点恢复
    def test_recover_transition_from_begin_checkpoint(self):
        item = self.service.create_item(
            {"title": "rec", "description": "d", "severity": "warning", "quantity": 5,
             "threshold": 10}, "creator", "sensor_operator")
        self.repo.insert_operation(
            "OP-REC-1", item["id"], "transition", 1, None,
            request={"target": "warning", "expected_version": 1}, actor="reviewer")
        result = self.service.transition(
            item["id"], "warning", 1, "reviewer", TRANSITION_ROLES["warning"][0],
            operation_no="OP-REC-1")
        self.assertEqual(result["status"], "warning")
        self.assertEqual(self.repo.get_item(item["id"])["version"], 2)
        op = self.repo.get_operation("OP-REC-1")
        self.assertEqual(op["status"], OP_COMPLETED)
        self.assertEqual(op["stage"], ST_DONE)
        audits = [e for e in self.repo.list_audit(item["id"]) if e["action"] == "transition"]
        self.assertEqual(len(audits), 1)

    def test_recover_transition_from_audited_checkpoint(self):
        item = self.service.create_item(
            {"title": "rec2", "description": "d", "severity": "warning", "quantity": 5,
             "threshold": 10}, "creator", "sensor_operator")
        # 模拟先到者已原子占用并挂审计，但批次尚未收尾（检查点之后崩溃）。
        with self.repo.conn:
            self.repo.conn.execute(
                "UPDATE items SET status='warning', version=2, updated_at=? WHERE id=?",
                (self.repo.conn.execute("SELECT updated_at FROM items WHERE id=?",
                                        (item["id"],)).fetchone()[0], item["id"]))
        self.repo.append_audit("transition", "桥梁告警", item["id"], "reviewer",
                               {"from": "normal", "to": "warning"},
                               operation_no="OP-REC-2")
        self.repo.insert_operation(
            "OP-REC-2", item["id"], "transition", 1, None,
            request={"target": "warning", "expected_version": 1}, actor="reviewer")
        result = self.service.transition(
            item["id"], "warning", 1, "reviewer", TRANSITION_ROLES["warning"][0],
            operation_no="OP-REC-2")
        self.assertEqual(result["status"], "warning")
        self.assertEqual(self.repo.get_item(item["id"])["version"], 2)
        op = self.repo.get_operation("OP-REC-2")
        self.assertEqual(op["status"], OP_COMPLETED)
        audits = [e for e in self.repo.list_audit(item["id"]) if e["action"] == "transition"]
        self.assertEqual(len(audits), 1)

    # ------------------------------------------------------- 旧记录补全
    def test_legacy_records_backfilled_and_usable(self):
        item = self.service.create_item(
            {"title": "leg", "description": "d", "severity": "warning", "quantity": 5,
             "threshold": 10}, "creator", "sensor_operator")
        # 直接写入一条缺少 operation_no / basis_version 的旧记录。
        with self.repo.conn:
            self.repo.conn.execute(
                """INSERT INTO records(item_id, kind, detail, status, external_ref,
                   operation_no, basis_version, created_by, created_at)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (item["id"], "inspection", "legacy", "open", "LEG-1", None, None,
                 "recorder", "2026-01-01T00:00:00+00:00"))
        self.repo._migrate_legacy()
        records = self.service.list_records(item["id"], "viewer")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["basis_version"], item["version"])
        self.assertIsNone(records[0]["operation_no"])
        # 补全后可正常推进（旧记录仍计入未关闭事项）。
        with self.assertRaises(ConflictError):
            self.service.transition(
                item["id"], "restored", 1, "reviewer", TRANSITION_ROLES["restored"][0])

    # ------------------------------------------------------- 交通通告留痕
    def test_traffic_notice_captured_in_snapshot(self):
        item = self.service.create_item(
            {"title": "tn", "description": "d", "severity": "critical", "quantity": 30,
             "threshold": 10}, "creator", "sensor_operator")
        self.service.transition(
            item["id"], "warning", 1, "reviewer", TRANSITION_ROLES["warning"][0],
            operation_no="OP-TN-1", traffic_notice_no="TN-42")
        op = self.repo.get_operation("OP-TN-1")
        self.assertEqual(op["traffic_notice_no"], "TN-42")
        self.assertEqual(op["basis_snapshot"]["traffic_notice_no"], "TN-42")
        # 不带通告号也可直接推进（角色匹配即可）。
        self.service.transition(
            item["id"], "restricted", 2, "reviewer", TRANSITION_ROLES["restricted"][0],
            operation_no="OP-TN-2")
        op2 = self.repo.get_operation("OP-TN-2")
        self.assertIsNone(op2["traffic_notice_no"])
        self.assertNotIn("traffic_notice_no", op2["basis_snapshot"])


if __name__ == "__main__":
    unittest.main()
