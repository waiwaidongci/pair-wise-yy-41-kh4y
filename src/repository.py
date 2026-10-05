from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError
from .rules import ENTITY, ID_PREFIX, STATES

SCHEMA_VERSION = 2


class Repository:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        # 崩溃注入：[(op_id_or_None, checkpoint)]，命中一次即移除，模拟写入中途失败
        self.crash_points: List[tuple] = []
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self._create_schema()

    # ---------------------------------------------------------------- schema
    def _create_schema(self) -> None:
        statuses = ",".join("'" + s.replace("'", "''") + "'" for s in STATES)
        version = int(self.conn.execute("PRAGMA user_version").fetchone()[0])
        has_items = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='items'").fetchone()
        legacy = has_items is not None and self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='batches'").fetchone() is None
        with self.conn:
            if version == 0 and not legacy:
                self.conn.executescript(f"""
                    CREATE TABLE IF NOT EXISTS items (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        title TEXT NOT NULL,
                        description TEXT NOT NULL,
                        severity TEXT NOT NULL,
                        quantity REAL NOT NULL DEFAULT 0,
                        threshold REAL NOT NULL DEFAULT 1,
                        status TEXT NOT NULL CHECK(status IN ({statuses})),
                        version INTEGER NOT NULL DEFAULT 1,
                        basis_version INTEGER NOT NULL DEFAULT 1,
                        weather TEXT,
                        traffic_notice_no TEXT,
                        external_ref TEXT,
                        created_by TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_items_external_ref
                        ON items(external_ref) WHERE external_ref IS NOT NULL;
                    CREATE TABLE IF NOT EXISTS records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                        kind TEXT NOT NULL,
                        detail TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'open'
                            CHECK(status IN ('open','closed','pending_review')),
                        external_ref TEXT,
                        op_id TEXT,
                        batch_id INTEGER REFERENCES batches(id),
                        created_by TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    );
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_records_item_ref
                        ON records(item_id, external_ref) WHERE external_ref IS NOT NULL;
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_records_op_id
                        ON records(op_id) WHERE op_id IS NOT NULL;
                    CREATE TABLE IF NOT EXISTS batches (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                        status TEXT NOT NULL CHECK(status IN ('open','decided')),
                        basis_version INTEGER NOT NULL,
                        submitted_basis_version INTEGER NOT NULL,
                        snapshot_json TEXT NOT NULL,
                        traffic_notice_no TEXT,
                        recompute_count INTEGER NOT NULL DEFAULT 0,
                        op_id TEXT,
                        decision_json TEXT,
                        created_by TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        decided_at TEXT
                    );
                    CREATE INDEX IF NOT EXISTS ix_batches_item ON batches(item_id, status);
                    CREATE TABLE IF NOT EXISTS audit_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        action TEXT NOT NULL,
                        entity_type TEXT NOT NULL,
                        entity_id INTEGER NOT NULL,
                        actor TEXT NOT NULL,
                        detail TEXT NOT NULL,
                        previous_hash TEXT NOT NULL,
                        entry_hash TEXT NOT NULL UNIQUE,
                        op_id TEXT,
                        step INTEGER NOT NULL DEFAULT 0,
                        created_at TEXT NOT NULL
                    );
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_audit_op_step
                        ON audit_events(op_id, step) WHERE op_id IS NOT NULL;
                    CREATE TABLE IF NOT EXISTS operations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        op_id TEXT NOT NULL UNIQUE,
                        kind TEXT NOT NULL,
                        item_id INTEGER REFERENCES items(id),
                        batch_id INTEGER REFERENCES batches(id),
                        request_hash TEXT NOT NULL,
                        params_json TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending'
                            CHECK(status IN ('pending','completed')),
                        checkpoint INTEGER NOT NULL DEFAULT 0,
                        outcome TEXT,
                        result_json TEXT,
                        actor TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                """)
                self.conn.execute("PRAGMA user_version = 2")
            elif legacy or version < SCHEMA_VERSION:
                self._migrate_legacy(version)
                self.conn.execute("PRAGMA user_version = 2")

    def _migrate_legacy(self, version: int) -> None:
        """旧库补全：为缺操作号/依据版本的历史记录回填后继续可用。"""
        cols = {r["name"] for r in self.conn.execute("PRAGMA table_info(items)")}
        if "basis_version" not in cols:
            self.conn.execute("ALTER TABLE items ADD COLUMN basis_version INTEGER NOT NULL DEFAULT 1")
        if "weather" not in cols:
            self.conn.execute("ALTER TABLE items ADD COLUMN weather TEXT")
        if "traffic_notice_no" not in cols:
            self.conn.execute("ALTER TABLE items ADD COLUMN traffic_notice_no TEXT")
        # 批次表先建，records 重建时外键才能引用
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                status TEXT NOT NULL CHECK(status IN ('open','decided')),
                basis_version INTEGER NOT NULL,
                submitted_basis_version INTEGER NOT NULL,
                snapshot_json TEXT NOT NULL,
                traffic_notice_no TEXT,
                recompute_count INTEGER NOT NULL DEFAULT 0,
                op_id TEXT,
                decision_json TEXT,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                decided_at TEXT
            );
            CREATE INDEX IF NOT EXISTS ix_batches_item ON batches(item_id, status);
            CREATE TABLE IF NOT EXISTS operations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                op_id TEXT NOT NULL UNIQUE,
                kind TEXT NOT NULL,
                item_id INTEGER REFERENCES items(id),
                batch_id INTEGER REFERENCES batches(id),
                request_hash TEXT NOT NULL,
                params_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending','completed')),
                checkpoint INTEGER NOT NULL DEFAULT 0,
                outcome TEXT,
                result_json TEXT,
                actor TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
        """)
        rec_cols = {r["name"] for r in self.conn.execute("PRAGMA table_info(records)")}
        if "op_id" not in rec_cols or "pending_review" not in self._status_check("records"):
            self.conn.executescript("""
                ALTER TABLE records RENAME TO records_legacy;
                CREATE TABLE records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open'
                        CHECK(status IN ('open','closed','pending_review')),
                    external_ref TEXT,
                    op_id TEXT,
                    batch_id INTEGER REFERENCES batches(id),
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                INSERT INTO records(id, item_id, kind, detail, status, external_ref,
                                    op_id, created_by, created_at)
                SELECT id, item_id, kind, detail, status, external_ref,
                       'legacy:record:' || id, created_by, created_at
                FROM records_legacy;
                DROP TABLE records_legacy;
                CREATE UNIQUE INDEX ux_records_item_ref
                    ON records(item_id, external_ref) WHERE external_ref IS NOT NULL;
                CREATE UNIQUE INDEX ux_records_op_id
                    ON records(op_id) WHERE op_id IS NOT NULL;
            """)
        audit_cols = {r["name"] for r in self.conn.execute("PRAGMA table_info(audit_events)")}
        if "op_id" not in audit_cols:
            self.conn.execute("ALTER TABLE audit_events ADD COLUMN op_id TEXT")
            self.conn.execute("ALTER TABLE audit_events ADD COLUMN step INTEGER NOT NULL DEFAULT 0")
            self.conn.execute(
                "UPDATE audit_events SET op_id='legacy:audit:' || id, step=0 WHERE op_id IS NULL")
            self.conn.execute(
                "CREATE UNIQUE INDEX ux_audit_op_step ON audit_events(op_id, step) WHERE op_id IS NOT NULL")
        # 旧库的历史事件哈希未必按当前规则计算，统一按链重算（明细内容不变）
        self._rehash_legacy_audit()

    def _rehash_legacy_audit(self) -> None:
        from .audit import calculate_hash
        rows = self.conn.execute("SELECT * FROM audit_events ORDER BY id").fetchall()
        previous = "GENESIS"
        for row in rows:
            payload = {
                "action": row["action"], "entity_type": row["entity_type"],
                "entity_id": row["entity_id"], "actor": row["actor"],
                "detail": json.loads(row["detail"]), "created_at": row["created_at"],
            }
            entry_hash = calculate_hash(previous, payload)
            self.conn.execute(
                "UPDATE audit_events SET previous_hash=?, entry_hash=? WHERE id=?",
                (previous, entry_hash, row["id"]))
            previous = entry_hash
        now = utc_now()
        # 每个历史告警补一个批次：已推进过的按已决定冻结快照，仍在 normal 的保持进行中
        rows = self.conn.execute("SELECT * FROM items").fetchall()
        for row in rows:
            item = dict(row)
            decided = item["status"] != STATES[0]
            snapshot = self._legacy_snapshot(item)
            cur = self.conn.execute(
                """INSERT INTO batches(item_id, status, basis_version, submitted_basis_version,
                   snapshot_json, traffic_notice_no, recompute_count, op_id, decision_json,
                   created_by, created_at, updated_at, decided_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (item["id"], "decided" if decided else "open", item["basis_version"],
                 item["basis_version"], json.dumps(snapshot, ensure_ascii=False, sort_keys=True),
                 item.get("traffic_notice_no"), 0, f"legacy:batch:{item['id']}",
                 json.dumps({"to": item["status"], "legacy": True}, ensure_ascii=False)
                 if decided else None,
                 item["created_by"], now, now, now if decided else None),
            )
            self.conn.execute("UPDATE records SET batch_id=? WHERE item_id=?",
                              (cur.lastrowid, item["id"]))

    @staticmethod
    def _legacy_snapshot(item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "severity": item["severity"], "quantity": item["quantity"],
            "threshold": item["threshold"], "weather": item.get("weather"),
            "traffic_notice_no": item.get("traffic_notice_no"),
            "basis_version": item["basis_version"], "legacy": True,
        }

    def _status_check(self, table: str) -> str:
        ddl = self.conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
        return ddl[0] if ddl else ""

    def fail_after(self, checkpoint: int, op_id: Optional[str] = None) -> None:
        self.crash_points.append((op_id, checkpoint))

    def _maybe_crash(self, op_id: str, checkpoint: int) -> None:
        for needle in ((op_id, checkpoint), (None, checkpoint)):
            if needle in self.crash_points:
                self.crash_points.remove(needle)
                raise RuntimeError(f"模拟写入失败：操作号{op_id}在检查点{checkpoint}后中断")

    # ---------------------------------------------------------------- items
    @staticmethod
    def _row(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    def create_item_op(self, op: Dict[str, Any], title: str, description: str, severity: str,
                       quantity: float, threshold: float, weather: Optional[str],
                       external_ref: Optional[str], actor: str) -> Dict[str, Any]:
        """检查点1：告警落库并回挂操作号，同一事务原子完成。"""
        if op.get("item_id"):
            return self.get_item(int(op["item_id"]))
        with self._lock:
            linked = self.conn.execute(
                "SELECT * FROM items WHERE id=(SELECT item_id FROM operations WHERE op_id=?)",
                (op["op_id"],)).fetchone()
        if linked is not None:
            return dict(linked)
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO items(title, description, severity, quantity, threshold,
                       status, version, basis_version, weather, external_ref, created_by,
                       created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (title, description, severity, quantity, threshold, STATES[0], 1, 1,
                     weather, external_ref, actor, now, now),
                )
                item_id = int(cur.lastrowid)
                self.conn.execute(
                    "UPDATE operations SET item_id=?, checkpoint=? WHERE op_id=?",
                    (item_id, 1, op["op_id"]))
        except sqlite3.IntegrityError as exc:
            raise ConflictError("external_ref已存在") from exc
        self._maybe_crash(op["op_id"], 1)
        return self.get_item(item_id)

    def get_item(self, item_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise NotFoundError("项目不存在")
        return self._row(row)

    def list_items(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM items"
        params: tuple = ()
        if status:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY id DESC"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [self._row(row) for row in rows]

    # ------------------------------------------------------------- operations
    def begin_operation(self, op_id: str, kind: str, item_id: Optional[int], request_hash: str,
                        params: Dict[str, Any], actor: str) -> Dict[str, Any]:
        now = utc_now()
        params_json = json.dumps(params, ensure_ascii=False, sort_keys=True, default=str)
        with self._lock, self.conn:
            existing = self.conn.execute(
                "SELECT * FROM operations WHERE op_id=?", (op_id,)).fetchone()
            if existing is not None:
                row = self._decode_operation(dict(existing))
                if row["request_hash"] != request_hash:
                    raise ConflictError("同一操作号提交了不同请求内容")
                return row
            self.conn.execute(
                """INSERT INTO operations(op_id, kind, item_id, request_hash, params_json,
                   status, checkpoint, actor, created_at, updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (op_id, kind, item_id, request_hash, params_json, "pending", 0, actor, now, now))
        return self.get_operation(op_id)

    def get_operation(self, op_id: str) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM operations WHERE op_id=?", (op_id,)).fetchone()
        if row is None:
            raise NotFoundError("操作号不存在")
        return self._decode_operation(dict(row))

    def find_operation(self, op_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM operations WHERE op_id=?", (op_id,)).fetchone()
        return self._decode_operation(dict(row)) if row is not None else None

    def _decode_operation(self, row: Dict[str, Any]) -> Dict[str, Any]:
        row["params"] = json.loads(row["params_json"])
        if row.get("result_json"):
            row["result"] = json.loads(row["result_json"])
        return row

    def list_pending_operations(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM operations WHERE status='pending' ORDER BY id").fetchall()
        return [self._decode_operation(dict(r)) for r in rows]

    def set_checkpoint(self, op_id: str, checkpoint: int, batch_id: Optional[int] = None) -> None:
        with self._lock, self.conn:
            if batch_id is None:
                self.conn.execute(
                    "UPDATE operations SET checkpoint=?, updated_at=? WHERE op_id=? AND checkpoint<?",
                    (checkpoint, utc_now(), op_id, checkpoint))
            else:
                self.conn.execute(
                    """UPDATE operations SET checkpoint=?, batch_id=?, updated_at=?
                       WHERE op_id=? AND checkpoint<?""",
                    (checkpoint, batch_id, utc_now(), op_id, checkpoint))

    def complete_operation(self, op_id: str, outcome: str, result: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock, self.conn:
            self.conn.execute(
                """UPDATE operations SET status='completed', checkpoint=?, outcome=?,
                   result_json=?, updated_at=? WHERE op_id=?""",
                (7, outcome, json.dumps(result, ensure_ascii=False, sort_keys=True, default=str),
                 utc_now(), op_id))
        return self.get_operation(op_id)

    # --------------------------------------------------------------- batches
    def ensure_batch_for_op(self, op: Dict[str, Any], item: Dict[str, Any],
                            snapshot: Dict[str, Any]) -> Dict[str, Any]:
        """检查点2：取得本告警当前进行中批次；没有则开启。后到操作复用同一批次。"""
        op_id = op["op_id"]
        if op.get("batch_id"):
            return self.get_batch(int(op["batch_id"]))
        now = utc_now()
        with self._lock, self.conn:
            row = self.conn.execute(
                """SELECT * FROM batches WHERE item_id=? AND status='open' ORDER BY id DESC LIMIT 1""",
                (item["id"],)).fetchone()
            if row is None:
                cur = self.conn.execute(
                    """INSERT INTO batches(item_id, status, basis_version,
                       submitted_basis_version, snapshot_json, traffic_notice_no, recompute_count,
                       op_id, created_by, created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (item["id"], "open", item["basis_version"], item["basis_version"],
                     json.dumps(snapshot, ensure_ascii=False, sort_keys=True),
                     item.get("traffic_notice_no"), 0, None, op["actor"], now, now))
                batch_id = int(cur.lastrowid)
            else:
                batch_id = int(row["id"])
            self.conn.execute(
                "UPDATE operations SET batch_id=?, checkpoint=?, updated_at=? WHERE op_id=?",
                (batch_id, 2, now, op_id))
        self._maybe_crash(op_id, 2)
        return self.get_batch(batch_id)

    def get_batch(self, batch_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if row is None:
            raise NotFoundError("批次不存在")
        return self._decode_batch(dict(row))

    def list_batches(self, item_id: int) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM batches WHERE item_id=? ORDER BY id", (item_id,)).fetchall()
        return [self._decode_batch(dict(r)) for r in rows]

    @staticmethod
    def _decode_batch(row: Dict[str, Any]) -> Dict[str, Any]:
        row["snapshot"] = json.loads(row.pop("snapshot_json"))
        if row.get("decision_json"):
            row["decision"] = json.loads(row["decision_json"])
        return row

    def decide_batch_op(self, op_id: str, batch_id: int, snapshot: Dict[str, Any],
                        decision: Dict[str, Any]) -> None:
        """检查点5：决定落定，按当时依据冻结快照。"""
        now = utc_now()
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT status FROM batches WHERE id=?", (batch_id,)).fetchone()
            if row is None:
                raise NotFoundError("批次不存在")
            if row["status"] != "decided":
                self.conn.execute(
                    """UPDATE batches SET status='decided', basis_version=?, snapshot_json=?,
                       decision_json=?, op_id=COALESCE(op_id, ?), decided_at=?, updated_at=?
                       WHERE id=?""",
                    (snapshot["basis_version"],
                     json.dumps(snapshot, ensure_ascii=False, sort_keys=True),
                     json.dumps(decision, ensure_ascii=False, sort_keys=True),
                     op_id, now, now, batch_id))
            self.conn.execute(
                "UPDATE operations SET checkpoint=?, updated_at=? WHERE op_id=?",
                (5, now, op_id))
        self._maybe_crash(op_id, 5)

    def open_next_batch(self, item_id: int, item: Dict[str, Any]) -> Dict[str, Any]:
        """决定落定后开启后续批次承接下一阶段处置（幂等：已有进行中批次则复用）。"""
        now = utc_now()
        with self._lock:
            row = self.conn.execute(
                """SELECT * FROM batches WHERE item_id=? AND status='open'
                   ORDER BY id DESC LIMIT 1""", (item_id,)).fetchone()
            if row is not None:
                return self._decode_batch(dict(row))
            snapshot = {
                "severity": item["severity"], "quantity": item["quantity"],
                "threshold": item["threshold"], "weather": item.get("weather"),
                "traffic_notice_no": item.get("traffic_notice_no"),
                "basis_version": item["basis_version"],
            }
            cur = self.conn.execute(
                """INSERT INTO batches(item_id, status, basis_version,
                   submitted_basis_version, snapshot_json, traffic_notice_no, recompute_count,
                   op_id, created_by, created_at, updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (item_id, "open", item["basis_version"], item["basis_version"],
                 json.dumps(snapshot, ensure_ascii=False, sort_keys=True),
                 item.get("traffic_notice_no"), 0, None, "system", now, now))
            return self.get_batch(int(cur.lastrowid))

    # -------------------------------------------------------------- records
    def add_record_op(self, op: Dict[str, Any], item_id: int, kind: str, detail: str,
                      status: str, external_ref: Optional[str], batch_id: int) -> Dict[str, Any]:
        """检查点3：处置记录落库（带操作号与批次），原子推进检查点。"""
        op_id = op["op_id"]
        with self._lock:
            existing = self.conn.execute(
                "SELECT * FROM records WHERE op_id=?", (op_id,)).fetchone()
        if existing is not None:
            return dict(existing)
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO records(item_id, kind, detail, status, external_ref, op_id,
                       batch_id, created_by, created_at) VALUES(?,?,?,?,?,?,?,?,?)""",
                    (item_id, kind, detail, status, external_ref, op_id, batch_id,
                     op["actor"], now),
                )
                record_id = int(cur.lastrowid)
                self.conn.execute(
                    "UPDATE operations SET checkpoint=?, updated_at=? WHERE op_id=?",
                    (3, now, op_id))
        except sqlite3.IntegrityError as exc:
            raise ConflictError("记录唯一标识已存在") from exc
        self._maybe_crash(op_id, 3)
        return self.get_record(record_id)

    def add_deferred_note_op(self, op: Dict[str, Any], item_id: int, detail: str,
                             batch_id: int) -> Dict[str, Any]:
        """并发后到者：留下 pending_review 现场记录待复核（幂等于操作号）。"""
        return self.add_record_op(op, item_id, "on_site_note", detail, "pending_review",
                                  None, batch_id)

    def get_record(self, record_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        if row is None:
            raise NotFoundError("记录不存在")
        return dict(row)

    def list_records(self, item_id: int) -> List[Dict[str, Any]]:
        self.get_item(item_id)
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM records WHERE item_id=? ORDER BY id", (item_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def open_record_count(self, item_id: int) -> int:
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM records WHERE item_id=? AND status='open'",
                (item_id,),
            ).fetchone()
        return int(row["n"])

    def review_record(self, record_id: int, status: str) -> Dict[str, Any]:
        with self._lock, self.conn:
            cur = self.conn.execute(
                "UPDATE records SET status=? WHERE id=? AND status='pending_review'",
                (status, record_id))
            if cur.rowcount == 0:
                exists = self.conn.execute("SELECT 1 FROM records WHERE id=?", (record_id,)).fetchone()
                if exists is None:
                    raise NotFoundError("记录不存在")
                raise ConflictError("该记录不是待复核状态")
        return self.get_record(record_id)

    # ------------------------------------------------------------- decision
    def transition_item_op(self, op_id: str, item_id: int, target: str,
                           expected_version: int, expected_status: str) -> tuple:
        """检查点3（决定路径）：严格乐观占用，先到者推进；返回(成功, 当前版本)。

        重放幂等由服务层依据操作检查点处理：本方法只在版本与源状态同时匹配时占用。
        """
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """UPDATE items SET status=?, version=version+1, updated_at=?
                   WHERE id=? AND version=? AND status=?""",
                (target, now, item_id, expected_version, expected_status))
            if cur.rowcount == 0:
                row = self.conn.execute(
                    "SELECT version, status FROM items WHERE id=?", (item_id,)).fetchone()
                if row is None:
                    raise NotFoundError("项目不存在")
                return False, int(row["version"])
            self.conn.execute(
                "UPDATE operations SET checkpoint=?, updated_at=? WHERE op_id=?",
                (3, now, op_id))
        self._maybe_crash(op_id, 3)
        return True, expected_version + 1

    def apply_basis_op(self, op_id: str, item_id: int, fields: Dict[str, Any],
                       snapshot: Dict[str, Any], from_basis_version: int) -> Dict[str, Any]:
        """依据更新单事务：推进 basis_version、重算未完成批次、推进检查点（重放不重复推进）。"""
        now = utc_now()
        with self._lock, self.conn:
            current = self.conn.execute(
                "SELECT basis_version FROM items WHERE id=?", (item_id,)).fetchone()
            if current is None:
                raise NotFoundError("项目不存在")
            if int(current["basis_version"]) == from_basis_version:
                assignments = ", ".join(f"{key}=?" for key in fields)
                params = list(fields.values()) + [now, item_id]
                self.conn.execute(
                    f"""UPDATE items SET {assignments}, basis_version=basis_version+1,
                        version=version+1, updated_at=? WHERE id=?""", params)
                self.conn.execute(
                    """UPDATE batches SET basis_version=(SELECT basis_version FROM items WHERE id=?),
                       snapshot_json=?, recompute_count=recompute_count+1, updated_at=?
                       WHERE item_id=? AND status='open'""",
                    (item_id, json.dumps(snapshot, ensure_ascii=False, sort_keys=True),
                     now, item_id))
            self.conn.execute(
                "UPDATE operations SET checkpoint=?, updated_at=? WHERE op_id=?",
                (3, now, op_id))
        self._maybe_crash(op_id, 3)
        return self.get_item(item_id)

    # ---------------------------------------------------------------- audit
    def append_audit(self, action: str, entity_type: str, entity_id: int, actor: str,
                     detail: dict, op_id: Optional[str] = None, step: int = 0) -> Dict[str, Any]:
        with self._lock, self.conn:
            if op_id is not None:
                dup = self.conn.execute(
                    "SELECT * FROM audit_events WHERE op_id=? AND step=?", (op_id, step)).fetchone()
                if dup is not None:
                    event = dict(dup)
                    event["detail"] = json.loads(event["detail"])
                    return event
            row = self.conn.execute(
                "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            previous = row["entry_hash"] if row else "GENESIS"
            event = make_entry(action, entity_type, entity_id, actor, detail, previous)
            self.conn.execute(
                """INSERT INTO audit_events(action, entity_type, entity_id, actor, detail,
                   previous_hash, entry_hash, op_id, step, created_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (event["action"], event["entity_type"], event["entity_id"], event["actor"],
                 json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
                 event["previous_hash"], event["entry_hash"], op_id, step, event["created_at"]),
            )
            event_id = int(self.conn.execute(
                "SELECT id FROM audit_events WHERE entry_hash=?", (event["entry_hash"],)).fetchone()[0])
        event["id"] = event_id
        event["op_id"] = op_id
        event["step"] = step
        return event

    def audit_written(self, op_id: str, checkpoint: int = 4) -> None:
        self.set_checkpoint(op_id, checkpoint)

    def list_audit(self, entity_id: Optional[int] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM audit_events"
        params: tuple = ()
        if entity_id is not None:
            sql += " WHERE entity_id=?"
            params = (entity_id,)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["detail"] = json.loads(item["detail"])
            result.append(item)
        return result

    def verify_audit_chain(self) -> bool:
        from .audit import calculate_hash
        with self._lock:
            rows = self.conn.execute("SELECT * FROM audit_events ORDER BY id").fetchall()
        previous = "GENESIS"
        for row in rows:
            if row["previous_hash"] != previous:
                return False
            payload = {
                "action": row["action"], "entity_type": row["entity_type"],
                "entity_id": row["entity_id"], "actor": row["actor"],
                "detail": json.loads(row["detail"]), "created_at": row["created_at"],
            }
            if calculate_hash(previous, payload) != row["entry_hash"]:
                return False
            previous = row["entry_hash"]
        return True

    def close(self) -> None:
        with self._lock:
            self.conn.close()
