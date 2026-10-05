from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError
from .rules import ENTITY, STATES


# 批次状态：pending=进行中（可从检查点恢复），completed=已完成（结果与快照不可变），
# conflict_review=并发冲突后已留下现场记录、待人工复核（同号重送沿用该结果）。
OP_PENDING = "pending"
OP_COMPLETED = "completed"
OP_CONFLICT_REVIEW = "conflict_review"

# 检查点阶段。
ST_BEGIN = "begin"
ST_DONE = "done"
ST_CONFLICT = "conflict"


class Repository:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self._create_schema()

    # ------------------------------------------------------------------ schema
    def _create_schema(self) -> None:
        statuses = ",".join("'" + s.replace("'", "''") + "'" for s in STATES)
        with self.conn:
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
                    weather TEXT,
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
                        CHECK(status IN ('open','closed')),
                    external_ref TEXT,
                    operation_no TEXT,
                    basis_version INTEGER,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, external_ref)
                );
                CREATE INDEX IF NOT EXISTS idx_records_operation_no
                    ON records(operation_no) WHERE operation_no IS NOT NULL;
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER NOT NULL,
                    actor TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    operation_no TEXT,
                    previous_hash TEXT NOT NULL,
                    entry_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_operation_no
                    ON audit_events(operation_no) WHERE operation_no IS NOT NULL;
                CREATE TABLE IF NOT EXISTS operations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    operation_no TEXT NOT NULL UNIQUE,
                    item_id INTEGER,
                    action TEXT NOT NULL,
                    basis_version INTEGER,
                    traffic_notice_no TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    stage TEXT NOT NULL DEFAULT 'begin',
                    request TEXT NOT NULL,
                    basis_snapshot TEXT,
                    result TEXT,
                    error TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_operations_item ON operations(item_id);
                CREATE INDEX IF NOT EXISTS idx_operations_status ON operations(status);
            """)
        self._migrate_legacy()

    def _columns(self, table: str) -> List[str]:
        with self._lock:
            rows = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        return [row["name"] for row in rows]

    def _migrate_legacy(self) -> None:
        """为旧库补全 operation_no / basis_version / weather 列并回填，使旧记录可继续使用。"""
        with self._lock, self.conn:
            item_cols = self._columns("items")
            if "weather" not in item_cols:
                self.conn.execute("ALTER TABLE items ADD COLUMN weather TEXT")
            record_cols = self._columns("records")
            if "operation_no" not in record_cols:
                self.conn.execute("ALTER TABLE records ADD COLUMN operation_no TEXT")
            if "basis_version" not in record_cols:
                self.conn.execute("ALTER TABLE records ADD COLUMN basis_version INTEGER")
            # 旧记录缺少依据版本：回填为其所属告警当前版本，缺失时按 1 处理。
            self.conn.execute(
                """UPDATE records SET basis_version = COALESCE(
                    (SELECT version FROM items WHERE items.id = records.item_id), 1)
                   WHERE basis_version IS NULL""")
            audit_cols = self._columns("audit_events")
            if "operation_no" not in audit_cols:
                self.conn.execute("ALTER TABLE audit_events ADD COLUMN operation_no TEXT")

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _item(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    @staticmethod
    def _operation(row: sqlite3.Row) -> Dict[str, Any]:
        data = dict(row)
        for key in ("request", "basis_snapshot", "result"):
            if data.get(key):
                data[key] = json.loads(data[key])
        return data

    # ------------------------------------------------------------------ items
    def create_item(self, title: str, description: str, severity: str,
                    quantity: float, threshold: float, external_ref: Optional[str],
                    actor: str, weather: Optional[str] = None) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO items(title, description, severity, quantity, threshold,
                       status, version, weather, external_ref, created_by, created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (title, description, severity, quantity, threshold, STATES[0], 1,
                     weather, external_ref, actor, now, now),
                )
                item_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("external_ref已存在") from exc
        return self.get_item(item_id)

    def get_item(self, item_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise NotFoundError("项目不存在")
        return self._item(row)

    def list_items(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM items"
        params: tuple = ()
        if status:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY id DESC"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [self._item(row) for row in rows]

    def update_item_basis(self, item_id: int, severity: str, quantity: float,
                          threshold: float, weather: Optional[str],
                          actor: str) -> Dict[str, Any]:
        """监测值/天气变化：更新依据并自增版本（依据版本）。未完成批次据此重算。"""
        now = utc_now()
        with self._lock, self.conn:
            row = self.conn.execute("SELECT 1 FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("项目不存在")
            self.conn.execute(
                """UPDATE items SET severity=?, quantity=?, threshold=?, weather=?,
                   version=version+1, updated_at=? WHERE id=?""",
                (severity, quantity, threshold, weather, now, item_id),
            )
        return self.get_item(item_id)

    def apply_transition(self, item_id: int, target: str, expected_version: int,
                         actor: str, audit_detail: dict,
                         operation_no: Optional[str]) -> tuple:
        """原子占用版本并在同一事务内挂审计，避免“已占用未挂审计”的中间态。

        返回 ('occupied', item) 先到者占用成功；
             ('conflict', item) 版本已被他人占用，后到者留待复核。
        """
        now = utc_now()
        with self._lock, self.conn:
            row = self.conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("项目不存在")
            current = self._item(row)
            cur = self.conn.execute(
                """UPDATE items SET status=?, version=version+1, updated_at=?
                   WHERE id=? AND version=?""",
                (target, now, item_id, expected_version),
            )
            if cur.rowcount == 0:
                return "conflict", current
            previous_row = self.conn.execute(
                "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            previous = previous_row["entry_hash"] if previous_row else "GENESIS"
            event = make_entry("transition", ENTITY, item_id, actor, audit_detail, previous)
            self.conn.execute(
                """INSERT INTO audit_events(action, entity_type, entity_id, actor, detail,
                   operation_no, previous_hash, entry_hash, created_at)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (event["action"], event["entity_type"], event["entity_id"], event["actor"],
                 json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
                 operation_no, event["previous_hash"], event["entry_hash"],
                 event["created_at"]),
            )
            row = self.conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            return "occupied", self._item(row)

    # ------------------------------------------------------------------ records
    def add_record(self, item_id: int, kind: str, detail: str, status: str,
                   external_ref: Optional[str], actor: str,
                   operation_no: Optional[str] = None,
                   basis_version: Optional[int] = None) -> Dict[str, Any]:
        now = utc_now()
        self.get_item(item_id)
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO records(item_id, kind, detail, status, external_ref,
                       operation_no, basis_version, created_by, created_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (item_id, kind, detail, status, external_ref, operation_no,
                     basis_version, actor, now),
                )
                record_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("记录唯一标识已存在") from exc
        with self._lock:
            row = self.conn.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        return dict(row)

    def list_records(self, item_id: int) -> List[Dict[str, Any]]:
        self.get_item(item_id)
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM records WHERE item_id=? ORDER BY id", (item_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def find_record_by_operation(self, operation_no: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM records WHERE operation_no=? ORDER BY id DESC LIMIT 1",
                (operation_no,),
            ).fetchone()
        return dict(row) if row else None

    def open_record_count(self, item_id: int) -> int:
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM records WHERE item_id=? AND status='open'",
                (item_id,),
            ).fetchone()
        return int(row["n"])

    # ------------------------------------------------------------------ audit
    def append_audit(self, action: str, entity_type: str, entity_id: int,
                     actor: str, detail: dict,
                     operation_no: Optional[str] = None) -> Dict[str, Any]:
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            previous = row["entry_hash"] if row else "GENESIS"
            event = make_entry(action, entity_type, entity_id, actor, detail, previous)
            cur = self.conn.execute(
                """INSERT INTO audit_events(action, entity_type, entity_id, actor, detail,
                   operation_no, previous_hash, entry_hash, created_at)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (event["action"], event["entity_type"], event["entity_id"], event["actor"],
                 json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
                 operation_no, event["previous_hash"], event["entry_hash"],
                 event["created_at"]),
            )
            event_id = int(cur.lastrowid)
        event["id"] = event_id
        event["operation_no"] = operation_no
        return event

    def audit_exists(self, operation_no: str, action: str) -> bool:
        with self._lock:
            row = self.conn.execute(
                "SELECT 1 FROM audit_events WHERE operation_no=? AND action=? LIMIT 1",
                (operation_no, action),
            ).fetchone()
        return row is not None

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

    # ------------------------------------------------------------------ operations
    def get_operation(self, operation_no: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM operations WHERE operation_no=?", (operation_no,)
            ).fetchone()
        return self._operation(row) if row else None

    def insert_operation(self, operation_no: str, item_id: Optional[int], action: str,
                         basis_version: Optional[int], traffic_notice_no: Optional[str],
                         request: Dict[str, Any], actor: str) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                self.conn.execute(
                    """INSERT INTO operations(operation_no, item_id, action, basis_version,
                       traffic_notice_no, status, stage, request, created_by, created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (operation_no, item_id, action, basis_version, traffic_notice_no,
                     OP_PENDING, ST_BEGIN,
                     json.dumps(request, ensure_ascii=False, sort_keys=True),
                     actor, now, now),
                )
        except sqlite3.IntegrityError as exc:
            raise ConflictError("操作号已存在") from exc
        return self.get_operation(operation_no)

    def update_operation(self, operation_no: str, **fields: Any) -> None:
        if not fields:
            return
        if "request" in fields and isinstance(fields["request"], dict):
            fields["request"] = json.dumps(fields["request"], ensure_ascii=False, sort_keys=True)
        if "basis_snapshot" in fields and isinstance(fields["basis_snapshot"], dict):
            fields["basis_snapshot"] = json.dumps(fields["basis_snapshot"], ensure_ascii=False, sort_keys=True)
        if "result" in fields and isinstance(fields["result"], dict):
            fields["result"] = json.dumps(fields["result"], ensure_ascii=False, sort_keys=True)
        fields["updated_at"] = utc_now()
        columns = ", ".join(f"{key}=?" for key in fields)
        values = list(fields.values()) + [operation_no]
        with self._lock, self.conn:
            self.conn.execute(f"UPDATE operations SET {columns} WHERE operation_no=?", values)

    def list_operations(self, item_id: Optional[int] = None,
                        status: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM operations"
        clauses: List[str] = []
        params: List[Any] = []
        if item_id is not None:
            clauses.append("item_id=?")
            params.append(item_id)
        if status is not None:
            clauses.append("status=?")
            params.append(status)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [self._operation(row) for row in rows]

    def close(self) -> None:
        with self._lock:
            self.conn.close()
