from __future__ import annotations

import hashlib
import json
import threading
import uuid
from collections import defaultdict
from typing import Any, Dict, Optional

from .domain import (ConflictError, ensure_role, normalize_severity,
                     require_notice_no, require_number, require_op_id,
                     require_positive_int, require_text)
from .repository import Repository
from .rules import (AUDIT_ROLES, BASIS_FIELD_ROLES, CREATE_ROLES, ENTITY,
                    RECORD_ROLES, STATES, VIEW_ROLES, basis_snapshot,
                    completion_blockers, escalation_required, notice_required,
                    priority_score, response_deadline_hours,
                    role_for_transition, validate_transition)


def _previous_state(target: str) -> str:
    if target in STATES:
        return STATES[STATES.index(target) - 1]
    return "unknown"


def _request_hash(params: Dict[str, Any]) -> str:
    raw = json.dumps(params, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _new_op_id(kind: str) -> str:
    return f"OP-{kind.upper()}-{uuid.uuid4().hex[:16]}"


class Service:
    def __init__(self, repository: Repository):
        self.repository = repository
        self._op_locks: Dict[str, threading.Lock] = defaultdict(threading.Lock)

    def _view(self, role: str) -> None:
        ensure_role(role, VIEW_ROLES)

    def _replayed_result(self, op_id: Optional[str],
                         params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """同号重送且已完成：返回首次存储结果（标记replayed），未命中返回None。

        带params时同时校验请求指纹，同号不同内容直接冲突。
        """
        if op_id is None:
            return None
        existing = self.repository.find_operation(op_id)
        if existing is None:
            return None
        if params is not None and existing["request_hash"] != _request_hash(params):
            raise ConflictError("同一操作号提交了不同请求内容")
        if existing["status"] != "completed":
            return None
        result = dict(existing.get("result") or {})
        result["op_id"] = op_id
        result["replayed"] = True
        return result

    def _submit(self, kind: str, params: Dict[str, Any], item_id: Optional[int],
                actor: str, executor) -> Dict[str, Any]:
        """按操作号运行：已完成沿用首次结果；未完成从检查点恢复；新操作登记后执行。"""
        op_id = require_op_id(params.get("op_id")) or _new_op_id(kind)
        request_hash = _request_hash(params)
        with self._op_locks[op_id]:
            op = self.repository.begin_operation(op_id, kind, item_id, request_hash,
                                                 params, actor)
            if op["status"] == "completed":
                stored = self.repository.get_operation(op_id)
                result = dict(stored.get("result") or {})
                result["op_id"] = op_id
                result["replayed"] = True
                return result
            result = executor(op, params)
            stored = self.repository.complete_operation(
                op_id, result.get("outcome", "applied"), result)
            result = dict(result)
            result["op_id"] = op_id
            result["replayed"] = False
            return result

    # -------------------------------------------------------------- create
    def create_item(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, CREATE_ROLES)
        actor = require_text(actor, "actor", 100)
        params = {
            "title": require_text(payload.get("title"), "title", 200),
            "description": require_text(payload.get("description"), "description"),
            "severity": normalize_severity(payload.get("severity")),
            "quantity": require_number(payload.get("quantity", 0), "quantity"),
            "threshold": require_number(payload.get("threshold", 1), "threshold", 0.000001),
            "weather": payload.get("weather"),
            "external_ref": payload.get("external_ref"),
        }
        if params["weather"] is not None:
            params["weather"] = require_text(params["weather"], "weather", 50)
        if params["external_ref"] is not None:
            params["external_ref"] = require_text(params["external_ref"], "external_ref", 100)
        if payload.get("op_id") is not None:
            params["op_id"] = require_op_id(payload.get("op_id"))
            prior = self._replayed_result(params["op_id"], params)
            if prior is not None:
                body = self.enrich(self.repository.get_item(int(prior["item_id"])))
                body["op_id"] = params["op_id"]
                body["replayed"] = True
                return body

        def execute(op: Dict[str, Any], p: Dict[str, Any]) -> Dict[str, Any]:
            item = self.repository.create_item_op(
                op, p["title"], p["description"], p["severity"], p["quantity"],
                p["threshold"], p["weather"], p["external_ref"], actor)
            # 告警创建即开启批次1，传感器/巡检追加事项与首次推进都挂该批次
            self.repository.ensure_batch_for_op(op, item, basis_snapshot(item))
            if op.get("checkpoint", 0) < 4:
                self.repository.append_audit("create", ENTITY, item["id"], actor, {
                    "op_id": op["op_id"], "title": p["title"],
                    "severity": p["severity"], "quantity": p["quantity"],
                    "basis_version": 1,
                    "priority": priority_score(p["severity"], p["quantity"],
                                               p["threshold"], 0, p["weather"]),
                }, op["op_id"], 4)
                self.repository.audit_written(op["op_id"], 4)
            return {"outcome": "created", "item_id": item["id"]}

        result = self._submit("create", params, None, actor, execute)
        # 与旧接口一致：返回体即告警对象；操作号/重放标记为附加字段
        item_id = result.get("item_id")
        if item_id is None and result.get("item"):
            item_id = result["item"]["id"]
        body = self.enrich(self.repository.get_item(int(item_id)))
        body["op_id"] = result["op_id"]
        body["replayed"] = result["replayed"]
        return body

    # -------------------------------------------------------------- record
    def add_record(self, item_id: int, payload: Dict[str, Any], actor: str,
                   role: str) -> Dict[str, Any]:
        ensure_role(role, RECORD_ROLES)
        actor = require_text(actor, "actor", 100)
        kind = require_text(payload.get("kind"), "kind", 100)
        detail = require_text(payload.get("detail"), "detail")
        status = payload.get("status", "open")
        if status not in ("open", "closed"):
            from .domain import ValidationError
            raise ValidationError("status必须是open或closed")
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        params = {"kind": kind, "detail": detail, "status": status,
                  "external_ref": external_ref}
        replay = None
        if payload.get("op_id") is not None:
            op_id_value = require_op_id(payload.get("op_id"))
            params["op_id"] = op_id_value
            replay = self._replayed_result(op_id_value, params)
        if replay is not None:
            body = dict(replay["record"])
            body["op_id"] = replay["op_id"]
            body["batch_id"] = replay["batch_id"]
            body["replayed"] = True
            return body

        def execute(op: Dict[str, Any], p: Dict[str, Any]) -> Dict[str, Any]:
            item = self.repository.get_item(item_id)
            batch = self.repository.ensure_batch_for_op(
                op, item, basis_snapshot(item))
            rows = self.repository.list_records(item_id)
            record = next((r for r in rows if r["op_id"] == op["op_id"]), None)
            if record is None:
                record = self.repository.add_record_op(
                    op, item_id, p["kind"], p["detail"], p["status"],
                    p["external_ref"], batch["id"])
            if op.get("checkpoint", 0) < 4:
                self.repository.append_audit("record", ENTITY, item_id, actor, {
                    "op_id": op["op_id"], "batch_id": batch["id"],
                    "record_id": record["id"], "kind": p["kind"], "status": p["status"],
                    "basis_version": self.repository.get_item(item_id)["basis_version"],
                }, op["op_id"], 4)
                self.repository.audit_written(op["op_id"], 4)
            return {"outcome": "recorded", "record": record,
                    "batch_id": batch["id"]}

        result = self._submit("record", params, item_id, actor, execute)
        body = dict(result["record"])
        body["op_id"] = result["op_id"]
        body["batch_id"] = result["batch_id"]
        body["replayed"] = result["replayed"]
        return body

    # ------------------------------------------------------------ advance
    def transition(self, item_id: int, target: str, expected_version: int,
                   actor: str, role: str, op_id: Optional[str] = None,
                   basis_version: Optional[int] = None,
                   traffic_notice_no: Optional[str] = None) -> Dict[str, Any]:
        actor = require_text(actor, "actor", 100)
        item = self.repository.get_item(item_id)
        ensure_role(role, role_for_transition(target))
        expected_version = require_positive_int(expected_version, "expected_version")
        if basis_version is not None:
            basis_version = require_positive_int(basis_version, "basis_version")
        explicit_op = op_id is not None
        if explicit_op:
            op_id = require_op_id(op_id)
        # 限行、封闭必须绑定交通通告号（新协议带操作号时强制；旧调用沿用原行为以便平滑迁移）
        if notice_required(target) and explicit_op:
            traffic_notice_no = require_notice_no(traffic_notice_no)
            if traffic_notice_no is None:
                from .domain import ValidationError
                raise ValidationError(f"转换到{target}必须提交traffic_notice_no交通通告号")
        params = {"target": target, "expected_version": expected_version}
        if op_id is not None:
            params["op_id"] = op_id
        if basis_version is not None:
            params["basis_version"] = basis_version
        if traffic_notice_no is not None:
            params["traffic_notice_no"] = traffic_notice_no
        # 同号重送：用规范化后的完整params校验指纹并沿用首次结果（先于状态/版本判断）
        if explicit_op:
            prior = self._replayed_result(op_id, params)
            if prior is not None:
                body = self.enrich(self.repository.get_item(item_id))
                body["op_id"] = op_id
                body["replayed"] = True
                body["outcome"] = prior.get("outcome", "applied")
                body["batch_id"] = prior.get("batch_id")
                if prior.get("snapshot"):
                    body["decision_snapshot"] = prior["snapshot"]
                if prior.get("record"):
                    body["pending_review_record_id"] = prior["record"]["id"]
                return body
        validate_transition(item["status"], target)

        def execute(op: Dict[str, Any], p: Dict[str, Any]) -> Dict[str, Any]:
            fresh = self.repository.get_item(item_id)
            blockers = completion_blockers(
                p["target"], self.repository.open_record_count(item_id))
            if blockers:
                raise ConflictError("；".join(blockers))
            # 同号重送可能已完成决定：批次已决则沿用首次结果
            batches = self.repository.list_batches(item_id)
            decided = next((b for b in batches if b["op_id"] == op["op_id"]
                            and b["status"] == "decided"), None)
            if decided is not None and op.get("checkpoint", 0) >= 5:
                updated = self.repository.get_item(item_id)
                return {"outcome": "advanced", "item": self.enrich(updated),
                        "batch_id": decided["id"], "snapshot": decided["snapshot"],
                        "next_batch_id": (self.repository.open_next_batch(
                            item_id, updated)["id"] if p["target"] != "restored" else None)}
            notice = p.get("traffic_notice_no") or fresh.get("traffic_notice_no")
            batch = self.repository.ensure_batch_for_op(op, fresh, basis_snapshot(fresh, notice))
            my_note = next((r for r in self.repository.list_records(item_id)
                            if r["op_id"] == op["op_id"]), None)
            already_won = (my_note is None and op.get("checkpoint", 0) >= 3
                           and fresh["status"] == p["target"])
            if already_won:
                # 本操作的检查点恢复：状态此前已由本操作推进，直接补齐决定落定
                won, current_version = True, fresh["version"]
            else:
                won, current_version = self.repository.transition_item_op(
                    op["op_id"], item_id, p["target"], p["expected_version"], fresh["status"])
            updated = self.repository.get_item(item_id)
            if not won:
                # 先到者已占用：判断是并发擦肩还是过期版本
                if current_version <= p["expected_version"]:
                    raise ConflictError("版本冲突，请刷新后重试")
                if current_version - p["expected_version"] > 1:
                    raise ConflictError("版本冲突，请刷新后重试")
                detail = (f"后到推进（{fresh['status']}->{p['target']}）与先到操作并发，"
                          f"提交依据版本{p.get('basis_version') or fresh['basis_version']}，"
                          f"交通通告号{notice or '未绑定'}，现场情况待复核")
                note = self.repository.add_deferred_note_op(
                    op, item_id, detail, batch["id"])
                self.repository.append_audit("deferred", ENTITY, item_id, actor, {
                    "op_id": op["op_id"], "batch_id": batch["id"],
                    "record_id": note["id"], "target": p["target"],
                    "expected_version": p["expected_version"],
                    "actual_version": current_version,
                    "traffic_notice_no": notice,
                    "basis_version": updated["basis_version"],
                }, op["op_id"], 4)
                self.repository.audit_written(op["op_id"], 4)
                body = self.enrich(updated)
                body.update({"outcome": "deferred_for_review",
                             "pending_review_record_id": note["id"],
                             "batch_id": batch["id"], "op_id": op["op_id"],
                             "replayed": False})
                return {"outcome": "deferred_for_review", "item": body,
                        "record": note, "batch_id": batch["id"]}
            # 先到者：按当时依据冻结决定快照，后续依据变化不改变已完成决定
            snapshot = basis_snapshot(updated, notice)
            snapshot["submitted_basis_version"] = p.get("basis_version", fresh["basis_version"])
            snapshot["traffic_notice_no"] = notice
            decision = {"from": fresh["status"], "to": p["target"],
                        "actor": actor, "traffic_notice_no": notice,
                        "basis_version": snapshot["basis_version"]}
            self.repository.append_audit("transition", ENTITY, item_id, actor, {
                "op_id": op["op_id"], "batch_id": batch["id"],
                "from": fresh["status"], "to": p["target"],
                "traffic_notice_no": notice,
                "basis_version": snapshot["basis_version"],
                "submitted_basis_version": snapshot["submitted_basis_version"],
                "escalation_required": escalation_required(
                    updated["severity"], updated["quantity"], updated["threshold"]),
            }, op["op_id"], 4)
            self.repository.audit_written(op["op_id"], 4)
            self.repository.decide_batch_op(op["op_id"], batch["id"], snapshot, decision)
            next_batch = None
            if p["target"] != "restored":
                nb = self.repository.open_next_batch(item_id, updated)
                next_batch = nb["id"]
            return {"outcome": "advanced", "item": self.enrich(updated),
                    "batch_id": batch["id"], "snapshot": snapshot,
                    "next_batch_id": next_batch}

        result = self._submit("advance", params, item_id, actor, execute)
        body = dict(result["item"])
        body["outcome"] = result["outcome"]
        body["op_id"] = result["op_id"]
        body["batch_id"] = result["batch_id"]
        body["replayed"] = result["replayed"]
        if result.get("snapshot"):
            body["decision_snapshot"] = result["snapshot"]
        if result.get("next_batch_id"):
            body["next_batch_id"] = result["next_batch_id"]
        if result.get("record"):
            body["pending_review_record_id"] = result["record"]["id"]
        return body

    # --------------------------------------------------------------- basis
    def update_basis(self, item_id: int, payload: Dict[str, Any], actor: str,
                     role: str) -> Dict[str, Any]:
        """监测值、天气或交通通告变化：推进依据版本，未完成批次按新依据重算。"""
        actor = require_text(actor, "actor", 100)
        before = self.repository.get_item(item_id)
        fields: Dict[str, Any] = {}
        if "quantity" in payload:
            ensure_role(role, BASIS_FIELD_ROLES["quantity"])
            fields["quantity"] = require_number(payload["quantity"], "quantity")
        if "threshold" in payload:
            ensure_role(role, BASIS_FIELD_ROLES["threshold"])
            fields["threshold"] = require_number(payload["threshold"], "threshold", 0.000001)
        if "severity" in payload:
            ensure_role(role, BASIS_FIELD_ROLES["severity"])
            fields["severity"] = normalize_severity(payload["severity"])
        if "weather" in payload:
            ensure_role(role, BASIS_FIELD_ROLES["weather"])
            weather = payload["weather"]
            fields["weather"] = None if weather in (None, "") else require_text(weather, "weather", 50)
        if "traffic_notice_no" in payload:
            ensure_role(role, BASIS_FIELD_ROLES["traffic_notice_no"])
            notice = payload["traffic_notice_no"]
            fields["traffic_notice_no"] = None if notice in (None, "") else require_notice_no(notice)
        if not fields:
            from .domain import ValidationError
            raise ValidationError("未提供可更新的依据字段")
        params = dict(fields)
        if payload.get("op_id") is not None:
            params["op_id"] = require_op_id(payload.get("op_id"))
            prior = self._replayed_result(params["op_id"], params)
            if prior is not None:
                body = self.enrich(self.repository.get_item(item_id))
                body["op_id"] = params["op_id"]
                body["replayed"] = True
                body["outcome"] = "basis_updated"
                body["recomputed_batches"] = prior.get("recomputed_batches", [])
                body["basis_version"] = prior.get("basis_version", body["basis_version"])
                return body

        def execute(op: Dict[str, Any], p: Dict[str, Any]) -> Dict[str, Any]:
            projected = dict(before)
            projected.update(fields)
            projected["basis_version"] = before["basis_version"] + 1
            snapshot = basis_snapshot(projected, fields.get(
                "traffic_notice_no", before.get("traffic_notice_no")))
            updated = self.repository.apply_basis_op(
                op["op_id"], item_id, fields, snapshot, before["basis_version"])
            self.repository.append_audit("basis_change", ENTITY, item_id, actor, {
                "op_id": op["op_id"], "fields": fields,
                "from_basis_version": before["basis_version"],
                "to_basis_version": updated["basis_version"],
            }, op["op_id"], 4)
            self.repository.audit_written(op["op_id"], 4)
            open_batches = [b["id"] for b in self.repository.list_batches(item_id)
                            if b["status"] == "open"]
            return {"outcome": "basis_updated", "item": self.enrich(updated),
                    "recomputed_batches": open_batches,
                    "basis_version": updated["basis_version"]}

        result = self._submit("basis", params, item_id, actor, execute)
        body = dict(result["item"])
        body["op_id"] = result["op_id"]
        body["replayed"] = result["replayed"]
        body["outcome"] = "basis_updated"
        body["recomputed_batches"] = result["recomputed_batches"]
        body["basis_version"] = result["basis_version"]
        return body

    def recover_operations(self, role: str, op_id: Optional[str] = None) -> list:
        """写入失败后按操作号从检查点恢复；不指定则恢复全部pending操作。

        恢复属于值班处置，viewer 无权触发；角色权限照旧执行。
        """
        ensure_role(role, set(["sensor_operator", "bridge_engineer", "traffic_authority"]))
        results = []
        if op_id is not None:
            target = [self.repository.get_operation(op_id)]
        else:
            target = self.repository.list_pending_operations()
        for op in target:
            if op["status"] == "completed":
                continue
            with self._op_locks[op["op_id"]]:
                outcome = self._resume(op)
                results.append(outcome)
        return results

    def _resume(self, op: Dict[str, Any]) -> Dict[str, Any]:
        kind = op["kind"]
        params = op["params"]
        item_id = op.get("item_id")
        if kind == "create":
            def execute(run_op, p):
                item = self.repository.get_item(int(run_op["item_id"]))
                self.repository.ensure_batch_for_op(run_op, item, basis_snapshot(item))
                if run_op.get("checkpoint", 0) < 4:
                    self.repository.append_audit("create", ENTITY, item["id"], op["actor"], {
                        "op_id": run_op["op_id"], "title": item["title"],
                        "severity": item["severity"], "quantity": item["quantity"],
                        "basis_version": item["basis_version"],
                        "recovered": True,
                    }, run_op["op_id"], 4)
                    self.repository.audit_written(run_op["op_id"], 4)
                return {"outcome": "created", "item_id": item["id"]}
            result = execute(op, params)
            self.repository.complete_operation(op["op_id"], "created", result)
            return {"op_id": op["op_id"], "outcome": "created", "recovered": True,
                    "item_id": result["item_id"]}
        if kind == "record":
            def execute(run_op, p):
                return self.add_record_resume(run_op, p)
            result = execute(op, params)
            self.repository.complete_operation(op["op_id"], "recorded", result)
            return {"op_id": op["op_id"], "outcome": "recorded", "recovered": True}
        if kind == "advance":
            result = self.advance_resume(op, params)
            self.repository.complete_operation(
                op["op_id"], result.get("outcome", "advanced"), result)
            return {"op_id": op["op_id"], "outcome": result.get("outcome"), "recovered": True}
        if kind == "basis":
            result = self.basis_resume(op, params)
            self.repository.complete_operation(op["op_id"], "basis_updated", result)
            return {"op_id": op["op_id"], "outcome": "basis_updated", "recovered": True}
        return {"op_id": op["op_id"], "outcome": "unknown", "recovered": False}

    def add_record_resume(self, op: Dict[str, Any], p: Dict[str, Any]) -> Dict[str, Any]:
        item_id = int(op["item_id"])
        item = self.repository.get_item(item_id)
        batch = self.repository.ensure_batch_for_op(op, item, basis_snapshot(item))
        rows = self.repository.list_records(item_id)
        record = next((r for r in rows if r["op_id"] == op["op_id"]), None)
        if record is None:
            record = self.repository.add_record_op(
                op, item_id, p["kind"], p["detail"], p["status"],
                p["external_ref"], batch["id"])
        if op.get("checkpoint", 0) < 4:
            self.repository.append_audit("record", ENTITY, item_id, op["actor"], {
                "op_id": op["op_id"], "batch_id": batch["id"],
                "record_id": record["id"], "kind": p["kind"], "status": p["status"],
                "basis_version": item["basis_version"], "recovered": True,
            }, op["op_id"], 4)
            self.repository.audit_written(op["op_id"], 4)
        return {"outcome": "recorded", "record": record, "batch_id": batch["id"]}

    def advance_resume(self, op: Dict[str, Any], p: Dict[str, Any]) -> Dict[str, Any]:
        item_id = int(op["item_id"])
        fresh = self.repository.get_item(item_id)
        # 检查点3及以后：决定（或延期记录）已经落库，只需补齐审计/批次
        note_rows = [r for r in self.repository.list_records(item_id)
                     if r["op_id"] == op["op_id"]]
        if note_rows and note_rows[0]["status"] == "pending_review":
            if op.get("checkpoint", 0) < 4:
                self.repository.append_audit("deferred", ENTITY, item_id, op["actor"], {
                    "op_id": op["op_id"], "record_id": note_rows[0]["id"],
                    "target": p["target"], "recovered": True,
                    "basis_version": fresh["basis_version"],
                }, op["op_id"], 4)
                self.repository.audit_written(op["op_id"], 4)
            return {"outcome": "deferred_for_review", "item": self.enrich(fresh),
                    "record": note_rows[0],
                    "batch_id": op.get("batch_id")}
        decided = next((b for b in self.repository.list_batches(item_id)
                        if b["op_id"] == op["op_id"] and b["status"] == "decided"), None)
        if decided is not None:
            next_id = None
            if p["target"] != "restored":
                next_id = self.repository.open_next_batch(item_id, fresh)["id"]
            return {"outcome": "advanced", "item": self.enrich(fresh),
                    "batch_id": decided["id"], "snapshot": decided["snapshot"],
                    "next_batch_id": next_id}
        # 检查点3已过但批次尚未落定：本操作此前已赢得占用（崩溃在3/4之间），补齐决定
        if op.get("checkpoint", 0) >= 3 and fresh["status"] == p["target"]:
            notice = p.get("traffic_notice_no") or fresh.get("traffic_notice_no")
            batch = self.repository.ensure_batch_for_op(op, fresh, basis_snapshot(fresh, notice))
            snapshot = basis_snapshot(fresh, notice)
            snapshot["submitted_basis_version"] = p.get("basis_version", fresh["basis_version"])
            decision = {"from": _previous_state(p["target"]), "to": p["target"],
                        "actor": op["actor"], "traffic_notice_no": notice,
                        "basis_version": snapshot["basis_version"], "recovered": True}
            if op.get("checkpoint", 0) < 4:
                self.repository.append_audit("transition", ENTITY, item_id, op["actor"], {
                    "op_id": op["op_id"], "batch_id": batch["id"],
                    "to": p["target"], "recovered": True, "traffic_notice_no": notice,
                    "basis_version": snapshot["basis_version"],
                }, op["op_id"], 4)
                self.repository.audit_written(op["op_id"], 4)
            self.repository.decide_batch_op(op["op_id"], batch["id"], snapshot, decision)
            next_id = None
            if p["target"] != "restored":
                next_id = self.repository.open_next_batch(item_id, fresh)["id"]
            return {"outcome": "advanced", "item": self.enrich(fresh),
                    "batch_id": batch["id"], "snapshot": snapshot,
                    "next_batch_id": next_id}
        # 检查点3之前：状态尚未推进，重新执行占用（原提交继续有效）
        notice = p.get("traffic_notice_no") or fresh.get("traffic_notice_no")
        batch = self.repository.ensure_batch_for_op(op, fresh, basis_snapshot(fresh, notice))
        won, current_version = self.repository.transition_item_op(
            op["op_id"], item_id, p["target"], p["expected_version"], fresh["status"])
        updated = self.repository.get_item(item_id)
        if not won:
            note = self.repository.add_deferred_note_op(
                op, item_id, "恢复时发现先到操作已占用，现场记录待复核", batch["id"])
            if op.get("checkpoint", 0) < 4:
                self.repository.append_audit("deferred", ENTITY, item_id, op["actor"], {
                    "op_id": op["op_id"], "record_id": note["id"], "recovered": True,
                    "target": p["target"], "basis_version": updated["basis_version"],
                }, op["op_id"], 4)
                self.repository.audit_written(op["op_id"], 4)
            return {"outcome": "deferred_for_review", "item": self.enrich(updated),
                    "record": note, "batch_id": batch["id"]}
        snapshot = basis_snapshot(updated, notice)
        snapshot["submitted_basis_version"] = p.get("basis_version", fresh["basis_version"])
        decision = {"from": fresh["status"], "to": p["target"], "actor": op["actor"],
                    "traffic_notice_no": notice, "basis_version": snapshot["basis_version"]}
        if op.get("checkpoint", 0) < 4:
            self.repository.append_audit("transition", ENTITY, item_id, op["actor"], {
                "op_id": op["op_id"], "batch_id": batch["id"], "from": fresh["status"],
                "to": p["target"], "recovered": True, "traffic_notice_no": notice,
                "basis_version": snapshot["basis_version"],
            }, op["op_id"], 4)
            self.repository.audit_written(op["op_id"], 4)
        self.repository.decide_batch_op(op["op_id"], batch["id"], snapshot, decision)
        next_id = None
        if p["target"] != "restored":
            next_id = self.repository.open_next_batch(item_id, updated)["id"]
        return {"outcome": "advanced", "item": self.enrich(updated),
                "batch_id": batch["id"], "snapshot": snapshot,
                "next_batch_id": next_id}

    def basis_resume(self, op: Dict[str, Any], p: Dict[str, Any]) -> Dict[str, Any]:
        item_id = int(op["item_id"])
        updated = self.repository.get_item(item_id)
        if op.get("checkpoint", 0) < 4:
            self.repository.append_audit("basis_change", ENTITY, item_id, op["actor"], {
                "op_id": op["op_id"], "fields": {k: v for k, v in p.items() if k != "op_id"},
                "recovered": True, "basis_version": updated["basis_version"],
            }, op["op_id"], 4)
            self.repository.audit_written(op["op_id"], 4)
        open_batches = [b["id"] for b in self.repository.list_batches(item_id)
                        if b["status"] == "open"]
        return {"outcome": "basis_updated", "item": self.enrich(updated),
                "recomputed_batches": open_batches,
                "basis_version": updated["basis_version"]}

    # ------------------------------------------------------------ review
    def review_pending_record(self, item_id: int, record_id: int,
                              payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, set(['bridge_engineer', 'traffic_authority']))
        actor = require_text(actor, "actor", 100)
        decision = payload.get("decision")
        if decision not in ("accepted", "dismissed"):
            from .domain import ValidationError
            raise ValidationError("decision必须是accepted或dismissed")
        record = self.repository.get_record(record_id)
        if record["item_id"] != item_id:
            from .domain import NotFoundError
            raise NotFoundError("记录不存在")
        # 采纳：现场记录转为已关闭事项；驳回：同样关闭，备注说明（仅工程师可采纳并继续推进）
        updated = self.repository.review_record(record_id, "closed")
        self.repository.append_audit("review", ENTITY, item_id, actor, {
            "record_id": record_id, "decision": decision,
            "note": require_text(payload.get("note", "复核完成"), "note", 2000),
        })
        return updated

    # ------------------------------------------------------------- reads
    def get_item(self, item_id: int, role: str) -> Dict[str, Any]:
        self._view(role)
        return self.enrich(self.repository.get_item(item_id))

    def list_items(self, role: str, status: Optional[str] = None) -> list:
        self._view(role)
        return [self.enrich(item) for item in self.repository.list_items(status)]

    def list_records(self, item_id: int, role: str) -> list:
        self._view(role)
        return self.repository.list_records(item_id)

    def list_batches(self, item_id: int, role: str) -> list:
        self._view(role)
        return self.repository.list_batches(item_id)

    def get_operation(self, op_id: str, role: str) -> Dict[str, Any]:
        self._view(role)
        return self.repository.get_operation(op_id)

    def audit(self, role: str, item_id: Optional[int] = None) -> list:
        ensure_role(role, AUDIT_ROLES)
        return self.repository.list_audit(item_id)

    @staticmethod
    def enrich(item: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(item)
        result["priority"] = priority_score(
            item["severity"], item["quantity"], item["threshold"], 0, item.get("weather"))
        result["deadline_hours"] = response_deadline_hours(
            item["severity"], item["quantity"], item["threshold"])
        result["escalation_required"] = escalation_required(
            item["severity"], item["quantity"], item["threshold"])
        return result
