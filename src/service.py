from __future__ import annotations

import uuid
from typing import Any, Dict, Optional

from .domain import (ConflictError, ensure_role, normalize_severity, require_number,
                     require_text)
from .repository import (OP_COMPLETED, OP_CONFLICT_REVIEW, OP_PENDING, ST_CONFLICT,
                         ST_DONE, Repository)
from .rules import (AUDIT_ROLES, CREATE_ROLES, ENTITY, RECORD_ROLES, SCENE_RECORD_KIND,
                    VIEW_ROLES, basis_snapshot, completion_blockers, derived_fields,
                    priority_score, requires_traffic_notice, role_for_transition,
                    validate_transition)


class Service:
    def __init__(self, repository: Repository):
        self.repository = repository

    # ------------------------------------------------------------------ helpers
    def _view(self, role: str) -> None:
        ensure_role(role, VIEW_ROLES)

    @staticmethod
    def _auto_operation_no() -> str:
        return "op:" + uuid.uuid4().hex

    @staticmethod
    def _basis_snapshot(item: Dict[str, Any], traffic_notice_no: Optional[str] = None,
                        weather: Optional[str] = None) -> Dict[str, Any]:
        return basis_snapshot(item, traffic_notice_no, weather)

    def _recompute_pending(self, item_id: int) -> None:
        """监测值/天气变化后，未完成批次按新依据重算派生指标（保留各自交通通告号）。"""
        item = self.repository.get_item(item_id)
        for op in self.repository.list_operations(item_id=item_id, status=OP_PENDING):
            snap = self._basis_snapshot(item, op.get("traffic_notice_no"), item.get("weather"))
            self.repository.update_operation(op["operation_no"], basis_snapshot=snap,
                                             basis_version=item["version"])

    def _leave_scene_record(self, item: Dict[str, Any], target: str, expected_version: int,
                            actor: str, operation_no: Optional[str],
                            basis_version: Optional[int],
                            traffic_notice_no: Optional[str]) -> None:
        """并发推进时后到者留下现场记录待复核，并挂一条审计。"""
        detail = {
            "reason": "version_conflict",
            "target": target,
            "expected_version": expected_version,
            "actual_version": item["version"],
            "operation_no": operation_no,
            "basis_version": basis_version if basis_version is not None else item["version"],
            "traffic_notice_no": traffic_notice_no,
            "attempted_by": actor,
        }
        self.repository.add_record(
            item["id"], SCENE_RECORD_KIND,
            "并发推进版本冲突，先到者已占用，待复核：" + str(detail),
            "open",
            external_ref=("conflict:" + operation_no) if operation_no else None,
            actor=actor, operation_no=operation_no,
            basis_version=basis_version if basis_version is not None else item["version"])
        self.repository.append_audit(
            "conflict", ENTITY, item["id"], actor, detail, operation_no=operation_no)

    # ------------------------------------------------------------------ writes
    def create_item(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, CREATE_ROLES)
        actor = require_text(actor, "actor", 100)
        operation_no = payload.get("operation_no")
        if operation_no is not None:
            operation_no = require_text(operation_no, "operation_no", 100)
        op = self.repository.get_operation(operation_no) if operation_no else None
        if op is not None and op["status"] == OP_COMPLETED:
            return op["result"]
        if op is not None and op["status"] == OP_CONFLICT_REVIEW:
            raise ConflictError(op.get("error") or "版本冲突，请刷新后重试")

        title = require_text(payload.get("title"), "title", 200)
        description = require_text(payload.get("description"), "description")
        severity = normalize_severity(payload.get("severity"))
        quantity = require_number(payload.get("quantity", 0), "quantity")
        threshold = require_number(payload.get("threshold", 1), "threshold", 0.000001)
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        weather = payload.get("weather")
        if weather is not None:
            weather = require_text(weather, "weather", 200)
        basis_version = payload.get("basis_version")

        # 恢复：若该批次先前已建出告警，直接复用，避免重复写入。
        if op is not None and op["status"] == OP_PENDING and op.get("item_id") is not None:
            try:
                existing = self.repository.get_item(op["item_id"])
                result = self.enrich(existing)
                self.repository.update_operation(
                    operation_no, status=OP_COMPLETED, stage=ST_DONE, result=result,
                    basis_snapshot=self._basis_snapshot(existing, None, weather),
                    item_id=existing["id"])
                return result
            except Exception:
                pass

        item = self.repository.create_item(title, description, severity, quantity,
                                           threshold, external_ref, actor, weather)
        self.repository.append_audit("create", ENTITY, item["id"], actor, {
            "title": title, "severity": severity, "quantity": quantity,
            "priority": priority_score(severity, quantity, threshold),
        }, operation_no=operation_no)
        result = self.enrich(item)
        if operation_no:
            if op is None:
                self.repository.insert_operation(
                    operation_no, item["id"], "create",
                    basis_version if basis_version is not None else item["version"],
                    None,
                    request={"title": title, "description": description, "severity": severity,
                             "quantity": quantity, "threshold": threshold,
                             "external_ref": external_ref, "weather": weather,
                             "basis_version": basis_version, "actor": actor},
                    actor=actor)
            self.repository.update_operation(
                operation_no, status=OP_COMPLETED, stage=ST_DONE, result=result,
                basis_snapshot=self._basis_snapshot(item, None, weather),
                item_id=item["id"], error=None)
        return result

    def add_record(self, item_id: int, payload: Dict[str, Any], actor: str,
                   role: str) -> Dict[str, Any]:
        ensure_role(role, RECORD_ROLES)
        actor = require_text(actor, "actor", 100)
        operation_no = payload.get("operation_no")
        if operation_no is not None:
            operation_no = require_text(operation_no, "operation_no", 100)
        op = self.repository.get_operation(operation_no) if operation_no else None
        if op is not None and op["status"] == OP_COMPLETED:
            return op["result"]
        if op is not None and op["status"] == OP_CONFLICT_REVIEW:
            raise ConflictError(op.get("error") or "版本冲突，请刷新后重试")

        kind = require_text(payload.get("kind"), "kind", 100)
        detail = require_text(payload.get("detail"), "detail")
        status = payload.get("status", "open")
        if status not in ("open", "closed"):
            raise ValueError("status必须是open或closed")
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        basis_version = payload.get("basis_version")

        # 恢复：同批次先前已写入记录则直接复用。
        if op is not None and op["status"] == OP_PENDING:
            existing = self.repository.find_record_by_operation(operation_no)
            if existing is not None:
                self.repository.update_operation(
                    operation_no, status=OP_COMPLETED, stage=ST_DONE, result=existing,
                    item_id=item_id, error=None)
                return existing

        record = self.repository.add_record(
            item_id, kind, detail, status, external_ref, actor,
            operation_no=operation_no,
            basis_version=basis_version)
        self.repository.append_audit("record", ENTITY, item_id, actor, {
            "record_id": record["id"], "kind": kind, "status": status,
        }, operation_no=operation_no)
        if operation_no:
            if op is None:
                self.repository.insert_operation(
                    operation_no, item_id, "record", basis_version, None,
                    request={"kind": kind, "detail": detail, "status": status,
                             "external_ref": external_ref, "basis_version": basis_version,
                             "actor": actor},
                    actor=actor)
            self.repository.update_operation(
                operation_no, status=OP_COMPLETED, stage=ST_DONE, result=record,
                item_id=item_id, error=None)
        return record

    def transition(self, item_id: int, target: str, expected_version: int,
                   actor: str, role: str,
                   operation_no: Optional[str] = None,
                   basis_version: Optional[int] = None,
                   traffic_notice_no: Optional[str] = None) -> Dict[str, Any]:
        actor = require_text(actor, "actor", 100)
        if operation_no is not None:
            operation_no = require_text(operation_no, "operation_no", 100)

        # 同号重送：沿用首次结果（含并发冲突的复核结果）。
        op = self.repository.get_operation(operation_no) if operation_no else None
        if op is not None and op["status"] == OP_COMPLETED:
            return op["result"]
        if op is not None and op["status"] == OP_CONFLICT_REVIEW:
            raise ConflictError(op.get("error") or "版本冲突，请刷新后重试")

        item = self.repository.get_item(item_id)
        # 恢复：若本批次先前已占用并挂审计（检查点之后），直接收尾，勿重复占用。
        already = bool(operation_no and self.repository.audit_exists(operation_no, "transition"))
        if already:
            updated = self.repository.get_item(item_id)
            result = self.enrich(updated)
            self.repository.update_operation(
                operation_no, status=OP_COMPLETED, stage=ST_DONE, result=result,
                basis_snapshot=self._basis_snapshot(updated, traffic_notice_no,
                                                    updated.get("weather")),
                item_id=item_id, traffic_notice_no=traffic_notice_no, error=None)
            return result

        ensure_role(role, role_for_transition(target))
        if not isinstance(expected_version, int) or expected_version < 1:
            raise ValueError("expected_version必须是正整数")
        if requires_traffic_notice(target) and traffic_notice_no is not None:
            traffic_notice_no = require_text(traffic_notice_no, "traffic_notice_no", 100)
        elif traffic_notice_no is not None:
            traffic_notice_no = require_text(traffic_notice_no, "traffic_notice_no", 100)

        weather = item.get("weather")
        observed_basis = basis_version if basis_version is not None else item["version"]
        snapshot = self._basis_snapshot(item, traffic_notice_no, weather)

        if op is None:
            op = self.repository.insert_operation(
                operation_no or self._auto_operation_no(), item_id, "transition",
                observed_basis, traffic_notice_no,
                request={"target": target, "expected_version": expected_version,
                         "basis_version": basis_version,
                         "traffic_notice_no": traffic_notice_no, "actor": actor},
                actor=actor)
            operation_no = op["operation_no"]

        # 并发推进：版本对不上即冲突（先到者已占用或依据已变），后到者留下现场记录待复核。
        if item["version"] != expected_version:
            self._leave_scene_record(item, target, expected_version, actor,
                                     operation_no, basis_version, traffic_notice_no)
            self.repository.update_operation(
                operation_no, status=OP_CONFLICT_REVIEW, stage=ST_CONFLICT,
                error="版本冲突，告警已被其他值班端推进",
                basis_snapshot=snapshot, item_id=item_id,
                traffic_notice_no=traffic_notice_no)
            raise ConflictError("版本冲突，请刷新后重试")

        validate_transition(item["status"], target)
        blockers = completion_blockers(target, self.repository.open_record_count(item_id))
        if blockers:
            raise ConflictError("；".join(blockers))

        # 版本一致：原子占用并挂审计。
        outcome, current = self.repository.apply_transition(
            item_id, target, expected_version, actor,
            audit_detail={"from": item["status"], "to": target,
                          "basis_version": observed_basis,
                          "traffic_notice_no": traffic_notice_no,
                          "escalation_required": snapshot["escalation_required"]},
            operation_no=operation_no)
        if outcome == "conflict":
            self._leave_scene_record(current, target, expected_version, actor,
                                     operation_no, basis_version, traffic_notice_no)
            self.repository.update_operation(
                operation_no, status=OP_CONFLICT_REVIEW, stage=ST_CONFLICT,
                error="版本冲突，告警已被其他值班端推进",
                basis_snapshot=snapshot, item_id=item_id,
                traffic_notice_no=traffic_notice_no)
            raise ConflictError("版本冲突，请刷新后重试")

        updated = self.repository.get_item(item_id)
        result = self.enrich(updated)
        self.repository.update_operation(
            operation_no, status=OP_COMPLETED, stage=ST_DONE, result=result,
            basis_snapshot=self._basis_snapshot(updated, traffic_notice_no,
                                                updated.get("weather")),
            item_id=item_id, traffic_notice_no=traffic_notice_no, error=None)
        return result

    # ------------------------------------------------------------------ basis
    def update_basis(self, item_id: int, payload: Dict[str, Any], actor: str,
                     role: str) -> Dict[str, Any]:
        """监测值/天气/交通通告变化：更新依据并自增版本，未完成批次按新依据重算。"""
        ensure_role(role, VIEW_ROLES)
        actor = require_text(actor, "actor", 100)
        item = self.repository.get_item(item_id)
        severity = normalize_severity(payload.get("severity", item["severity"]))
        quantity = require_number(payload.get("quantity", item["quantity"]), "quantity")
        threshold = require_number(payload.get("threshold", item["threshold"]),
                                   "threshold", 0.000001)
        weather = payload.get("weather", item.get("weather"))
        if weather is not None:
            weather = require_text(weather, "weather", 200)
        updated = self.repository.update_item_basis(item_id, severity, quantity,
                                                     threshold, weather, actor)
        self.repository.append_audit("basis", ENTITY, item_id, actor, {
            "severity": severity, "quantity": quantity, "threshold": threshold,
            "weather": weather,
            "priority": priority_score(severity, quantity, threshold),
        })
        self._recompute_pending(item_id)
        return self.enrich(updated)

    # ------------------------------------------------------------------ queries
    def get_item(self, item_id: int, role: str) -> Dict[str, Any]:
        self._view(role)
        return self.enrich(self.repository.get_item(item_id))

    def list_items(self, role: str, status: Optional[str] = None) -> list:
        self._view(role)
        return [self.enrich(item) for item in self.repository.list_items(status)]

    def list_records(self, item_id: int, role: str) -> list:
        self._view(role)
        return self.repository.list_records(item_id)

    def list_operations(self, role: str, item_id: Optional[int] = None,
                         status: Optional[str] = None) -> list:
        ensure_role(role, AUDIT_ROLES)
        return self.repository.list_operations(item_id, status)

    def audit(self, role: str, item_id: Optional[int] = None) -> list:
        ensure_role(role, AUDIT_ROLES)
        return self.repository.list_audit(item_id)

    @staticmethod
    def enrich(item: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(item)
        result.update(derived_fields(item["severity"], item["quantity"],
                                     item["threshold"]))
        return result
