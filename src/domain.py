from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional
class ErrorKind:
    VALIDATION="validation"; NOT_FOUND="not_found"; FORBIDDEN="forbidden"; CONFLICT="conflict"
class DomainError(Exception):
    kind=ErrorKind.VALIDATION
    def __init__(self,message): super().__init__(message); self.message=message
class ValidationError(DomainError): kind=ErrorKind.VALIDATION
class NotFoundError(DomainError): kind=ErrorKind.NOT_FOUND
class PermissionDenied(DomainError): kind=ErrorKind.FORBIDDEN
class ConflictError(DomainError): kind=ErrorKind.CONFLICT
SEVERITIES=['normal', 'watch', 'warning', 'critical']; STATES=['normal', 'warning', 'restricted', 'closed', 'restored']; ROLES=['sensor_operator', 'bridge_engineer', 'traffic_authority', 'viewer']
# 处置记录状态：open 待处置事项；closed 已关闭；pending_review 并发后到者留下的现场记录，待复核
RECORD_STATUSES=['open', 'closed', 'pending_review']
# 批次状态：open 进行中（依据变化时重算）；decided 已作出决定（冻结快照，保留当时依据）
BATCH_STATUSES=['open', 'decided']
# 操作检查点步骤编号（写入失败后按操作号从该点恢复）
CHECKPOINT_STARTED=0
CHECKPOINT_ITEM_WRITTEN=1
CHECKPOINT_BATCH_READY=2
CHECKPOINT_RECORD_WRITTEN=3
CHECKPOINT_AUDIT_WRITTEN=4
CHECKPOINT_STATUS_UPDATED=5
CHECKPOINT_SNAPSHOT_WRITTEN=6
CHECKPOINT_RESULT_STORED=7
@dataclass(frozen=True)
class Item:
    id:int; title:str; description:str; severity:str; quantity:float; threshold:float; status:str; version:int; basis_version:int; weather:Optional[str]; external_ref:Optional[str]; created_by:str; created_at:str; updated_at:str
@dataclass(frozen=True)
class Record:
    id:int; item_id:int; kind:str; detail:str; status:str; external_ref:Optional[str]; op_id:Optional[str]; batch_id:Optional[int]; created_by:str; created_at:str
@dataclass(frozen=True)
class AuditEntry:
    id:int; action:str; entity_type:str; entity_id:int; actor:str; detail:Dict[str,Any]; previous_hash:str; entry_hash:str; op_id:Optional[str]; step:Optional[int]; created_at:str
def require_text(value,field,max_length=2000):
    if not isinstance(value,str) or not value.strip(): raise ValidationError(f"{field}不能为空")
    value=value.strip()
    if len(value)>max_length: raise ValidationError(f"{field}不能超过{max_length}个字符")
    return value
def normalize_severity(value):
    if value not in SEVERITIES: raise ValidationError("severity不在允许范围内")
    return value
def require_number(value,field,minimum=0.0):
    if isinstance(value,bool): raise ValidationError(f"{field}必须是数字")
    try: number=float(value)
    except (TypeError,ValueError): raise ValidationError(f"{field}必须是数字")
    if number<minimum: raise ValidationError(f"{field}不能小于{minimum}")
    return number
def require_positive_int(value,field):
    if isinstance(value,bool) or not isinstance(value,int): raise ValidationError(f"{field}必须是正整数")
    if value<1: raise ValidationError(f"{field}必须是正整数")
    return value
def require_op_id(value):
    if value is None: return None
    return require_text(value,"op_id",80)
def require_notice_no(value):
    if value is None: return None
    return require_text(value,"traffic_notice_no",100)
def ensure_role(role,allowed):
    if role not in allowed: raise PermissionDenied("当前角色无权执行该操作")
