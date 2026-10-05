from __future__ import annotations
from .domain import ConflictError, ValidationError
TITLE='桥梁结构监测与限行决策'; ENTITY='桥梁告警'; ID_PREFIX='BM'
SEVERITIES=['normal', 'watch', 'warning', 'critical']; STATES=['normal', 'warning', 'restricted', 'closed', 'restored']; TRANSITIONS={'normal': ['warning'], 'warning': ['restricted'], 'restricted': ['closed'], 'closed': ['restored'], 'restored': []}; TRANSITION_ROLES={'warning': ['sensor_operator'], 'restricted': ['bridge_engineer'], 'closed': ['traffic_authority'], 'restored': ['bridge_engineer']}
CREATE_ROLES=set(['sensor_operator']); RECORD_ROLES=set(['sensor_operator', 'bridge_engineer']); AUDIT_ROLES=set(['bridge_engineer', 'viewer']); VIEW_ROLES=set(['sensor_operator', 'bridge_engineer', 'traffic_authority', 'viewer'])
SEVERITY_WEIGHT={'normal': 1.0, 'watch': 3.0, 'warning': 6.0, 'critical': 9.0}; DEADLINE_HOURS={'normal': 72, 'watch': 24, 'warning': 8, 'critical': 4}; TERMINAL_STATES=set(['restored'])
# 限行、封闭决策必须绑定交通通告记录；恢复不要求。
TRAFFIC_NOTICE_TARGETS=set(['restricted', 'closed'])
# 并发推进失败时留下的现场记录种类。
SCENE_RECORD_KIND='conflict_review'
def priority_score(severity,quantity=0.0,threshold=1.0,open_records=0):
    if severity not in SEVERITY_WEIGHT: raise ValidationError("unknown severity")
    ratio=quantity/threshold if threshold>0 else 1.0
    return max(0,min(10,int(round(SEVERITY_WEIGHT[severity]+min(4.0,ratio*4.0)+min(3.0,float(open_records))))))
def response_deadline_hours(severity,quantity=0.0,threshold=1.0):
    if severity not in DEADLINE_HOURS: raise ValidationError("unknown severity")
    ratio=quantity/threshold if threshold>0 else 1.0
    return max(1,int(DEADLINE_HOURS[severity]/max(1.0,ratio)))
def escalation_required(severity,quantity=0.0,threshold=1.0):
    return severity==SEVERITIES[-1] or (threshold>0 and quantity>=threshold)
def can_transition(current,target): return target in TRANSITIONS.get(current,[])
def validate_transition(current,target):
    if current not in STATES or target not in STATES: raise ValidationError("未知状态")
    if not can_transition(current,target): raise ConflictError(f"不能从{current}转换到{target}")
def completion_blockers(target,open_records): return ["仍有未关闭事项"] if target in TERMINAL_STATES and open_records>0 else []
def role_for_transition(target): return set(TRANSITION_ROLES.get(target,[]))
def requires_traffic_notice(target): return target in TRAFFIC_NOTICE_TARGETS
def derived_fields(severity,quantity,threshold,open_records=0):
    """由监测值、阈值和未关闭事项重算派生指标（优先级、响应期限、是否须升级）。"""
    return {
        "priority": priority_score(severity,quantity,threshold,open_records),
        "deadline_hours": response_deadline_hours(severity,quantity,threshold),
        "escalation_required": escalation_required(severity,quantity,threshold),
    }
def basis_snapshot(item,traffic_notice_no=None,weather=None):
    """生成决策所依据的监测快照；已完成批次永久保留该快照。"""
    snap={
        "basis_version": item.get("version"),
        "severity": item.get("severity"),
        "quantity": item.get("quantity"),
        "threshold": item.get("threshold"),
        "status": item.get("status"),
    }
    snap.update(derived_fields(item["severity"],item["quantity"],item["threshold"]))
    if traffic_notice_no is not None: snap["traffic_notice_no"]=traffic_notice_no
    if weather is not None: snap["weather"]=weather
    return snap
