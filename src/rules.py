from __future__ import annotations
from .domain import ConflictError, ValidationError
TITLE='桥梁结构监测与限行决策'; ENTITY='桥梁告警'; ID_PREFIX='BM'
SEVERITIES=['normal', 'watch', 'warning', 'critical']; STATES=['normal', 'warning', 'restricted', 'closed', 'restored']; TRANSITIONS={'normal': ['warning'], 'warning': ['restricted'], 'restricted': ['closed'], 'closed': ['restored'], 'restored': []}; TRANSITION_ROLES={'warning': ['sensor_operator'], 'restricted': ['bridge_engineer'], 'closed': ['traffic_authority'], 'restored': ['bridge_engineer']}
CREATE_ROLES=set(['sensor_operator']); RECORD_ROLES=set(['sensor_operator', 'bridge_engineer']); AUDIT_ROLES=set(['bridge_engineer', 'viewer']); VIEW_ROLES=set(['sensor_operator', 'bridge_engineer', 'traffic_authority', 'viewer'])
# 限行、封闭、恢复：角色匹配后可直接改；预警仍只由传感操作员发起
NOTICE_REQUIRED_TARGETS=set(['restricted', 'closed'])
# 依据字段 -> 允许更新的角色。交通通告号由交通主管部门登记，天气/监测值由传感侧更新，等级由桥梁工程师确认
BASIS_FIELD_ROLES={
    'quantity': set(['sensor_operator']),
    'threshold': set(['sensor_operator']),
    'severity': set(['bridge_engineer']),
    'weather': set(['sensor_operator', 'bridge_engineer', 'traffic_authority']),
    'traffic_notice_no': set(['traffic_authority']),
}
# 推进（限行/封闭）决定需要冻结快照的依据字段
BASIS_SNAPSHOT_FIELDS=('severity', 'quantity', 'threshold', 'weather', 'traffic_notice_no')
SEVERITY_WEIGHT={'normal': 1.0, 'watch': 3.0, 'warning': 6.0, 'critical': 9.0}; DEADLINE_HOURS={'normal': 72, 'watch': 24, 'warning': 8, 'critical': 4}; TERMINAL_STATES=set(['restored'])
def priority_score(severity,quantity=0.0,threshold=1.0,open_records=0,weather=None):
    if severity not in SEVERITY_WEIGHT: raise ValidationError("unknown severity")
    ratio=quantity/threshold if threshold>0 else 1.0
    weather_bump=1.0 if isinstance(weather,str) and weather.strip() in ('typhoon','storm','gale','heavy_rain') else 0.0
    return max(0,min(10,int(round(SEVERITY_WEIGHT[severity]+min(4.0,ratio*4.0)+min(3.0,float(open_records))+weather_bump))))
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
def notice_required(target): return target in NOTICE_REQUIRED_TARGETS
def basis_snapshot(item, traffic_notice_no=None):
    """决定时冻结当时依据，已完成决定后续不受新依据影响。"""
    snapshot={field: item.get(field) for field in BASIS_SNAPSHOT_FIELDS}
    if traffic_notice_no is not None:
        snapshot['traffic_notice_no']=traffic_notice_no
    snapshot['basis_version']=item.get('basis_version', 1)
    snapshot['priority']=priority_score(item.get('severity','normal'), item.get('quantity',0.0),
                                        item.get('threshold',1.0), item.get('_open_records',0),
                                        item.get('weather'))
    return snapshot
