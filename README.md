# 桥梁结构监测与限行决策

融合传感、巡检、交通荷载和天气数据，生成限载限行或恢复建议。
所有处置以**可恢复批次（recoverable batch）**串联告警、处置记录和审计事件。

## 可恢复批次模型

- **操作号 `op_id`**：每个提交携带操作号；同号重送沿用首次结果（响应带 `replayed:true`），同号不同内容返回 409。未带操作号的旧调用由系统补号，继续可用。
- **依据版本 `basis_version`**：监测值、天气、交通通告号任一变化即推进。未完成批次按新依据重算快照；已完成决定保留当时快照，事后依据变化不影响决定。
- **交通通告号 `traffic_notice_no`**：限行（restricted）、封闭（closed）必须绑定；恢复（restored）不需要。
- **批次 `batches`**：告警创建即开启批次 1，传感器与巡检追加的事项挂当前进行中批次；每次推进把当前批次按当时依据冻结为 `decided`，并开启下一批次承接后续处置。
- **并发推进**：同一告警两个值班端同时提交时，先到者按 `expected_version` 占用；后到者不覆盖决定，而是留下 `pending_review` 现场记录，经工程师/交通主管复核关闭。
- **写入失败恢复**：每个操作按检查点（0 登记 → 1 告警落库 → 2 批次就绪 → 3 记录/状态落库 → 4 审计落库 → 5 决定冻结 → 7 结果存盘）记录进度；崩溃后按操作号从检查点续跑，审计事件按 `(op_id, step)` 去重，不重复写入。
- **旧库迁移**：首次以新版打开旧库时自动补全 `basis_version`、为历史记录补操作号、回填批次、重建审计哈希链，随后可按新协议继续使用。
- **角色权限照旧**：sensor_operator / bridge_engineer / traffic_authority / viewer 的原有矩阵不变；交通通告号仅交通主管可登记。

## 模块结构

- `app.py`：参数解析、依赖组装和HTTP服务启动。
- `src/domain.py`：数据结构、错误、状态、检查点编号和基础校验。
- `src/rules.py`：状态机、角色矩阵、依据字段权限、优先级和决定快照。
- `src/repository.py`：SQLite建表/旧库迁移、事务、操作表、批次、检查点和审计链。
- `src/service.py`：操作幂等、权限检查、批次编排、并发占用/现场记录、依据重算和崩溃恢复。
- `src/http_api.py`：JSON路由和统一错误响应。
- `src/audit.py`：UTC时间和SHA-256审计事件。
- `static/index.html`：最小演示页。
- `tests/`：完整流程、规则、失败和可恢复批次/迁移测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8318
```

默认端口为`8318`，首次启动自动建库；旧版数据库打开时自动迁移。使用`X-Actor`和`X-Role`请求头传递身份。

## 主要接口

- `GET /health`
- `GET /api/items` / `POST /api/items`（body 可带 `op_id`、`weather`）
- `GET /api/items/{id}`
- `POST /api/items/{id}/records`（传感器/巡检追加事项，可带 `op_id`）
- `POST /api/items/{id}/basis`（更新 `quantity`/`threshold`/`severity`/`weather`/`traffic_notice_no`，可带 `op_id`）
- `POST /api/items/{id}/transition`，提交 `target`、`expected_version`、`op_id`、`basis_version`、`traffic_notice_no`
- `GET /api/items/{id}/batches`（查看批次与决定快照）
- `PATCH /api/items/{id}/records/{rid}/review`（复核后到者留下的现场记录）
- `GET /api/operations/{op_id}`（查询操作状态、检查点与首次结果）
- `GET /api/operations/recover` 或 `POST /api/items/{id}/recover`（按操作号恢复，body 可给 `op_id`）
- `GET /api/audit`

允许角色：sensor_operator, bridge_engineer, traffic_authority, viewer。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
