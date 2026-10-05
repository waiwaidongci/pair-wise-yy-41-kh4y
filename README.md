# 桥梁结构监测与限行决策

融合传感、巡检、交通荷载和天气数据，生成限载限行或恢复建议。

## 模块结构

- `app.py`：参数解析、依赖组装和HTTP服务启动。
- `src/domain.py`：数据结构、错误、状态和基础校验。
- `src/rules.py`：状态机、角色矩阵、优先级、期限和关闭不变量。
- `src/repository.py`：SQLite建表、事务、版本控制和审计链。
- `src/service.py`：权限检查、用例编排、并发控制和审计。
- `src/http_api.py`：JSON路由和统一错误响应。
- `src/audit.py`：UTC时间和SHA-256审计事件。
- `static/index.html`：最小演示页。
- `tests/`：完整流程、规则和失败测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8318
```

默认端口为`8318`，首次启动自动建库。使用`X-Actor`和`X-Role`请求头传递身份。

## 主要接口

- `GET /health`
- `GET /api/items`
- `POST /api/items`
- `GET /api/items/{id}`
- `POST /api/items/{id}/records`
- `POST /api/items/{id}/transition`，必须提交`expected_version`
- `POST /api/items/{id}/basis`：监测值/天气变化后更新依据并自增依据版本
- `GET /api/audit`

允许角色：sensor_operator, bridge_engineer, traffic_authority, viewer。监测偏差与预警阈值之比和多条异常记录决定告警等级；限行与封闭决策必须绑定交通通告记录。

## 可恢复批次（操作号 / 依据版本 / 交通通告号）

所有写操作（建告警、加记录、推进状态）都可作为一次**批次**提交，请求体携带：

- `operation_no`：操作号。同号重送沿用首次结果，不会重复写入；并发冲突的批次重送沿用首次冲突结果。
- `basis_version`：决策所依据的监测版本（依据版本）。
- `traffic_notice_no`：交通通告号，作为决策依据留痕（限行、封闭、恢复在角色匹配后可直接改）。

行为约定：

- **先到者占用，后到者留现场记录待复核**：同一告警并发推进时，先到者原子占用版本并挂审计；后到者版本对不上，写入一条 `kind=conflict_review` 的现场记录（`status=open`）并返回 409，待人工复核。
- **快照与重算**：已完成决定保留当时的依据快照（监测值、天气、交通通告号、优先级、响应期限、是否升级）；监测值或天气变化后，未完成批次按新依据重算派生指标。
- **从检查点恢复**：写入失败后按 `operation_no` 从检查点继续，已占用并挂审计的批次直接收尾，不重复占用、不重复挂审计。
- **旧记录补全**：旧库缺少 `operation_no` / `basis_version` 的记录在启动时自动回填（依据版本补为所属告警当前版本），补全后继续可用。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
