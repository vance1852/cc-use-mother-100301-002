# 极地科考站协作服务：科研样品监管链

本项目在极地科考站基础能力（机构、站点、操作者、角色权限、请求幂等、SQLite 事务、
哈希串联审计）之上，提供完整的**科研样品监管链（chain of custody）**：采样计划生成
容器谱系，分装、合并、借出、归还、消耗与派生分析沿不可跳跃的父子关系推进，对称量、
封签、交接人员、校准依据和环境记录保留当时版本；离线设备补传只追加、不改写，异常
事件进入隔离裁决。

## 监管模型

- **采样计划 → 容器谱系**：采样计划预登记本批原始岩芯容器与期望封签；`core_generated`
  只能生成计划声明过的容器。`aliquot`（分装）建立单父子边，`merge`（合并）建立多父
  子边；任何派生事件都必须能沿父子边接续到已确认容器，否则按“无法接续”隔离。
- **不可跳跃的事件链**：`weigh / seal / open / transfer / loan / return / consume /
  environment / analysis` 逐事件改变容器投影并自增乐观版本号；事件载荷以规范化 JSON
  与 SHA-256 摘要固化，封存当时的称量值、封签编号、交接人员、校准依据与环境读数。
- **离线优先**：现场事件携带 `device_id + local_sequence`。恢复连接后按本地序列补传；
  重复上传永远回放第一次的处理结果（已确认事件或隔离案件），同序列不同内容进入隔离，
  绝不覆盖已确认历史。时钟倒退、序列缺口、越权开封、封签不符、前后矛盾（乐观版本
  冲突）、无法接续、质量矛盾一律进入**隔离裁决**。
- **裁决只追加**：管理员可接受或驳回隔离事件。接受时事件以“经裁决”标记追加进监管链
  与审计链，并可越过过期的乐观版本声明；仍无法接续的事件不能被接受。驳回只记录结论，
  被驳回事件视为从未发生。任何裁决都不改写既有事件。
- **溯源**：按分析结果可还原对应容器、全部祖先（含合并的多父）、采样计划以及每次
  保管转移的时间线。
- **管控视图**：识别失联设备、超温容器、封签异常、超期借出与待裁决样品；质量守恒
  报告以事件重放台账核对每个计划根容器的初始质量 = 现存 + 消耗/分析 + 登记损耗，
  跨计划合并按比例分摊，凭空增加或消失都会被标出。

## 目录

- src/polar_station_foundation/
  - `custody.py`：监管链领域服务（谱系、事件回放、离线提交、隔离裁决、溯源、告警与守恒）；
  - `custody_domain.py`：事件类型与隔离原因代码；
  - `models.py`：容器、监管事件、分析、隔离案件等数据对象；
  - `service.py` / `storage.py` / `audit.py` / `clock.py`：主体权限、SQLite 事务、
    哈希审计链与可替换时钟；
  - `api.py`：HTTP/JSON 边界；`acceptance.py`：离线端到端验收。
- tests/：基础规则、监管链规则、存储事务、HTTP 路由与端到端验收测试。

## 环境

- Linux，Python 3.11+，仅使用标准库与 SQLite。

## 测试

    PYTHONPATH=src python3 -m unittest discover -s tests -v
    python3 -m compileall -q src tests

## 离线验收

    PYTHONPATH=src python3 -m polar_station_foundation.acceptance

验收会演练：计划建档 → 离线岩芯生成（含一次封签离线补录）→ 补录事件隔离与管理员
裁决接受 → 重复上传回放原结果 → 时钟倒退隔离并驳回 → 分装/借出/归还（封签异常）/
派生分析 → 分析溯源 → 质量守恒核对 → 审计链校验，成功时输出一行 status 为 ok 的 JSON。

## HTTP 服务

    PYTHONPATH=src python3 -m polar_station_foundation.api --database polar_station.sqlite3 \
        --host 127.0.0.1 --port 8080

写入接口用 `X-Actor-Id` 标识操作者，在线写入可用 `X-Request-Id` 做第二幂等键；
离线事件的第一幂等键是包络中的 `device_id + local_sequence`。

主要接口：

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /sampling-plans` | 登记采样计划与岩芯容器、期望封签 |
| `POST /custody-events` | 提交监管事件；异常返回 `accepted:false` 与 `case_id` |
| `GET  /containers` | 按 plan/site/status 列出容器投影 |
| `GET  /containers/{id}` | 容器当前状态（持有人、封签、质量、版本、告警位） |
| `GET  /containers/{id}/lineage` | 容器祖先谱系与全部事件 |
| `GET  /containers/{id}/events` | 该容器相关的不可变事件 |
| `GET  /analyses/{id}/provenance` | 分析结果溯源：容器、祖先、保管转移时间线 |
| `GET  /quarantine-cases?status=` | 列出隔离案件 |
| `POST /quarantine-cases/{id}/adjudication` | 管理员 accept / reject |
| `GET  /alerts?before=<ISO8601>` | 失联、超温、封签异常、超期借出、待裁决 |
| `GET  /conservation-report?plan_id=` | 父子质量守恒核对 |

### 事件包络示例

```json
{
  "device_id": "dev-field-07",
  "local_sequence": 1042,
  "event_type": "aliquot",
  "actor_id": "operator-001",
  "occurred_at": "2026-10-01T09:00:00Z",
  "expected_version": {"core-a": 1, "tube-a1": 0},
  "payload": {
    "parent_id": "core-a",
    "container_id": "tube-a1",
    "kind": "tube",
    "mass_grams": 300.0,
    "opened_seal_id": "seal-a",
    "parent_mass_after": 700.0,
    "seal": {"seal_id": "seal-a1"},
    "reseal": {"seal_id": "seal-a-r1"}
  }
}
```

健康检查 `GET /health` 同时返回审计哈希链校验结果；服务重启后 SQLite 中的谱系、
事件、隔离案件与审计历史继续保留。
