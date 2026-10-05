# 守住科研样品监管链基础服务

本项目提供极地科考站服务端应用共用的基础能力，用于登记科考机构、站点、操作者和结构化业务资料，并提供角色权限、请求幂等、SQLite 事务与哈希串联审计。具体的物流、样品、能源、医疗和许可业务可在这些边界上扩展自己的状态、规则和接口。

项目当前包含两个包：

- src/polar_station_foundation/：领域模型、SQLite 存储、权限服务、审计链、HTTP 路由和离线验收；
- src/polar_station_custody/：样品监管链服务，在基础边界之上实现容器谱系、监管事件、离线补录、隔离裁决与数量核对；
- tests/：基础规则、事务边界、接口路由、监管链规则和端到端验收测试。

## 样品监管链

- 采样计划登记时生成根容器谱系；分装、合并、借出、归还、消耗和派生分析都沿父子关系推进，每个事件必须声明所触容器的基准版本，版本不符即无法接续；
- 称量器具、封签、交接人员、校准依据和环境记录以版本化依据文档保存，事件引用时把当时版本的内容快照写入事件载荷，事后更新依据不影响历史；
- 野外设备恢复连接后按 `(device_id, local_sequence)` 提交历史事件，重复上传只返回原处理结果，同一序列号上传不同内容会作为矛盾隔离；
- 时钟倒退、越权开封、前后矛盾、无法接续的事件进入隔离裁决；裁决只能驳回或在事件仍适用时追加并入，绝不能覆盖已确认历史；
- 研究人员可按分析结果编号还原容器谱系与每一次保管转移；管理员可识别失联（借出逾期）、超温、封签异常和待裁决样品，并核对父子样品数量没有凭空增加或消失。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅使用 Python 标准库和 SQLite

## 测试

    PYTHONPATH=src python3 -m unittest discover -s tests -v

## 构建检查

    python3 -m compileall -q src tests

## 离线验收

    PYTHONPATH=src python3 -m polar_station_foundation.acceptance
    PYTHONPATH=src python3 -m polar_station_custody.acceptance

验收命令会在临时 SQLite 数据库中登记科考机构、操作者、站点和业务资料，核对幂等回执与审计链；监管链验收还会演练分装、借还、派生、合并、消耗、设备补录、隔离裁决、溯源与数量核对，成功时输出一行 status 为 ok 的 JSON 并以退出码 0 结束。

## HTTP 服务

    PYTHONPATH=src python3 -m polar_station_foundation.api --database polar_station.sqlite3 --host 127.0.0.1 --port 8080
    PYTHONPATH=src python3 -m polar_station_custody.api --database custody.sqlite3 --host 127.0.0.1 --port 8081

健康检查使用 GET /health。写入接口通过 X-Actor-Id 标识操作者，服务重启后 SQLite 中的业务状态和审计历史继续保留。监管链服务在 /custody/ 前缀下提供计划、设备、依据、事件、裁决、溯源、核对与总览接口，其余路径（组织、操作者、场所等）由基础服务处理。
