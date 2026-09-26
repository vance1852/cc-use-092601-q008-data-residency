# 增加数据集驻留合规与算力调度联动基础平台

本项目是一套可离线运行的 Python 服务端平台，用于管理数据中心、互联通道、加速卡资源批次、租户预约、容量分配、交付情景与硬件稳定性准入。业务状态、幂等结果和审计事件保存在 SQLite 中，适合调度、质量、风险和审计人员在单个 Linux 应用容器内协作。

## 目录

- `src/compute_fabric/`：站点、通道、资源库存、预约、容量分配、情景分析，以及数据集驻留合规与跨区域训练调度联动；
- `src/accelerator_lab/`：加速卡测点导入、排除复核、分析任务租约和准入决定；
- `src/silicon_qualification/`：AI 加速芯片批次、测量、分析与质量审批；
- `fixtures/`：离线验收使用的结构化协议与测点；
- `tests/`：核心规则、错误边界、事务、API 和命令行验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅使用 Python 标准库和 SQLite

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 构建检查

```bash
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m compute_fabric.acceptance --workspace .
PYTHONPATH=src python3 -m accelerator_lab.acceptance --workspace .
PYTHONPATH=src python3 -m silicon_qualification.acceptance
```

这些命令使用临时 SQLite 数据库完成站点、资源、预约、分配、测点分析和芯片准入流程，不访问外部网络。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m compute_fabric.api --database compute.sqlite3 --host 127.0.0.1 --port 8080
PYTHONPATH=src python3 -m accelerator_lab.api --database lab.sqlite3 --host 127.0.0.1 --port 8081
PYTHONPATH=src python3 -m silicon_qualification.api --database silicon.sqlite3 --host 127.0.0.1 --port 8082
```

服务均提供 `GET /health`，其余接口使用 JSON。进程重启后可以继续查询 SQLite 中的业务状态和审计历史。

## 数据集驻留合规与调度联动

跨区域训练作业在生成候选站点时必须同时通过三类边界，规则逐条记录并可向租户解释站点被排除的具体原因：

- 数据边界：站点地域属于数据集确定版本登记的驻留地域范围；访问主体持有该版本的有效授权且未到期、未撤回；
- 算力边界：站点提供计划所需的算力产品，且可用算力满足计划需求；
- 网络边界：站点网络分区与计划要求的网络分区一致。

合规接口（`compute_fabric.api`，通过 `X-Actor-Id` 标识操作者）：

- `POST /datasets`（compliance）：登记数据集确定版本、驻留地域范围和内容摘要；
- `POST /authorizations/import`（compliance）：按 `batch_id` 批量导入主体授权，事务内全有或全无，重复提交返回同一稳定结果；
- `POST /authorizations/revoke`（compliance）：撤回主体授权。未启动的计划立即取消；运行中的计划进入 `blocked_running` 等待人工处置（halt 或 continue_monitored），系统不会静默迁移站点。已发生的合规访问记录只追加、不抹除；
- `POST /training/plans`（tenant）：提交作业计划，必须引用确定的数据集版本，返回候选站点与逐站点规则解释；
- `POST /training/plans/{id}/launch`、`/running`（dispatcher）：在候选站点启动并标记运行，启动前再次校验授权与边界；
- `GET /training/plans/{id}/tenant`：租户最小视图，仅含本主体计划的候选与排除原因；
- `GET /training/plans/{id}/dispatcher`：调度员视图，含跨主体调度字段与逐条边界判定；
- `GET /training/plans/{id}/audit`（auditor/compliance）：审计视图，含数据集版本、授权状态和完整访问记录。
