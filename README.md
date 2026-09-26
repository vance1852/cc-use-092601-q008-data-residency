# 增加数据集驻留合规与算力调度联动基础平台

本项目是一套可离线运行的 Python 服务端平台，用于管理数据中心、互联通道、加速卡资源批次、租户预约、容量分配、交付情景、硬件稳定性准入，以及**数据集驻留合规与跨区域训练调度联动**。业务状态、幂等结果和审计事件保存在 SQLite 中，适合调度、质量、风险、合规和审计人员在单个 Linux 应用容器内协作。

## 数据集驻留合规与调度联动

跨区域训练作业不再只选择 GPU 集群：作业计划必须引用**确定的数据集版本**，系统在同时满足数据、授权、算力和网络四类边界的站点才生成候选。

- **登记**：数据集版本登记内容指纹 `content_sha256`、允许驻留地域 `region_scope`、网络边界 `network_boundary` 与到期时间；站点（facility）登记地域与网络边界。
- **授权**：登记访问主体（租户/用户）、地域范围、网络边界与到期时间。批量授权导入在单事务内执行，**全有或全无**；以幂等键重复提交返回首次稳定结果。
- **调度联动**：作业计划引用确定版本；评估器对每个站点依次判定数据版本（未登记/冻结/到期/驻留地域/网络域）、主体授权（无授权/主体不符/地域/网络/到期/撤回）、算力（产品/可用量），仅全部通过的站点进入候选，并保留每个被排除站点的规则代码与中文解释。
- **撤回与到期**：授权撤回立即阻止**未启动**计划（实时重算，仍有其它合规站点时保留候选）；**运行中**作业只在原站点变得不合规时进入 `manual_hold` 人工处置，系统不取消、不静默迁移；已发生的合规访问写入只追加表，撤回不抹除历史。
- **版本冻结**：冻结版本阻断全部未启动计划并挂起运行中计划，等待人工处置。
- **最小必要视图**：
  - 租户：只见本租户计划的合规结论、候选站点与排除原因，不见内部算力库存数值与授权明细；
  - 调度员：额外可见资源需求、指派站点与提交人；
  - 审计/合规：额外可见授权依据、合规访问记录与人工处置留痕。

新增角色：`tenant`（提交/查看自己的作业）、`compliance`（登记数据集与版本、导入/撤回授权、冻结版本、人工处置）。

### 合规接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/datasets` | 登记数据集 |
| POST | `/datasets/versions` | 登记数据集版本（指纹、地域、网络边界、到期） |
| POST | `/datasets/versions/{version_id}/freeze` | 冻结版本 |
| GET | `/datasets/{dataset_id}` | 按角色投影的数据集视图 |
| POST | `/grants/import` | 批量授权导入（全有或全无，幂等） |
| POST | `/grants/{grant_id}/revoke` | 撤回授权 |
| POST | `/job-plans` | 提交作业计划（必须引用确定版本，幂等） |
| GET | `/job-plans` | 按角色列出计划（支持 `state`、`tenant_id` 过滤） |
| GET | `/job-plans/{plan_id}` | 计划的最小必要视图 |
| POST | `/job-plans/{plan_id}/evaluate` | 重新评估候选站点 |
| POST | `/job-plans/{plan_id}/dispatch` | 派发到合规候选站点（带期望版本号） |
| POST | `/job-plans/{plan_id}/running` | 标记运行中 |
| POST | `/job-plans/{plan_id}/disposition` | 人工处置：`resume`/`cancel`/`complete` |
| GET | `/job-plans/{plan_id}/sites/{facility_id}` | 解释某站点被排除/入选的具体规则 |
| GET | `/compliance/accesses` | 合规访问记录（审计/合规，只追加） |

## 目录


- `src/compute_fabric/`：站点、通道、资源库存、预约、容量分配和情景分析；
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
