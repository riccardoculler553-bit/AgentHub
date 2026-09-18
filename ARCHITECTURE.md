# AgentHub 开发架构文档（当前实现快照 · V1.5 Remote-Exec-1.0）

> 版本：AgentHub V1.5 Remote-Exec-1.0 + V1.3 Workflow Engine + V1.2 Tool-Using Agent + V1.1 Reliable-1.0 + MVP-Real-1.0 + Routing-1.0（底层 DeviceLink V1.0 已含 2026-09-05 公网架构调整）· 更新日期：2026-09-12
> 测试基线：`pytest tests/`（排除已知挂起的 test_capability_acceptance.py，技术债）→ **299 passed**
> 里程碑：**MVP 已全链路实机跑通**（§16）；**V1.1 可靠性执行落地**（§17）；**V1.2 Tool-Using Agent 落地**（§18）；**V1.3 Workflow Engine 落地**（§19）；**V1.4 Capability Runtime + V1.5 远程执行落地**（能力包/Artifact 数据平面/输入链路/Python venv Runtime/Workspace，首个真实能力 data.excel.preprocess 实机验收通过，见 §20）。

AgentHub 是构建在 DeviceLink（多设备 WebSocket 注册与连接管理平台）之上的**任务控制与智能编排层**：Admin/主 Agent 把自然语言或结构化请求转化为任务（Task），经命令注册表（Command Registry）与能力注册表（Capability Registry）双重校验后，通过 DeviceLink 长连接派发到指定子电脑的 Worker 执行，回报结果、支持多步串行、自动重试、超时看门狗、取消与离线重派。

---

## 1. 系统定位：四层平面架构

```
┌─────────────────────────────────────────────────────────────────────┐
│  Reasoning Plane（推理平面）                                          │
│  Main Agent（LangGraph）：理解请求 → 规划 ExecutionPlan → 驱动任务闭环   │
│  planner(LLM/规则) · tools · graph(决策: finish/retry/replan)          │
├─────────────────────────────────────────────────────────────────────┤
│  Control Plane（控制平面）                                            │
│  TaskService · TaskDispatcher · TaskMonitor · CommandRegistry ·       │
│  CapabilityRegistry · Admin API · Dashboard                           │
├─────────────────────────────────────────────────────────────────────┤
│  Communication Plane（通信平面 = DeviceLink）                          │
│  ConnectionHub（内存路由） · DeviceConnection · Envelope 协议 ·         │
│  HeartbeatMonitor · 注册/认证/撤销                                     │
├─────────────────────────────────────────────────────────────────────┤
│  Execution Plane（执行平面）                                           │
│  子电脑 DeviceClient：WebSocketClient · TaskManager ·                  │
│  Executor Registry（echo/python…） · 本地脚本                          │
└─────────────────────────────────────────────────────────────────────┘
```

**关键边界**：
- Agent 永远不直接触碰 WebSocket / 数据库细节 —— 派发走 `TaskDispatcher`，等待是纯 DB 轮询（离线由 TaskMonitor 自动重派）。
- 业务代码永远不直接触碰 WebSocket —— 唯一缝隙是 `DeviceLinkService`（换传输层只改这一个类）。
- Command（系统允许做什么）与 Executor（本机实际能做什么）分离，双侧一致才可执行。

## 2. 技术栈

| 层 | 技术 |
| --- | --- |
| 服务端 | Python 3.11+ / FastAPI / Uvicorn / SQLAlchemy 2.0 / Alembic |
| 数据库 | MySQL 8.4（生产，`pymysql`）；测试用文件型 SQLite |
| 通信 | WebSocket（`websockets` + FastAPI 原生 WS）/ `httpx` |
| 主 Agent | LangGraph ≥ 0.2 / LangChain-Core / LangChain-OpenAI（可选依赖 `agent` extra，无 Key 自动回退规则规划器） |
| LLM | 智谱 GLM（`glm-5.3-flash`，OpenAI 兼容 API `open.bigmodel.cn/api/paas/v4`）；结构化输出用 **function_calling** 模式（GLM 会在 JSON 后附加散文，破坏 json_schema 严格解析） |
| 钉钉接入 | **裸 Stream 协议**（`websockets` 手写：connections/open → WSS → ACK/分发，移植自 dingtalk-xbot-audit 实战项目）；不用官方 SDK——其 `-union` 网关端点对本应用不投递群聊回调 |
| 客户端 | Python 3.11+ 控制台程序，零 UI 框架 |
| Dashboard | 单文件原生 HTML/JS（`server/app/static/index.html`），无构建步骤 |

## 3. 目录结构（实际文件）

```
Websocket/
├── ARCHITECTURE.md                    # 本文档
├── pyproject.toml                     # 依赖（base/dev/agent）
├── server/
│   ├── main.py                        # uvicorn 入口（host/port 取自 settings）
│   ├── alembic.ini
│   ├── migrations/versions/
│   │   ├── 0001_initial_tables.py     # DeviceLink 5 张表
│   │   ├── 0002_agenthub_tables.py    # AgentHub 6 张表（commands/capabilities/tasks…）
│   │   ├── 0003_agent_runs.py         # MVP agent_runs 表
│   │   ├── 0004_v1_1_reliable_execution.py  # V1.1：task_steps.current_attempt_id / task_attempts.timeout_at / agent_runs 幂等唯一键
│   │   └── 0005_v1_2_agent_tool_calls.py    # V1.2：agent_tool_calls 审计表 / agent_runs.state_json + tool_call_count
│   └── app/
│       ├── main.py                    # FastAPI 应用 + lifespan（启动两个后台监控循环）
│       ├── core/
│       │   ├── config.py              # settings（.env 读取，AgentHub 段见 §12）
│       │   ├── security.py            # 注册码/Token 生成、SHA256 哈希
│       │   └── exceptions.py          # 业务异常 → HTTP 状态码映射
│       ├── db/
│       │   ├── database.py            # engine/SessionLocal（StaticPool 仅限纯内存 SQLite）
│       │   └── models.py              # users/devices/注册码/tokens/连接审计 5 表
│       ├── registration/service.py    # 一次性注册码（5 分钟 TTL，原子消费）
│       ├── auth/
│       │   ├── token.py               # 设备 Token 签发/校验/撤销（只存哈希）
│       │   ├── admin.py               # require_admin（X-Admin-Token）
│       │   └── dependencies.py
│       ├── device/                    # 设备业务 + 状态仓储
│       ├── command/
│       │   ├── db_models.py           # commands 表
│       │   └── service.py             # 注册表 CRUD + 种子命令 + 严格参数校验
│       ├── config/
│       │   └── agent_tools.json       # 业务路由表（用户可随时改：label/keywords/command→设备白名单）
│       ├── integrations/dingtalk/     # 钉钉 Stream 接入（裸协议，见 §16.2）
│       │   ├── client.py              # connections/open + WSS 监听 + ACK + 群节流 + 断线重连
│       │   ├── parser.py              # 回调 payload → IncomingMessage（@前缀剥离）
│       │   ├── models.py / sender.py  # 消息模型 / sessionWebhook 回复器
│       │   └── tools/                 # tools/dingtalk_probe.py（根目录）诊断探针
│       ├── capability/
│       │   ├── db_models.py           # device_capabilities 表
│       │   └── service.py             # 能力上报替换/查询/校验
│       ├── task/
│       │   ├── db_models.py           # tasks/task_steps/task_attempts/task_events
│       │   ├── models.py              # Pydantic API 模型
│       │   ├── errors.py              # TaskError 族（状态码映射）
│       │   ├── state.py               # V1.1 状态机：Task/Attempt 转移表 + 终态集合 + can_transition
│       │   ├── service.py             # 生命周期 + 设备事件账本（事件门序见 §17.2）
│       │   ├── dispatcher.py          # 派发器（attempt 创建/信封组装 + CAS 原子抢单，§17.3）
│       │   ├── monitor.py             # 后台循环：PENDING 重派 + 超时看门狗 + 重启恢复（§17.6）
│       │   └── device_link.py         # AgentHub ↔ DeviceLink 唯一缝隙
│       ├── agent/                     # Agent 层（三形态：mvp 现役 / tool_agent V1.2 / legacy 归档）
│       │   ├── core/                  #   V1.2 内核（§18.1）：state 契约 / context / policies 七闸门 / runner / prompts
│       │   ├── graph/                 #   V1.2 LangGraph：nodes（8 节点）/ routing / graph 装配
│       │   ├── llm/                   #   V1.2 LLM 层：AgentDecision schemas / 结构化 client / 折叠重试 service
│       │   ├── tools/                 #   V1.2 九个标准工具 + base（AgentTool/ToolResult/错误码）+ registry
│       │   ├── service.py             #   AgentService（tool_agent）：handle_message / resume_run / cancel_run
│       │   ├── runs.py                #   AgentRunService：落库 / WAITING_USER 停靠重开 / state_json 序列化
│       │   ├── db_models.py           #   AgentRun（幂等键 + V1.2 扩列）/ AgentToolCall 审计
│       │   ├── legacy/                #   V1.0 完整 Agent 归档（§10，回归测试仍覆盖）
│       │   └── mvp/                   # MVP Agent（单命令意图链，已实机跑通，§16.1；AGENT_MODE=mvp 默认）
│       │       ├── schemas.py         #   ExecutionIntent（意图+命令+点名设备）
│       │       ├── analyzer.py        #   GLM function_calling 意图分析 + 规则兜底
│       │       ├── tools.py           #   AgentToolRegistry（agent_tools.json 加载/缓存）
│       │       ├── graph.py           #   analyze→resolve→create_task→dispatch→wait_result→build_reply
│       │       └── service.py         #   MvpAgentService.handle_message（内存 run 表 + 后台图执行）
│       ├── workflow/                  # V1.3 Workflow Engine（§19）：确定性多步业务编排
│       │   ├── schemas.py             #   WorkflowDefinitionIn / RunOut 等 Pydantic 契约（name/version/step 重名校验）
│       │   ├── registry.py            #   定义注册表：create/validate（命令存在+enabled）/find（active 版本）/enable（同名单激活）
│       │   ├── engine.py              #   编排内核：create_run / start_run / advance / start_step（CAS）/ handle_task_result / cancel_run / recover_run
│       │   ├── resolver.py            #   上下文参数解析：{{ variables.x }} / {{ steps.<name>.result.<k> }}（全量匹配保类型，混合串走字符串替换）
│       │   ├── context.py             #   运行上下文：variables + steps 结果（重启安全，持久化在 workflow_runs.context_json）
│       │   ├── service.py             #   WorkflowService：Agent Tools 与 Admin API 的唯一入口（两方都不直写 SQL）
│       │   ├── monitor.py             #   WorkflowMonitor：慢速安全网 sweep（错过终态通知/孤儿步骤/PENDING 搁浅 run 修复）
│       │   ├── runtime.py             #   WorkflowRuntime：任务终态观察者回调 + call_soon_threadsafe 派发跳跃
│       │   ├── waiters.py             #   run 终态事件等待器（加速器，DB 才是权威）
│       │   ├── state.py / errors.py   #   双状态机（run/step）+ 错误码（workflow_* 小写蛇形）
│       │   └── db_models.py           #   workflows / workflow_steps / workflow_runs / workflow_step_runs / workflow_events
│       ├── websocket/
│       │   ├── hub.py                 # ConnectionHub（threading.Lock 内存路由）
│       │   ├── connection.py          # 单连接包装 + 状态机
│       │   ├── protocol.py            # Envelope 协议（见 §6）
│       │   └── heartbeat.py           # HeartbeatMonitor（45s 阈值/5s 扫描）
│       ├── api/
│       │   ├── registration.py        # POST /api/device-registration（admin）
│       │   ├── devices.py             # 注册/列表/详情/撤销/下发消息
│       │   ├── websocket.py           # WS /api/ws/device + 消息分派
│       │   ├── tasks.py               # 任务/命令/能力管理 API（admin）
│       │   ├── agent.py               # Agent API：run / message / runs / runs/{id} / runs/{id}/message / runs/{id}/cancel（admin）
│       │   ├── workflows.py           # V1.3 Workflow API：定义 CRUD/enable/disable + runs 创建/查询/取消/events（admin，§19.6）
│       │   └── (mvp|tool_agent)       # 同一套端点，AGENT_MODE 决定装配（§16.1 / §18.5）
│       └── static/index.html          # Dashboard v2（登录+设备/任务/命令/能力/Agent）
├── client/
│   ├── main.py                        # DeviceClient 入口（注册→连接→重连循环；含 INFO 日志配置：控制台 + client/logs/worker.log 轮转）
│   ├── identity.py / storage.py       # 本地身份持久化（DEVICELINK_HOME）
│   ├── registration.py / auth.py      # 注册流程 / TokenManager
│   ├── websocket.py / reconnect.py / heartbeat.py
│   ├── protocol.py                    # 与服务端同构的 Envelope
│   └── worker/
│       ├── manager.py                 # TaskManager（接收/去重/并发1/取消/回报 + ExecutionLedger 集成）
│       ├── ledger.py                  # 执行账本（~/.devicelink/worker.db）：attempt_id 幂等 + 断线重连补报
│       ├── registry.py                # Executor Registry（capabilities.json）
│       ├── executor.py                # Executor 基类 + ExecutionError
│       ├── capabilities.json          # 本机能力声明（每台子电脑各自维护，name 必须对齐服务端 command）
│       ├── executors/echo.py / executors/python_executor.py
│       ├── executors/yingdao.py       # 影刀执行器（§16.3：忙检查→settle→关弹窗→启动确认→日志 end 标记完成判定）
│       ├── executors/busy_check.py    # 影刀日志标记扫描 / 忙检测 / 启动确认 / 成功弹窗 WM_CLOSE
│       └── scripts/python_demo.py     # 预注册脚本（约定映射）
└── tests/
    ├── conftest.py                    # 文件型 SQLite + TestClient 夹具 + 测试专用路由表（禁 LLM）
    ├── unit/                          # protocol/reconnect/registration/token/状态机
    │   ├── _agentloop.py              # V1.2 测试基建：ScriptedLLM / probe_tool / 图装配助手
    │   ├── test_tool_registry.py / test_tool_policy.py          # 注册表语义 / 七闸门
    │   ├── test_agent_contract.py / test_agent_loop.py / test_agent_llm.py   # 契约键集 / 图循环 / LLM 重试
    │   ├── test_agent_confirmation.py / test_agent_resume.py    # 确认停靠 / 状态恢复
    │   └── test_agent_audit.py / test_agent_service.py          # 审计 / Run 生命周期
    ├── integration/                   # 注册流 / WebSocket 流
    └── agenthub/
        ├── _worker.py                 # FakeWorker（线程内 WS 会话 + 自动应答）
        ├── test_acceptance.py         # 验收：隔离/离线重派/幂等/重试账本
        ├── test_agent_tools.py        # V1.2 工具集成（真实 TaskService/Dispatcher/命令注册表）
        ├── test_agent_runs_api.py     # Run API：message 恢复 / cancel / 详情
        ├── test_acceptance_v12.py     # V1.2 验收矩阵：10 场景（§18.8）
        ├── test_mvp.py                # MVP 验收：attempt_id/未知设备/离线/忙跃迁/不支持意图
        ├── test_reliability.py        # V1.1 可靠性矩阵（§17.8）
        ├── test_yingdao_executor.py   # 影刀执行管线（忙门/启动确认/日志完成判定/超时）
        └── test_busy_check.py         # 日志标记扫描单测
```

## 4. 网络与部署架构

```
Internet ──HTTPS/WSS──> 公网暴露层 ──────> DeviceLink/AgentHub Server ──WS──> Device A/B/C…
                      （Tailscale Funnel   FastAPI @ 127.0.0.1:8000
                       /Tunnel/ngrok/Nginx）        │
                                                  MySQL 8（持久状态）
                                                  ConnectionHub（内存实时路由）
```

- Server 只绑 `127.0.0.1:8000`，公网可达性完全由暴露层决定；核心代码只认 `SERVER_PUBLIC_URL`。
- Client 无需公网可达：主动出站 WSS，Server→Client 消息复用长连接。
- Device ≠ Connection：一台设备可同时持有多条连接（多开/重连竞态），在线状态按设备聚合。
- Worker 双向能力：连接建立后立即上报 `device.capabilities`；断线由 `ReconnectManager` 指数退避重连。

## 5. 数据模型（13 张表）

### DeviceLink 层（迁移 0001）

| 表 | 说明 |
| --- | --- |
| `users` | 用户（默认 admin，启动时 ensure） |
| `devices` | 设备档案：device_id(UUID)/name/hostname/platform/status/last_seen_at/revoked_at |
| `device_registration_codes` | 注册码：仅存 SHA256 哈希，一次性 + 5 分钟 TTL |
| `device_tokens` | 设备 Token：仅存哈希，可撤销 |
| `websocket_connections` | 连接审计：connection_id/connected_at/disconnected_at/close_code |

### AgentHub 层（迁移 0002）

| 表 | 说明 |
| --- | --- |
| `commands` | **Command Registry**：command_name(唯一)/version/executor_type/executor_config/params_schema/timeout/enabled。定义“系统允许执行什么”，不含实现 |
| `device_capabilities` | **Capability Registry**：device_id + command_name（唯一约束）+ version/enabled。设备上报，整体替换（幂等） |
| `tasks` | 任务：task_id/name/created_by/target_device_id/status/priority/max_attempts/timeout_at。**V1.3 扩列（溯源，§19.4）**：`source_type`（API/AGENT/WORKFLOW）、`workflow_run_id`、`workflow_step_run_id`（均索引/可空，API 任务保持 NULL） |
| `task_steps` | 步骤：task_id + order_no + command + params(JSON) + status；V1.0 继承任务 target_device_id（列保留给未来跨设备工作流）。**V1.1 增列 `current_attempt_id`**：该步骤当前有效尝试，过期事件门（§17.2）据此判定 |
| `task_attempts` | 尝试账本：task_id + step_id + attempt_no + status + dispatch_message_id + error_code/message。每次派发/重试都新增一行。**V1.1 增列 `timeout_at`**：看门狗与结果竞速的判定时钟 |
| `task_events` | 全生命周期事件流：task/step/attempt + event_type + payload。供 Dashboard Timeline / 审计 / Agent 状态恢复 |
| `agent_runs` | **AgentRun**（MVP）：一次用户请求一行 —— run_id/channel/conversation_id/message_id/input_text/status(RUNNING/SUCCESS/FAILED)/task_id/ack_reply/final_reply/reply_webhook/error/created_at/finished_at。Run ≠ Task：一轮消息一个 Run，业务执行一个 Task；Run 是会话轮次审计（可扇出多 Task），Task 活过服务重启。回复**先落库再推送**，sender 故障不丢审计记录。**V1.1：`UNIQUE(channel, message_id)` 幂等键**（API 通道 message_id 存 NULL 豁免唯一约束，迁移 0004 先清空历史空串）。**V1.2 扩列（迁移 0005）**：`state_json`（WAITING_USER 停靠时序列化的 AgentState，恢复无需 LangGraph checkpointer）+ `tool_call_count`（本 Run 实际执行的工具调用数，恢复时续算全局预算）；状态机扩展为 `RUNNING / WAITING_USER / SUCCESS / FAILED / CANCELLED` |

### Agent 工具调用审计（迁移 0005，V1.2）

| 表 | 说明 |
| --- | --- |
| `agent_tool_calls` | **每一次走进策略链的工具调用一行**（含被拒绝的）：run_id/tool_call_id/tool_name/arguments(JSON)/status(RUNNING/SUCCESS/FAILED/REJECTED)/result(截断 JSON)/error_code/started_at/finished_at。由 ToolPolicy——唯一准入闸门——写入，工具 handler 永不触碰；大载荷写入时截断（§124：存摘要不存巨blob），Run 行的 tool_call_count 保持权威计数 |

### Workflow 编排（V1.3，§19；ensure_task_source_columns 启动补列）

| 表 | 说明 |
| --- | --- |
| `workflows` | 流程定义：workflow_id/name/version（`UNIQUE(name, version)`，不可变——修复发新版本）/status(DRAFT/ENABLED/DISABLED)/active_singleton/risk_level/requires_confirmation/definition_json（全量快照，enable 时重校验） |
| `workflow_steps` | 定义步骤：workflow_id + order_no + name（同流程唯一，上下文引用键）+ command + params（模板）+ device_id（可空=自动选机）+ on_failure(stop/retry) + retry_policy(max_attempts/retry_on) + enabled |
| `workflow_runs` | 运行实例：run_id/workflow_id/workflow_name+version（冗余快照）/status(PENDING/RUNNING/SUCCESS/FAILED/CANCELLED)/trigger_type(api/agent/manual)/created_by/context_json（variables + steps 结果，重启安全）/current_step_run_id/error_code/error_message/started_at/finished_at |
| `workflow_step_runs` | 步骤实例：run_id + order_no + name + command/status(PENDING/READY/RUNNING/SUCCESS/FAILED/CANCELLED/SKIPPED)/task_id（映射到 Task Engine，一不多；SKIPPED 恒为 NULL）/retry_count/result/error_code/started_at/finished_at |
| `workflow_events` | 编排事件流：run_id/step_run_id/event_type（workflow.created/started/step_started/step_success/step_retry/step_failed/completed/failed/cancelled）+ payload。Dashboard 时间线 / 审计 |

**状态机**（Task）：`PENDING → DISPATCHING → SENT → ACCEPTED → RUNNING → SUCCESS / FAILED / TIMEOUT / CANCELLED`
（Step：PENDING/RUNNING/…；Attempt：DISPATCHING/SENT/ACCEPTED/RUNNING/…）

## 6. WebSocket 通信协议

### Envelope（版本 1）

```json
{"id": "msg_xxx", "type": "task.dispatch", "version": 1, "timestamp": 1788595200000, "data": {}}
```

### 消息类型全集

| type | 方向 | 说明 |
| --- | --- | --- |
| `heartbeat` / `heartbeat_ack` | 双向 | 心跳保活（默认 15s 间隔；45s 阈值判离线） |
| `device.connected` | S→C | 认证通过后的首包（携带 device_id） |
| `device.disconnected` | S→C | 预留 |
| `message` / `message_ack` | 双向 | 通用文本消息 + 传输 ACK |
| `error` | S→C | 协议/业务错误 |
| `device.capabilities` | C→S | **能力上报**：`{"capabilities": [{"name","version"}]}`，服务端整体替换入库，回 `message_ack{stored}` |
| `task.dispatch` | S→C | 派发：`{task_id, step_id, attempt_id, command, params, timeout}`（attempt_id 供 Worker Ledger 幂等记账） |
| `task.accept` | C→S | 任务级 ACK（区别于传输 ACK）：attempt → ACCEPTED |
| `task.running` | C→S | 开始执行：step/task → RUNNING |
| `task.progress` | C→S | 进度：`{progress, message}`（仅记事件，不改状态） |
| `task.result` | C→S | 终态回报：`{status: success/failed/cancelled, result?, error?}` |
| `task.cancel` | S→C | 取消（管理端发起 / 超时看门狗），设备置 cancel 事件 |

### 连接生命周期

1. `WS /api/ws/device` + `Authorization: Bearer <device_token>`（HTTP 头认证）
2. 校验通过 → 发 `device.connected`，注册进 ConnectionHub；失败按 close code（4401/4403 等）
3. 客户端首包上报能力声明 → 之后循环：收 `task.dispatch` → TaskManager 处理 → 回报
4. 撤销设备：`revoke` API 主动以 4403 关闭全部连接（客户端见 4403 不再重连）

## 7. 服务端核心组件与启动流程

`app/main.py` lifespan（TestClient 与生产一致）：

```
Base.metadata.create_all → ensure_default_user
→ CommandService.ensure_seed_commands()        # 种子：echo / python.demo
→ HeartbeatMonitor.start   # 阈值 45s，每 5s 扫描：超宽限期 → 设备置 OFFLINE
→ TaskMonitor.start        # 每 3s 扫描（见 §8.4）
→ yield → 取消两个后台任务 → server stopped
```

- **ConnectionHub**：`threading.Lock` 保护的内存路由表（可从任意事件循环访问，测试 portal 也安全）；`send_to_device` 逐连接发送、失败剪枝。
- **DeviceLinkService**：AgentHub → DeviceLink 的唯一缝隙（`send_task` / `is_online`）。

## 8. 任务系统全链路

### 8.1 创建：四重校验（TaskService.create）

```
target_device 必填且存在且未撤销
  → 每个步骤: 命令存在且 enabled（require_executable）
  → 严格参数校验: params_schema 必填类型匹配 + 禁止额外键（防参数走私）
  → 能力校验: device_capabilities 存在且 enabled
任一失败 → 422 task_validation_failed；通过 → Task(PENDING) + Steps + task.created 事件
```

### 8.2 派发（TaskDispatcher.dispatch_task）

```
PENDING + 设备在线 → CapabilityService.has_capability 再核（缺失 → 任务 FAILED/DEVICE_CAPABILITY_MISSING）
→ 创建 TaskAttempt(DISPATCHING, attempt_no=次数+1) → 任务 DISPATCHING → commit
→ 组装 task.dispatch 信封（timeout 取自命令注册表）→ DeviceLinkService.send_task
→ 发送失败(0 连接, 竞态掉线) → 回滚 attempt → 任务回 PENDING（Monitor 兜底重派）
→ 成功 → attempt SENT, 任务 SENT, timeout_at = now + command.timeout → task.sent 事件
```

设备回报链（TaskService.handle_device_event）：`task.accept → ACCEPTED`、`task.running → RUNNING(step+task)`、`task.progress → 事件`、`task.result → 终态`。
安全边界：**设备只能回报派发给自己的 attempt**（device_id 不匹配直接忽略）。
多步串行：某步 SUCCESS 且还有 PENDING 步骤 → `advance=True` → API 层异步触发下一步派发。

### 8.3 幂等（双侧）

- 服务端：同一步骤重复 dispatch 由 attempt 账本与状态机约束；task_id/step_id 全局唯一。
- Worker 端：`(task_id, step_id)` 去重 —— 重复派发只回报当前状态（`task.running`），绝不二次执行（验收用例覆盖）；**ExecutionLedger**（本地 SQLite `~/.devicelink/worker.db`）按 `attempt_id`（UNIQUE）幂等记账，执行完成但回报未送达的结果在重连后**补报**，服务端不会重复跑已完成的 RPA 作业。

### 8.4 TaskMonitor 可靠性闭环（每 3s 扫描）

| 循环 | 逻辑 |
| --- | --- |
| **离线重派** | PENDING 任务：设备上线 → 立即派发；等待超过 `TASK_OFFLINE_MAX_WAIT`(600s) → 任务 TIMEOUT |
| **超时看门狗** | SENT/ACCEPTED/RUNNING 且 `timeout_at` 已过 → 任务 TIMEOUT + 尽力发送 `task.cancel` 给设备 |
| **取消通知** | 管理端 `POST /tasks/{id}/cancel` → `notify_cancel` 尽力下发 task.cancel |

取消语义：Worker 收到 cancel → 置 `asyncio.Event` → Executor 轮询到后终止子进程（terminate→kill）→ 回报 `task.result{cancelled}`；迟到结果不覆盖 CANCELLED（`late_result` 事件保留审计）。

重试语义：`POST /tasks/{id}/retry` 仅限可重试状态；从该步骤最早 FAILED/TIMEOUT/**PENDING**（覆盖“超时但步骤从未执行”的看门狗场景）重开 → attempt_no 递增记账；超过 `TASK_MAX_ATTEMPTS`(3) → 409 `invalid_task_state`（验收用例断言 attempt 1→2→3 与 409）。

> Worker 侧已知限制：执行中连接死亡则回报丢失，由服务端看门狗超时兜底（文档见 manager.py 头注）。

## 9. Worker 端（子电脑）架构

### 9.1 DeviceClient 主循环（client/main.py）

```
加载本地身份(无则注册: --code 一次性码) → TaskManager.ensure_consumer()
→ 循环: WebSocketClient.connect(Bearer token)
   → on device.connected → 上报 device.capabilities（每次连接一次）
   → 收 task.dispatch → TaskManager.on_dispatch / 收 task.cancel → on_cancel
   → 连接断开 → 指数退避重连（认证类 close code 不重连）
```

### 9.2 TaskManager（worker/manager.py）

- 单消费者 `asyncio.Queue`，**max_concurrency = 1**（V1.0）；WS 循环永不阻塞。
- `(task_id, step_id)` 去重；无本地执行器 → 直接回报 `task.result{failed, COMMAND_NOT_FOUND}`；参数非法 → `INVALID_PARAMS`。
- 每次尝试经 **ExecutionLedger** 记账（attempt_id 幂等 + 断线重连补报，见 §8.3）。
- 执行回报序列：`task.accept → task.running → (task.progress*) → task.result`；所有回报经 `_report` 包 Envelope 发送。

### 9.3 Executor Registry（worker/registry.py）

```
worker/capabilities.json:  [{"name": "echo", "executor_type": "echo"}, {"name": "python.demo", "executor_type": "python"}]
EXECUTOR_TYPES = {"echo": EchoExecutor, "python": PythonExecutor}
可上报能力 = 本地声明 且 有对应实现（reportable_capabilities）
可执行 = Command Registry(服务端允许) ∧ Executor Registry(本机能做)
```

### 9.4 执行器契约（worker/executor.py）

```python
validate(params) -> None  # ValueError 表示参数非法
execute(params, config, progress(pct, msg), cancel: asyncio.Event) -> dict  # JSON 可序列化
# 结构化失败: raise ExecutionError(code, message)
```

- **EchoExecutor**：原样回显 `message`，用于全链路验收。
- **PythonExecutor**：**约定映射、永不接受 Agent 传路径** —— 命令 `python.demo` → `worker/scripts/python_demo.py`；脚本以 `--params-json <json>` 接参，stdout 最后一行 JSON 对象即结果；取消时 terminate→kill；非零退出 → `EXECUTOR_FAILED`。

## 10. Legacy Agent（V1.0 完整 Agent，已归档 agent/legacy/）

> V1.2 起，该实现整体迁入 `agent/legacy/`（保留回归测试兼容）；现役 Tool-Using Agent 见 **§18**。`AGENT_MODE=tool_agent` 切换。

### 10.1 状态与图（agent/state.py, graph.py）

`AgentState`（TypedDict，图内飞行状态）：`user_request / context / execution_plan / target_device_id / task_id / task_status / task_result / error / decision / message / retry_count / replan_count`。

```
START → gather_context（设备/命令/能力快照）
      → plan（build_plan；PlanError → 记 error 重进 plan）
      → [has_plan?] ──否→ evaluate
            │是
      create_task（tools.create_task, created_by="main_agent"）
      → [has_task?] ──否→ evaluate
            │是
      dispatch（TaskDispatcher，Agent 不碰 WS）
      → wait_result（纯 DB 轮询: agent_poll_interval=2s, 上限 agent_max_wait=300s）
      → evaluate（按任务终态/错误 → decision: finish | retry | replan）
      → retry（重派同一任务; retry 预算耗尽 → evaluate）   条件路由 _after_retry
      → replan（原始请求 + error 重建计划; MAX_REPLANS=2）→ plan
      → finish → END
```

节点纪律：**每个节点自开 DB 会话**（不跨 await 共享）；等待只轮询 DB（离线重派由 TaskMonitor 完成）；`recursion_limit=60`；`AgentService.run` 用 `astream` 收敛最终状态并输出 `_RESULT_KEYS` 结果（含 request）。

### 10.2 规划器（agent/planner.py）

- **LLM 规划**（配置 `OPENAI_API_KEY` 时启用）：`ChatOpenAI(model=AGENTHUB_MODEL, temperature=0)` + `planner_system.txt` JSON-only 契约；输入为运行时快照（devices/commands/capabilities/previous_error）；输出经 `_validate_plan` 结构校验（命令必须存在且 enabled），剥码/容错解析。
- **规则回退**（无 Key 或 LLM 失败，Agent 循环永不被 LLM 可用性阻塞）：三级匹配（命令名精确 → 分词匹配 → 描述关键词）；echo 提取引号内消息、python.demo 提取 ``` 围栏代码；设备提示按 device_id/名称匹配。

### 10.3 工具层（agent/tools.py）

`list_devices / get_device / list_commands / get_command / create_task / get_task_detail / retry_task / cancel_task` —— 全部封装 Service 层，Agent 不直接访问 DB/WS。

### 10.4 运行入口

`POST /api/agent/run`（admin）：同步运行 —— 图到达决策（成功/重试耗尽/重规划预算用尽）后返回 `AgentRunOut`（request/execution_plan/task_id/task_status/task_result/error/decision/message/retry_count/replan_count）。Dashboard Agent 页签即调用此接口。

## 11. HTTP API 一览

| 方法/路径 | 鉴权 | 说明 |
| --- | --- | --- |
| `POST /api/device-registration` | Admin | 创建一次性注册码（5 分钟 TTL，明文仅返回一次） |
| `POST /api/devices/register` | 无 | 设备凭码换取身份（device_id + device_token） |
| `GET /api/devices` | 无 | 设备列表（含在线连接数；Dashboard 轮询用） |
| `GET /api/devices/{id}` | Admin | 设备详情 |
| `POST /api/devices/{id}/revoke` | — | 撤销设备并以 4403 关闭在线连接 |
| `POST /api/devices/{id}/messages` | Admin | 向设备下发文本消息 |
| `GET /api/devices/{id}/capabilities` | — | 设备能力（任务创建前等待上报的探测点） |
| `GET /api/capabilities` | Admin | 全部设备能力（按设备分组） |
| `POST /api/tasks` | Admin | 创建任务（四重校验，在线即派发）→ 201 TaskDetailOut |
| `GET /api/tasks` · `GET /api/tasks/{id}` · `GET /api/tasks/{id}/events` | Admin | 列表 / 详情(steps+attempts+events) / 事件流 |
| `POST /api/tasks/{id}/cancel` · `/retry` | Admin | 取消 / 重试（attempts 耗尽 → 409） |
| `GET /api/commands` · `GET /api/commands/{name}` | Admin | 命令注册表查询 |
| `POST /api/agent/run` | Admin | 自然语言 → Agent 闭环运行 |
| `POST /api/agent/message` | Admin | 创建（或恢复）一轮 AgentRun，立即返回 run_id（§18.5） |
| `GET /api/agent/runs` · `GET /api/agent/runs/{id}` | Admin | Run 列表 / 详情（含 tool_call_count、状态、最终回复） |
| `POST /api/agent/runs/{id}/message` | Admin | 向 WAITING_USER 的 Run 追加用户回复（恢复执行/确认高危操作） |
| `POST /api/agent/runs/{id}/cancel` | Admin | 关闭一个未结束的 Run（业务任务**不会**被自动取消，需显式 cancel_task） |
| `POST /api/workflows` · `GET /api/workflows` · `GET /api/workflows/{id}` | Admin | Workflow 定义创建（校验后 DRAFT）/ 列表 / 详情（§19.6） |
| `POST /api/workflows/{id}/enable` · `/disable` | Admin | 启用（同名单旧版本自动停用 + 命令白名单重校验）/ 停用 |
| `POST /api/workflows/{id}/runs` | Admin | 创建并立即启动一次运行 → 201 WorkflowRunOut（active_singleton 单例护栏 409） |
| `GET /api/workflow-runs` · `GET /api/workflow-runs/{id}` · `GET /api/workflow-runs/{id}/events` | Admin | 运行列表（status/limit）/ 详情（steps+context）/ 事件流 |
| `POST /api/workflow-runs/{id}/cancel` | Admin | 取消运行（run CAS → 终态 + 当前步骤 Task 一并 request_cancel） |
| `WS /api/ws/device` | Bearer Token | 设备长连接（见 §6） |

Admin 鉴权：请求头 `X-Admin-Token` 对比 `AGENTHUB_ADMIN_TOKEN`；为空 = 开放模式（仅限本地开发，测试默认此模式）。

## 12. 配置项（.env / 环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `APP_HOST` / `APP_PORT` | `127.0.0.1` / `8000` | 服务绑定（只绑回环） |
| `SERVER_PUBLIC_URL` | `http://127.0.0.1:8000` | 交给客户端的公网地址（暴露层决定） |
| `DATABASE_URL` | 由 `DB_*` 组装 MySQL | 生产 MySQL；测试注入文件型 SQLite |
| `REGISTRATION_CODE_TTL` | `300` | 注册码有效期（秒） |
| `HEARTBEAT_INTERVAL` / `OFFLINE_THRESHOLD` | `15` / `45` | 心跳间隔 / 离线阈值 |
| `AGENTHUB_ADMIN_TOKEN` | 空 | Admin 令牌；空 = 开放模式 |
| `TASK_OFFLINE_MAX_WAIT` | `600` | PENDING 等待设备上限（超时 → TIMEOUT） |
| `TASK_MAX_ATTEMPTS` | `3` | 每步骤最大尝试次数 |
| `OPENAI_API_KEY` / `OPENAI_API_BASE` / `AGENTHUB_MODEL` | 空 / 空 / `gpt-4o-mini` | LLM（完整 Agent 规划 + MVP 意图分析共用）；无 Key 自动回退规则。生产：智谱 `glm-5.3-flash` + `https://open.bigmodel.cn/api/paas/v4` |
| `AGENT_POLL_INTERVAL` / `AGENT_MAX_WAIT` | `2` / `300` | 完整 Agent 等待轮询间隔 / 上限（秒） |
| `AGENT_RUN_MAX_WAIT` | `1900` | MVP 后台图等待任务终态上限（秒；yingdao.audit 命令超时 1800s，需留余量） |
| `AGENT_MODE` | `mvp` | Agent 形态开关：`mvp`（固定管线，§16）或 `tool_agent`（V1.2 Tool-Using Agent，§18） |
| `AGENT_MAX_TOOL_CALLS` | `8` | 每 AgentRun 全局工具调用预算（恢复 Run 时续算，§18.4） |
| `AGENT_MAX_LLM_RETRIES` | `2` | LLM 瞬时错误 / 输出解析失败的折叠重试次数 |
| `AGENT_MAX_RUNTIME` | `120` | 单 Run 运行时上限（秒，evaluate 节点运行时护栏） |
| `AGENT_TOOL_WAIT_MAX` | `1900` | 工具内部等待任务终态上限（秒；LLM 永不轮询，§18.3） |
| `AGENT_CONFIRM_ACTIONS` | `false` | `execute_command` 是否要求用户确认（ACTION 级风险；retry/cancel_task 恒需确认） |
| `AGENT_DEFAULT_DEVICE_NAME` | `办公室电脑02` | 仅有提示文案用途；路由不再注入默认设备（由白名单/自动发现决定） |
| `AGENT_TOOLS_CONFIG` | 空 | 业务路由表 JSON 路径；空 = `server/config/agent_tools.json`（运行时可改） |
| `DINGTALK_CLIENT_ID` / `DINGTALK_CLIENT_SECRET` / `DINGTALK_ROBOT_CODE` | 空 | 钉钉 Stream 接入凭证；ID+SECRET 缺一即禁用集成（服务照常启动） |
| `DINGTALK_THROTTLE_SECONDS` | `30` | 群聊触发节流（同一会话滚动窗口内至多一次；≤0 关闭） |
| `DEVICELINK_HOME` | 平台默认 | 客户端身份文件目录（含 ExecutionLedger 的 worker.db；测试重定向） |

## 13. 安全设计

- **凭证不落明文**：注册码 / Device Token 仅存 SHA256，明文只在创建时展示一次；撤销即时生效（4403 踢线 + 客户端停止重连）。
- **Admin Token 与 Device Token 分离**：管理面全部 `require_admin` 护栏；设备面 Bearer Token + 仅能回报派发给自己的 attempt。
- **命令白名单 + 双注册表**：Server 注册表定义允许项；Worker 只执行本地预注册脚本（命令名→约定脚本名，正则白名单 `^[A-Za-z0-9_]+\.py$`，拒绝路径注入）；**Agent 永远不能传递可执行路径**。
- **参数走私防护**：params_schema 严格校验（必填 + 类型 + 禁止额外键）。
- **幂等性**：重复派发不重复执行（Worker 去重）；能力整体替换幂等。
- **网络**：服务只绑 127.0.0.1，公网经 Tailscale Funnel（WireGuard 加密隧道）。

## 14. 测试体系（193 passed）

| 层 | 文件 | 覆盖 |
| --- | --- | --- |
| unit | test_protocol / test_reconnect / test_registration_service / test_token | 信封解析、指数退避、一次性码生命周期、Token 哈希 |
| unit | test_task_state_machine | V1.1 状态机：全部合法转移接受、非法转移拒绝、终态集合封闭性 |
| unit | test_agent_contract / test_tool_registry / test_tool_policy | V1.2 契约：AgentState 键集锁定、ToolResult 携带 call_id、注册表语义（重名/禁用）、策略七闸门（存在/enabled/args 严格/权限/确认/双重限额/异常封装） |
| unit | test_agent_loop / test_agent_llm | 图循环：tool_call→observe→evaluate→finish、预算护栏、LLM 折叠重试、注入拒绝回归 |
| unit | test_agent_confirmation / test_agent_resume | 确认停靠与恢复：肯定答复跳过 LLM 直执行、否定取消、预算续算、state_json 往返 |
| unit | test_agent_audit / test_agent_service | agent_tool_calls 审计（REJECTED/SUCCESS/FAILED + 截断）、AgentService 幂等/恢复/取消 |
| integration | test_registration_flow / test_websocket_flow | 注册→连接→心跳→撤销全流程 |
| agenthub | test_acceptance / test_agent | **验收**：多设备隔离（B 永远收不到 A 的任务）、离线重派、重复派发幂等、重试 attempts 账本与 409 |
| agenthub | test_agent_tools / test_agent_runs_api | V1.2 工具集成（真实 TaskService/Dispatcher/命令注册表）、Run API（message 恢复 / cancel / 详情） |
| unit | test_workflow_state / test_workflow_resolver / test_workflow_registry | V1.3：双状态机（合法转移/终态封闭/retry 非转移/未知态拒绝）、参数解析（整值保类型/混合串替换/递归解析/缺失与未来步骤引用拒绝/坏语法原样保留）、定义注册表（命令白名单+enabled/契约校验/重名拒绝/active 版本/enable 互斥/enable 重校验） |
| agenthub | test_acceptance_v12 | **V1.2 验收矩阵（§18.8）**：10 大业务场景全过 |
| agenthub | test_workflow_run | **V1.3 验收矩阵（§19.9）**：10 大业务场景全过（FakeWorker 全链路，DB 为事实源） |
| agenthub | test_mvp | **MVP 验收**：成功链路（ack/final 回复 + attempt_id 三元组）、未知设备、离线、忙设备、忙跃迁路由、不支持意图 |
| agenthub | test_reliability | **V1.1 可靠性矩阵（§17.7）**：CAS 抢单唯一胜者、过期 attempt 事件仅审计、TIMEOUT/CANCELLED 后迟到 SUCCESS 不复活、结果-看门狗竞速唯一胜者、AgentRun message_id 幂等、Device API 鉴权、多连接聚合在线、重启恢复 |
| agenthub | test_yingdao_executor | 影刀执行管线：忙门（EXECUTOR_BUSY）、启动确认重试、**日志 end 标记完成判定**、超时终止、取消 |
| agenthub | test_busy_check | 日志标记扫描（start/end 时间戳解析、task_end_seen 判定边界） |

测试基建要点（踩坑后固化）：
- **文件型 SQLite**：TestClient 的 WS 会话各自运行在线程/portal 上，共享内存连接（StaticPool）会跨会话破坏事务 → `conftest.py` 注入文件型 DB；`database.py` 仅对纯内存 SQLite 启用 StaticPool。
- **FakeWorker**：线程内 `websocket_connect` + 自动应答（success/fail/silent/caps_only 四种行为）；`stop()` 必须 `session.__exit__()`（starlette 的 `close()` 不会解除 `receive_json` 阻塞）。
- **时序确定性**：建任务前用 `wait_for_capabilities` 等能力上报落库，规避异步上报竞态。
- **测试自有路由表**：`agent_tools.json` 用户可运行时编辑（label/keywords/devices），测试写入临时路由表 + autouse 夹具还原，不依赖生产配置。
- **强制离线确定性**：`.env` 若配了真 LLM Key，load_dotenv 会在夹具 pop 后回灌 → 直接对 settings 对象置 `openai_api_key = None`，测试永远走规则兜底。
- **Worker 账本隔离**：TaskManager 测试必须传 `ExecutionLedger(tmp_path)` —— 默认 `~/.devicelink/worker.db` 跨会话残留，破坏 attempt 幂等假设。
- 诊断手段：`pytest --timeout-method=thread --capture=no` + `PYTHONUNBUFFERED=1` + `py-spy dump --pid`。

## 15. 已知限制与演进方向（V1.1+）

| 现状限制 | 演进方向 |
| --- | --- |
| 任务单目标设备（target_device_id 必填） | 跨设备多步工作流（task_steps.device_id 列已预留） |
| Worker 并发 = 1，执行中断线回报由 Ledger 重连补报兜底 | 并发可配 + 结果实时推送 |
| 单业务命令（yingdao.audit 审单） | 多业务命令注册（OCR、导出、对账…），agent_tools.json 持续扩行 |
| 轮询等待（Agent / Monitor 均为周期扫描） | 事件驱动（DB notify / 内部总线） |
| MVP 意图单轮、无对话记忆 | **V1.2 已交付多轮交互核心**（ask_user 停靠 / WAITING_USER 恢复 / 确认流）；仍限每会话一个停靠 Run，自由对话记忆演进到 V1.3 |
| 工具同步执行（execute_command 阻塞至终态或超时） | 异步工具模型（agent_tool_calls 的 PENDING 状态已预留） |
| 恢复时每工具调用账本重启（全局预算连续） | 每工具账本持久化进 state_json |
| DingTalk 单机器人、回复走 sessionWebhook | 多机器人/多租户、主动推送（群播报任务状态） |
| Dashboard 轮询刷新 | WebSocket 推送实时刷新 |

## 16. MVP 全链路实机落地（MVP-Real-1.0 + Routing-1.0）

> **2026-09-07 已在生产环境实机跑通**：钉钉群 @机器人 → 语义理解 → 设备路由 → 子电脑影刀执行 → 日志级完成判定 → 群内自动回复，全程无人值守。

### 16.1 端到端链路（实测时序）

```
用户在钉钉群 @机器人：「运行办公室电脑02的Text1」
  │
  ├─ 钉钉网关 CALLBACK 帧（topic=/v1.0/im/bot/messages/get）
  │    → 裸 Stream WSS 收帧 → 先 ACK（协议要求）
  │    → GroupThrottle（同群 30s 滚动窗口，超限回「操作太频繁」）
  │    → message_id 幂等（重连重放/重试不双跑）
  │
  ├─ MvpAgentService.handle_message：AgentRun(RUNNING) 落库 → 后台图执行，HTTP/WS 零阻塞
  │
  ├─ [analyze] 意图分析
  │    LLM：GLM glm-5.3-flash function_calling → ExecutionIntent{intent:run_command,
  │    device_name:"办公室电脑02", command:"yingdao.audit"}（command 必须在注册表内）
  │    规则兜底：关键词「Text1」精确匹配 + 设备名最长匹配（无 Key / LLM 异常自动回退）
  │
  ├─ [resolve] 设备路由（"哪台电脑能跑什么"）
  │    候选序：消息点名 → agent_tools.json 白名单（按优先级）→ 自动发现（无白名单时）
  │    第一个 注册∧有能力∧在线∧不忙 的设备胜出；忙设备自动跃迁下一个候选
  │    → ACK 群内回复：「收到，正在启动办公室电脑02的Text1任务。」
  │
  ├─ [create_task] Task(PENDING, created_by=mvp_agent) ← 四重校验（§8.1）
  ├─ [dispatch] TaskDispatcher → attempt_1 → WS task.dispatch → Worker
  │
  ├─ Worker（办公室电脑02）影刀执行管线（§16.4）
  │    忙检查 → settle 3s → 关成功弹窗 → 启动 ShadowBot → 启动确认(≤2 次重试)
  │    → 每秒轮询影刀日志，end 标记晚于本次 start 时间戳 → 完成
  │    → task.accept → running → progress → result{success}
  │
  ├─ [wait_result] task_waiters 事件唤醒（DB 轮询兜底，上限 AGENT_RUN_MAX_WAIT=1900s）
  ├─ [build_reply] 「办公室电脑02的Text1任务已完成。」
  └─ 回复先落 AgentRun.final_reply → 再推钉钉 sessionWebhook → AgentRun(SUCCESS)
```

**实机验证记录（2026-09-07）**：
- 群聊 @机器人 与私聊均正常收发（企业内部应用机器人，非 webhook 自定义机器人）
- ACK 秒回 + 影刀真实运行约 30s 后群内收到完成回复，时间线与 Dashboard 任务详情一致
- 子电脑控制台实时滚动执行日志，同时落盘 `client/logs/worker.log`（自动轮转）
- 服务端 `server/logs/server.log` 可见钉钉帧日志、设备连接、任务派发全过程

### 16.2 钉钉 Stream 接入（integrations/dingtalk/）

**为什么不用官方 SDK**：`dingtalk-stream` SDK 的网关端点分配到 `-union` 网关，对本应用**不投递群聊 @ 回调**（私聊正常、群聊静默）。改为移植实战项目 dingtalk-xbot-audit 的裸协议：

1. `POST https://api.dingtalk.com/v1.0/gateway/connections/open`（clientId/secret + CALLBACK 订阅）→ `{endpoint, ticket}`
2. `websockets.connect(endpoint?ticket=…)` 长连接
3. 帧类型：SYSTEM(ping/disconnect) / EVENT / CALLBACK；**每帧先 ACK**（code+messageId 回显）
4. CALLBACK 且 topic 匹配 → `parse_incoming`（剥离 @前缀，提取 text/conversation_id/sender/message_id/sessionWebhook）
5. 断线自动重连（连续失败 10s → 60s 退避）；凭证错误连续 3 次降频

**群聊踩坑修复记录**：
- 必须用**企业内部应用机器人**拉进群（webhook 自定义机器人不走 Stream 回调）
- 应用可见范围须覆盖群成员（改为全体员工并重新发布后生效）
- 解析器**不得**校验 atUsers 与 robotCode 的对应关系（群聊 @ 场景该字段不可靠）

### 16.3 业务路由表（server/config/agent_tools.json，运行时可改）

```json
{
  "tools": [
    {
      "name": "audit",
      "label": "Text1",
      "command": "yingdao.audit",
      "keywords": ["Text1"],
      "devices": ["办公室电脑02"]
    }
  ]
}
```

- `keywords`：触发词（规则分析 + LLM 目录提示共用；短词用精确匹配防误触发）
- `devices`：**优先级白名单** —— 消息点名设备 → 依次尝试白名单设备；留空 = 自动发现所有上报了该能力的设备
- 设备选择错误优先级：`device_busy > device_offline > capability_missing > device_not_found`（"在忙"比"离线"更值得先告诉用户；离线优先于能力缺失）
- 增加新业务 = 加一行配置（服务端）+ 一段 capabilities.json（子电脑），零代码

### 16.4 影刀执行器（client/worker/executors/yingdao.py + busy_check.py）

每台子电脑在 `client/worker/capabilities.json` 各自维护配置：

```json
{
  "name": "yingdao.audit", "executor_type": "yingdao", "version": "1.0",
  "robot_uuid": "<影刀机器人 UUID>",
  "shadowbot_path": "F:/ShadowBot/ShadowBot.exe",
  "args": ["shadowbot:Run?robot-uuid={robot_uuid}"],
  "wait_for_exit": true, "check_mode": "log",
  "log_dir": "%LOCALAPPDATA%/ShadowBot/log",
  "settle_after_end_seconds": 3, "launch_verify_seconds": 4,
  "launch_max_retries": 2, "close_success_box": true,
  "success_box_keywords": ["运行成功", "执行成功"]
}
```

执行管线（移植自 dingtalk-xbot-audit 实战逻辑）：

```
忙检查（log 模式读日志标记 / process 模式查进程）→ 忙 → EXECUTOR_BUSY
→ settle 等待（上个任务收尾 3s）
→ 关闭残留成功弹窗（WM_CLOSE，关键词匹配标题）
→ 启动 ShadowBot.exe（shadowbot:Run?robot-uuid=…）
→ 启动确认：launch_verify_seconds 内进程出现，失败重试（≤launch_max_retries）
→ 完成等待（每秒轮询）：
    - check_mode=log：**日志 end 标记是唯一完成判据**
      启动前记录日志最新 start 时间戳 our_start_time；
      task_end_seen() 只认时间戳晚于 our_start_time 的 end 标记
      （ShadowBot.exe 可能秒退——launcher 模式——或常驻不退，进程存活均不可信；
        完全读不到日志文件时才回退进程退出判定）
    - check_mode=process：进程退出即完成
→ 超时（命令注册表 timeout 1800s）→ terminate_robot → EXECUTOR_TIMEOUT
→ 取消事件 → terminate_robot → EXECUTOR_CANCELLED
```

错误码 → 用户文案映射（build_reply）：`EXECUTOR_BUSY`→"当前正在运行其他程序"；`EXECUTOR_LAUNCH_FAILED/START_FAILED`→"启动失败，请检查该电脑上的影刀配置"；其余带错误详情后缀。

### 16.5 MVP Agent 图与 AgentRun 语义

- 图：`analyze → resolve → create_task → dispatch → wait_result → build_reply`，任意节点置 `error` 短路至 build_reply 转中文友好文案（§16.1 时序图）
- `dispatch` 返回 false 时复查任务状态：FAILED（DEVICE_BUSY 竞态）→ 忙文案；回 PENDING（发送瞬间掉线）→ 留在 wait_result 交给 TaskMonitor 重派
- `wait_result` 用 **task_waiters 进程内事件**即时唤醒（DB 轮询 10s 兜底），不依赖 Monitor 扫描节奏
- AgentRun 与 Task 分离：一轮消息一个 Run（幂等键 message_id），业务执行一个 Task；回复先落库再推送，钉钉 webhook 故障不丢审计记录
- Dashboard / `GET /api/agent/runs` 可查每轮消息的 ack_reply / final_reply / task_id / 状态，全链路可观测

### 16.6 生产部署清单（当前实机配置）

| 端 | 配置 |
| --- | --- |
| 主电脑（服务端） | `.env`：`DINGTALK_CLIENT_ID/SECRET/ROBOT_CODE`、`OPENAI_API_KEY=<智谱 key>`、`OPENAI_API_BASE=https://open.bigmodel.cn/api/paas/v4`、`AGENTHUB_MODEL=glm-5.3-flash`；`uvicorn app.main:app` 启动；Dashboard 生成注册码；`server/config/agent_tools.json` 维护业务→设备白名单 |
| 子电脑（办公室电脑02） | 安装 Python 3.11+ 与影刀；`client/worker/capabilities.json` 填真实 `robot_uuid`/`shadowbot_path`（**路径用双反斜杠或正斜杠**，单反斜杠会 JSON 解析失败→能力上报为空→静默故障）；`python main.py --code <注册码> --name 办公室电脑02` 注册并常驻 |
| 钉钉开放平台 | 企业内部应用，开通机器人能力，Stream 模式（无需公网回调），可见范围覆盖目标群成员 |
| 公网分享 | `tailscale funnel 8000`（可选）；Dashboard 受 `AGENTHUB_ADMIN_TOKEN` 保护 |

---

## 17. V1.1 可靠性执行（Reliable-1.0）

> 目标：把「任务只会被一个调度器派发、一个执行者回报、终态不可逆、消息不重复消费」从约定变成**由状态机与 CAS 强制**的机制。规格见 `docs/V1.1_PLAN.md`。

### 17.1 双状态机（task/state.py）

```python
TASK_TRANSITIONS = {
    "PENDING":     {"DISPATCHING", "CANCELLED", "TIMEOUT"},
    "DISPATCHING": {"SENT", "PENDING", "CANCELLED", "FAILED"},   # PENDING: 发送失败回滚
    "SENT":        {"ACCEPTED", "RUNNING", "SUCCESS", "FAILED", "CANCELLED", "TIMEOUT"},
    "ACCEPTED":    {"RUNNING", "SUCCESS", "FAILED", "CANCELLED", "TIMEOUT"},
    "RUNNING":     {"SUCCESS", "FAILED", "CANCELLED", "TIMEOUT"},
    "SUCCESS": set(), "FAILED": set(), "CANCELLED": set(), "TIMEOUT": set(),   # 终态封闭
}
# ATTEMPT_TRANSITIONS 同构，另含 TIMEOUT→FAILED 的 retry 重开由 request_retry 显式重置
```

- `can_transition(kind, from, to)` 是唯一的状态变更裁判（unit 级全组合覆盖）。
- 终态集合 `TASK_TERMINAL_STATES / ATTEMPT_TERMINAL_STATES` 从转移表推导，天然一致。

### 17.2 设备事件门序（TaskService.handle_device_event）

设备回报按以下顺序过闸，**任何一闸拒绝都只记审计事件、绝不改状态**：

| 闸 | 规则 | 违例审计事件 |
| --- | --- | --- |
| 1. 归属 | attempt.device_id ≠ 上报设备 → 直接忽略 | （无事件，安全边界） |
| 2. 过期 | 事件带的 attempt_id ≠ step.current_attempt_id → 仅审计 | `task.late_event{reason: stale_attempt}` |
| 3. 终态 | 任务已终态（SUCCESS/FAILED/CANCELLED/TIMEOUT）→ 尝试结果按事实记账（若转移合法），任务不复活 | `task.late_result{reason: task_terminal}` |
| 4. 状态机 | `can_transition` 拒绝非法转移 → 仅审计 | `task.late_result{reason: illegal_transition}` |
| 5. 落账 | 记 `task_events`（始终带 attempt_id）→ commit → task_waiters 唤醒 | — |

- `task.result{success}` 成功路径：先置 step SUCCESS 并 **`db.flush()`**（SessionLocal 是 `autoflush=False`，不 flush 则 `next_pending_step` 查询看不见刚关闭的步骤 → 多步误报 advance）。
- `advance=True` 语义 = 「存在后续 PENDING 步骤，需要派发下一步」，由 API/WS 层异步触发 Dispatcher。

### 17.3 Dispatcher CAS 原子抢单

```python
claimed = db.query(Task).filter(
    Task.task_id == task_id, Task.status == "PENDING"
).update({"status": "DISPATCHING"}, synchronize_session=False)
if claimed != 1:
    return False   # 另一个调度器已赢
```

- 条件 UPDATE 是数据库层原子操作：两个调度器并发派发同一任务，恰好一个 `claimed==1`（测试用 `asyncio.gather` 强制交错验证）。
- 发送失败（0 连接/竞态掉线）→ 回滚 attempt、任务回 PENDING，Monitor 兜底重派。

### 17.4 结果-看门狗竞速（单胜者）

`TaskService.timeout_running` 双 CAS：

1. **Attempt CAS**：`UPDATE task_attempts SET status='TIMEOUT' WHERE attempt_id=? AND status IN (LIVE)` → 只有关在此刻仍活跃的尝试能被判超时；
2. **Task CAS**：`UPDATE tasks SET status='TIMEOUT' WHERE task_id=? AND status IN (LIVE)` → 结果回报与看门狗并发时**恰好一方赢**：
   - 结果先到 → 任务 SUCCESS；看门狗 CAS 0 行 → `notify_device=False`、状态原样返回（不产生第二个终态事件）；
   - 看门狗先到 → 任务 TIMEOUT；迟到的 SUCCESS 落入 §17.2 终态闸 → `task.late_result`，任务不复活。

### 17.5 AgentRun 消息幂等（UNIQUE(channel, message_id)）

- 迁移 0004：`UPDATE agent_runs SET message_id = NULL WHERE message_id = ''` + `UNIQUE(channel, message_id)`；
- 钉钉/API 重放或重试同一 `message_id` → 命中唯一键 → 返回**已有 run_id**（同轮消息不二次执行）；
- API 直连通道无 message_id → 存 NULL（SQLAlchemy 唯一约束对 NULL 不生效，每条自成 Run）。

### 17.6 服务器重启恢复

`TaskMonitor.recover_stuck_dispatching()`（启动时、Monitor 循环前执行）：

- `DISPATCHING` 任务 = 「CAS 抢单后、发送确认前崩溃」的僵尸态 → 任务回 `PENDING`，其 `DISPATCHING` attempt 置 `FAILED(error_code=SERVER_RESTART)`，记 `task.recovered` 事件，Monitor 随后正常重派；
- `SENT/ACCEPTED/RUNNING` 等已送达状态不动，由超时看门狗按 `timeout_at` 收敛。

### 17.7 设备在线语义与 Admin 鉴权加固

- **在线 = hub 实时连接数 > 0**（`DeviceService.to_out`，V1.1 §38 多连接聚合）：DB `devices.status` 是 HeartbeatMonitor 宽限期（45s）的滞后视图，不再作为 API 的在线判据 —— 一台设备两条连接，断一条仍在线（`connection_count` 同源）。
- **Device API 全部管理面护栏**：`GET /api/devices`（列表）补上 `require_admin`（此前漏护）；注册码创建 / 任务 / Agent / 能力查询路由级护栏在位。`AGENTHUB_ADMIN_TOKEN` 为空 = 开放模式（本机开发）。

### 17.8 可靠性测试矩阵（tests/agenthub/test_reliability.py + unit）

| 用例 | 验证点 |
| --- | --- |
| test_cas_claim_admits_exactly_one_dispatcher | 双调度器竞速 → 恰一个 `task.dispatching` 事件，任务回 PENDING 无残留 attempt |
| test_dispatch_to_online_device_creates_single_attempt | 全链路单尝试单派发（Monitor 扫描周期不重复派） |
| test_late_success_after_timeout_stays_timeout | TIMEOUT 后迟到 SUCCESS：任务不复活，`late_result{task_terminal}` |
| test_late_success_after_cancel_keeps_cancelled | 取消赢竞速 → CANCELLED 永久 |
| test_result_beats_watchdog_single_winner | 结果先到 → SUCCESS；看门狗 CAS no-op |
| test_stale_attempt_event_is_audit_only | 非当前 attempt 事件零状态变更 |
| test_agentrun_same_message_id_same_run / empty_message_id_never_collides | 消息幂等 / NULL 豁免 |
| test_device_api_requires_admin | 列表/撤销 401/200 边界 |
| test_multi_connection_device_stays_online | 双连接断一仍在线（count=1），全断离线 |
| test_server_restart_recovers_stuck_dispatching | DISPATCHING 僵尸恢复 → PENDING + SERVER_RESTART 账本 |

---

## 18. V1.2 Tool-Using Agent（Tool-Agent-1.0）

> 目标：把 Main Agent 从「MVP 固定管线」升级为**会思考的 Tool-Using Agent** —— LLM 在九个标准工具 + 真实任务引擎（V1.1 状态机/CAS/看门狗全量继承）上自主「规划 → 调用 → 观察 → 再决策」；高危操作用户确认；WAITING_USER 停靠可恢复；每一次调用全程审计。规格见开发文档 §20-§22/§36-§57/§122-§139。

### 18.1 目录结构（server/app/agent/）

```
agent/
├── core/              # 与图解耦的 Agent 内核
│   ├── state.py           # AgentState 契约（TypedDict；契约测试锁定键集，防 prompt 越权扩字段）
│   ├── context.py         # AgentContext：用户/会话上下文 + pending_confirmation（停靠参数）
│   ├── policies.py        # ToolPolicy：七闸门准入链 + 调用账本 + 审计写入
│   ├── runner.py          # AgentRunner.run() / resume()：装配图、预算、状态初始化
│   └── prompts.py         # llm_decide 提示词组装（system + human + 工具目录）
├── graph/
│   ├── nodes.py           # 8 个图节点（§18.3）+ 观察事实压缩 _compact
│   ├── routing.py         # 条件路由（决策分流 / 停靠 / 终态）
│   └── graph.py           # build_graph(runner, policy)
├── llm/
│   ├── schemas.py         # AgentDecision：action ∈ {tool_call, ask_user, finish}（Pydantic 收口）
│   ├── client.py          # 结构化输出客户端（function_calling 模式，拒绝自由文本）
│   ├── service.py         # 折叠重试：瞬时错误 / 输出解析失败 ≤ AGENT_MAX_LLM_RETRIES
│   └── errors.py          # LLMError 体系
├── tools/                 # 九个标准工具（§18.2）+ base.py（AgentTool/ToolResult/错误码）
├── service.py             # AgentService：API/钉钉入口 → Run 生命周期（§18.5）
├── runs.py                # AgentRunService：落库、WAITING_USER 停靠/重开、state_json 序列化
├── db_models.py           # AgentRun（V1.2 扩列）/ AgentToolCall
├── legacy/                # V1.0 完整 Agent 归档（§10），回归测试仍覆盖
└── mvp/                   # MVP 固定管线（§16），AGENT_MODE=mvp 默认
```

### 18.2 九个标准工具（tools/，build_default_registry 每次 fresh 构建）

| 工具 | 风险级 | 确认 | 说明 |
| --- | --- | --- | --- |
| `list_devices` | READ | — | 设备列表；在线=hub 实时连接数、busy=活跃任务实时视图（非滞后 DB） |
| `get_device_status` | READ | — | 单设备实时简报（同上语义） |
| `get_device_capabilities` | READ | — | 设备能力表（能否跑什么命令） |
| `get_recent_tasks` | READ | — | 最近任务（按设备/命令/状态过滤）——「昨天为什么失败」的诊断入口 |
| `get_task_detail` | READ | — | 任务完整详情：steps/attempts/最终结果事件 |
| `get_task_events` | READ | — | 生命周期事件流（写入前上下文压缩：去 payload 留错误码） |
| `retry_task` | WRITE | **恒需确认** | 走 TaskService.request_retry（RETRYABLE 态校验 + attempts 账本），派发后等待终态 |
| `cancel_task` | WRITE | **恒需确认** | 走 TaskService.request_cancel（非终态即可取消），best-effort task.cancel 下发 |
| `execute_command` | ACTION | `AGENT_CONFIRM_ACTIONS` | **唯一执行口**：命令必须存在于 Command Registry（enabled）+ params 对 params_schema 严格校验 —— `script_path`/`shell`/`python_code` 无处可落（幻觉参数被 schema 拒绝） |

工具纪律（PDF §23/§36-§37）：
- `args_schema` 一律 Pydantic `extra="forbid"`；LLM 只能填工具定义的参数，不能定义新参数。
- 工具永不直接写 SQL / 碰 WebSocket —— 只经 Service 层（TaskService/TaskDispatcher），V1.1 状态机是唯一真相源。
- 任何失败都是 `ToolResult.fail(error_code)`：**策略拒绝与业务失败都是给 LLM 的观察，不崩溃图**（observe → replan/ask_user/finish）。
- 每工具 `max_calls` + 每 Run 全局 `AGENT_MAX_TOOL_CALLS` 双重预算。

### 18.3 Agent 循环（LangGraph：understand → … → build_reply）

```
START → understand      目标重述（恢复 Run 时叠加用户补充）
      → load_context    设备/能力/命令快照 + 最近任务摘要（LLM 的世界状态）
      → plan            LLM 步骤规划（降级：直接单步执行，规划失败不阻塞）
      → llm_decide      AgentDecision{tool_call | ask_user | finish}
      → execute_tool    ToolPolicy 七闸门 → handler ──CONFIRMATION_REQUIRED→ ask_user（停靠）
      → observe         ToolResult → 紧凑事实（JSON 截断，facts 永远短于阈值）
      → evaluate        程序化护栏：全局/运行时预算；语义完成判断留给 LLM
      └→ llm_decide（循环） … → finish → build_reply → END
```

## 19. V1.3 Workflow Engine（Workflow-1.0）

> 里程碑：V1.3 在 V1.1 任务引擎与 V1.2 Tool-Using Agent 之上，落地**确定性多步业务编排**。LLM 只决定"跑哪个流程、填什么变量"（§18 Agent），步骤推进/传参/重试/取消/恢复全部由状态机完成 —— **编排链路零 LLM 参与**（§205）。`AGENT_MODE=mvp` 生产默认行为不变。

### 19.1 三引擎分工（L# 三层心智模型）

| 引擎 | 职责 | 决策方式 |
| --- | --- | --- |
| Agent 引擎（§18） | 理解意图 → 选择流程 → `run_workflow` | LLM 动态决策（仅选流程/填变量） |
| Workflow 引擎（§19） | 步骤推进 / 上下文传参 / 失败策略 / 取消 / 重启恢复 | 纯确定性状态机，DB 为唯一事实 |
| 任务引擎（§8/§17） | 单命令可靠执行（派发/幂等/重试/看门狗） | CAS + 事件门序（Workflow 不复刻） |

设计铁律：
- **单一入口**：Agent Tools 与 Admin API 都走 `WorkflowService`，二者都不直写编排 SQL（§31）。
- **永不绕过任务引擎**：步骤执行一律 `TaskService.create(source_type="WORKFLOW", workflow_run_id, workflow_step_run_id)`，派发/重试/超时归 Dispatcher/TaskMonitor 管（§72/§84）。
- **Task 是事实源**：步骤状态只从 Task 终态事实单向同步（§57/§58），错过通知由慢速安全网修复（§19.8）。
- **双重 CAS**：run `PENDING→RUNNING` 与 step `READY→RUNNING` 均条件更新；并发触发只可能有一个胜者，输家零副作用（§90）。

### 19.2 数据模型（表结构详见 §5）

五张表：`workflows`（定义 + `definition_json` 快照 + `active_singleton`）、`workflow_steps`（有序步骤定义）、`workflow_runs`（运行实例 + `context_json` + `current_step_run_id`）、`workflow_step_runs`（步骤实例，`task_id` 回链）、`workflow_events`（编排事件流）。`tasks` 扩列溯源字段；启动时 `ensure_task_source_columns` 补列（迁移缺省时兜底，测试直建表场景）。

### 19.3 双状态机（workflow/state.py）

```
workflow_runs:      PENDING → RUNNING → SUCCESS | FAILED | CANCELLED
                    PENDING → CANCELLED（启动前直接取消）
workflow_step_runs: PENDING → READY → RUNNING → SUCCESS | FAILED | CANCELLED
                    失败终局后其余未启动步骤 → SKIPPED
```

- 终态集合封闭（`WORKFLOW_TERMINAL_STATES` / `STEP_RUN_TERMINAL_STATES`），`can_workflow_transition` 拒绝一切非法/回退转移。
- **retry 不是 step 状态转移**：步骤重试复用 Task 引擎的 `request_retry`（Task 回到 PENDING 重派），step 停在 RUNNING 只累计 `retry_count`。

### 19.4 执行链路（事件驱动 + 慢速安全网）

```
create_run（active_singleton 查重 → 建 run + 全部 step_run(PENDING) + context_json）
  → start_run（CAS PENDING→RUNNING）→ advance
  → start_step（CAS READY→RUNNING → resolve_params → 选设备 → TaskService.create → 回填 task_id）
  → TaskDispatcher 照常派发 → Worker 执行 → task.result
  → task/events.notify_task_terminal（V1.3 新增观察者广播）
  → WorkflowRuntime.on_task_terminal（线程安全；非主循环时 call_soon_threadsafe 跳回）
  → engine.handle_task_result（按 Task 终态事实同步 step）→ advance → …
  → 全部 step SUCCESS → run SUCCESS → workflow_waiters.notify（唤醒等待者）
```

- **advance**：跳到首个 READY/PENDING 步骤启动；无活跃步骤且全 SUCCESS → run SUCCESS。每次调用只推进一步，天然串行。
- **start_step 内三道闸**：步骤定义缺失/禁用 → `WORKFLOW_STEP_INVALID`；参数解析失败 → `WORKFLOW_PARAM_RESOLUTION_FAILED`；无在线设备报所需能力或 TaskService 校验拒绝 → `WORKFLOW_TASK_CREATE_FAILED`。
- **设备选择（§113 确定性最小实现）**：步骤固定 `device_id` 优先；否则在 DB 在线设备中按名称序取首个上报该命令能力者 —— 全部校验仍由 Task Engine 四重校验兜底。
- **失败分支 `_handle_step_failure`**：`on_failure=retry` 且 `retry_count < max_attempts` 且错误码命中 `retry_on`（空 = 全部）→ `TaskService.request_retry` + `retry_count+1`，Task 保持 PENDING 由 Dispatcher 重发；预算耗尽或 `InvalidTaskState` → step FAILED + 后续 SKIPPED + run FAILED（`fail_workflow` CAS，取消方竞争失败方零副作用 §51）。
- **Task CANCELLED → 级联**：step CANCELLED + 未启动步骤 CANCELLED + run CANCELLED（用户/Admin 取消步骤任务时 run 跟随，§49）。
- **用户取消 `cancel_run`**：先 CAS run → CANCELLED（PENDING/RUNNING 均可），再处理步骤：RUNNING/READY → CANCELLED 并对未终态的 Task `request_cancel`（返回 `notify_task_id` 供 API 层下发 `task.cancel` 信封）；PENDING → CANCELLED。终态 run 取消 → `INVALID_WORKFLOW_STATE` 409。

### 19.5 上下文与参数解析（context.py / resolver.py）

- **上下文形状**：`{"variables": {...}, "steps": {name: {"status", "task_id", "result"}}}`。每步终态后 `record_step_result` 并整体持久化到 `workflow_runs.context_json`（重启安全，§87）。
- **模板语法**：`{{ variables.x }}`、`{{ steps.<步骤名>.result.<k> }}`（支持嵌套路径）。
- **解析规则（§80）**：
  - 模板占满整个字符串 → 取引用值**原类型**（对象/数字不字符串化）；
  - 模板混入普通文本 → 字符串替换（被引用值必须可字符串化，对象引用直接失败）；
  - 引用不存在的变量 / 未知步骤 / 路径缺失 / **未来步骤** / **失败步骤** → `WORKFLOW_PARAM_RESOLUTION_FAILED`（编排期即失败该步骤，绝不猜值、绝不空跑）；
  - 坏模板语法（`{{` 不闭合等）原样保留不报错。
- `resolve_params` 递归处理 dict/list/str。

### 19.6 Admin API（app/api/workflows.py，全部 admin）

| 组 | 端点 | 要点 |
| --- | --- | --- |
| 定义 | `POST/GET /api/workflows`、`GET /{id}` | 创建走 `WorkflowRegistry.validate`（命令白名单 + enabled + 重名步骤拒绝），创建后 DRAFT |
| 启停 | `POST /{id}/enable`、`/{id}/disable` | enable = 同名单旧版本自动停用 + **命令白名单重校验**（防注册表变化后带病激活） |
| 运行 | `POST /api/workflows/{workflow_id}/runs` | 创建即启动；`active_singleton` 冲突 → 409 `WORKFLOW_ALREADY_RUNNING` |
| 运行查询 | `GET /api/workflow-runs`（status/limit）、`/{run_id}`、`/{run_id}/events` | 详情含步骤列表与 context |
| 取消 | `POST /api/workflow-runs/{run_id}/cancel` | 语义见 §19.4；`notify_task_id` 非空时下发 `task.cancel` |

错误码映射：`WORKFLOW_NOT_FOUND`/`WORKFLOW_VERSION_NOT_FOUND` 404、`WORKFLOW_ALREADY_RUNNING`/`INVALID_WORKFLOW_STATE` 409、`WORKFLOW_INVALID` 400，其余 `WORKFLOW_*` 500。

### 19.7 Agent 工具（tools/workflow.py，默认工具集 9 → 14）

| 工具 | 风险 | 说明 |
| --- | --- | --- |
| `list_workflows` | READ | 列出已注册流程与启用状态 |
| `get_workflow` | READ | 查看定义（步骤/命令/参数来源） |
| `run_workflow` | ACTION + 确认 + waits_task | 启动流程并**等待终态**：waiter 事件优先、DB 轮询兜底（镜像任务 `_wait_terminal`），超时返回当前状态不撒谎（§119） |
| `get_workflow_run` | READ | 查询运行进度（当前步骤/各步结果） |
| `cancel_workflow_run` | ACTION + 确认 | 取消运行（当前步骤 Task 一并取消） |

Agent 只传 `workflow` 名 + `version`（可选）+ `variables` —— **步骤如何执行完全由引擎决定**，LLM 无法注入步骤级指令（§72/§77）。

### 19.8 WorkflowMonitor（慢速安全网，monitor.py）

终态观察者负责实时推进；monitor 每 `WORKFLOW_MONITOR_INTERVAL=5s` sweep 修复错过通知留下的缺口（§83/§120/§124）：

- `PENDING` run（create 与 start 之间崩溃）→ `start_run`；
- `RUNNING` run 的当前 Task 在 DB 已终态 → `handle_task_result` 重同步（**只补同步，绝不重建任务** §54）；
- `RUNNING` 步骤无 `task_id` 或 Task 不存在 → `WORKFLOW_ORPHAN_STEP` 失败（不可猜，§56）；
- 无活跃步骤但仍有未完成步骤 → `advance` 续跑。

启动时 `recover_from_restart()` 先扫一遍（main.py lifespan，TaskMonitor 同款结构）。它不复制任务超时/上线逻辑（§84）—— 单个 run 修复失败只记日志，不阻塞整个 sweep。

### 19.9 验收矩阵（tests/agenthub/test_workflow_run.py —— 10 场景全过，FakeWorker 全链路、工具零 mock）

| # | 场景 | 验证点 |
| --- | --- | --- |
| 1 | 三步流程成功 | run SUCCESS、各 step SUCCESS、事件流完整（started/step_ready/step_started/step_success/completed） |
| 2 | 跨步传参 | step2 参数引用 `steps.s1.result.*`（整值保类型）与 `variables.*` |
| 3 | 重试后成功 | 首败 → request_retry → 第 2 次 SUCCESS，run 不受影响 |
| 4 | 重试预算耗尽 | `max_attempts` 用尽 → step FAILED + run FAILED + 后续 SKIPPED |
| 5 | on_failure=stop | 重试走 Task 引擎 request_retry（非 LLM 决策），stop 策略立即终局 |
| 6 | 运行中取消 | run CANCELLED + 当前 Task CANCELLED + 剩余步骤 CANCELLED |
| 7 | 单例护栏 | `active_singleton` 运行中再启动 → 409 `WORKFLOW_ALREADY_RUNNING` |
| 8 | 错过通知修复 | 模拟丢失终态事件 → Monitor sweep 从 Task 事实重同步 → run SUCCESS |
| 9 | 孤儿步骤 | RUNNING 无 task_id → `WORKFLOW_ORPHAN_STEP` → run FAILED |
| 10 | CAS 防重复建任务 | 并发 handle_task_result / advance 竞争 → 每步骤只创建一个 Task |

### 19.10 V1.3 里程碑记录（2026-09-08）

- 新增 12 个 workflow 模块文件 + 5 张表 + `tasks` 溯源扩列 + 任务终态观察者（task/events.py）+ 10 个 Admin 端点 + 5 个 Agent 工具（目录 9 → 14）。
- 全量回归基线：**232 passed**（V1.2 基线 193 + workflow unit 29 + agenthub 集成 10）。
- 生产影响：零。`AGENT_MODE=mvp` 默认管线不变；Workflow 功能随迁移与启动钩子就位，创建/启用定义后才产生行为。

## 20. V1.4+V1.5 Capability Runtime 与远程执行（Capability-1.0 → Remote-Exec-1.0）

> 里程碑：V1.4 落地能力注册表三表（capabilities/capability_versions/capability_packages）+
> 包上传/发布/下载 API + Worker 侧 Lazy Pull（CapabilityManager/Cache/Puller，SHA256 校验）+
> Artifact 平面（上传/去重）。V1.5 在此之上打通**「程序包 + 数据源 → 指定 Worker 执行 → 产物回传」**
> 的完整真实链路，首个真实能力 `data.excel.preprocess`（Polars Excel 预处理）实机验证通过。

### 20.1 四元模型（V1.5 铁律）

```
Capability = 程序（包）      Artifact = 数据
Task       = 一次执行        Worker    = 执行地点
```

- WebSocket 只传控制与引用（task/capability.execute、input_artifacts 引用列表）；大文件一律走 HTTP 数据平面。
- Task 只保存 Artifact **引用**（`tasks.artifact_ids` JSON：`[{"artifact_id","role"}]`），永不保存本地路径。
- 业务 Capability 保持纯 CLI（`--input/--mapping/--output`），不 import 任何 AgentHub SDK；双模式同代码（本地 CLI / AgentHub env 注入）。

### 20.2 输入 Artifact 链路（§13/§15/§16/§26）

```
run_capability(capability, version?, device?, inputs{manifest输入名: [artifact_id]}, params?)
  → TaskService.create（校验引用存在）→ task.artifact_ids=[{artifact_id, role}]
  → Dispatcher._dispatch_capability：查 Artifact 表补 name/checksum
      capability.execute payload += input_artifacts:[{artifact_id, name, checksum, role}]
  → Worker TaskManager._prepare_inputs（validate 之前）：
      ArtifactDownloader.download（Bearer 设备令牌 + SHA256 校验 + <work>/artifact_cache/<checksum> 缓存）
      → 复制到 <exec>/<role 或 manifest.path>/ → params[<manifest输入名>] = 目录路径
  → PythonExecutor 注入 outputs（params[output_dir]=<exec>/output，自动建目录）
  → 执行 → result.json artifacts 或 output/ 扫描 → ArtifactUploader 上传 → task.result 只带 artifact 引用
```

- `GET /api/artifacts/{id}/download`：设备 Bearer 或 admin（V1.4 起仅 admin；V1.5 开放设备令牌，并修复 storage_path 相对 artifacts_root 的路径 bug）。
- 错误码（§33）：`ARTIFACT_NOT_FOUND`（创建时引用校验/派发时行缺失）、`ARTIFACT_DOWNLOAD_FAILED`、`ARTIFACT_CHECKSUM_MISMATCH`（不重试）、`PYTHON_ENV_CREATE_FAILED`、`DEPENDENCY_INSTALL_FAILED`。

### 20.3 Python Runtime（§29/§30/§34/§35）

- 包内 `requirements.txt` 存在时：`<work>/envs/<name>/<version>` 建 venv + `pip install -r requirements.txt`（cancel-aware 轮询等待，tempfile 输出防 Windows 管道死锁）；`.deps_ok` 标记存 requirements 哈希，命中即复用（无网络）。无 requirements 用 `sys.executable`。
- 业务成功判据 = exit 0 **且** 产物存在（result.json artifacts 或声明输出目录扫描），失败无产物 → FAILED。

### 20.4 Workspace（§17/§42）

```
<work>/executions/<attempt_id>/     # execution_id == attempt_id，天然任务隔离
├── input/    mapping/             # 输入 Artifact 落地（role 或 manifest.inputs[].path）
├── output/                        # 产物目录（manifest.outputs[].path，默认 output）
├── context.json  params.json      # 执行上下文留痕
<work>/capabilities/<name>/<version>/   # 包缓存（checksum 标记复用）
<work>/envs/<name>/<version>/           # venv 依赖缓存（.deps_ok）
<work>/artifact_cache/<checksum>/       # 数据缓存（§31 最简版，无 LRU）
```

### 20.5 首个真实能力：data.excel.preprocess

- 源：`D:\Slaes - 副本\preprocess_tool`（capability/ 程序本体 + sandbox/ 数据沙箱 + dist/ 构建产物；`build.py` 打包，sandbox 永不进包）。
- manifest：runtime=python，entrypoint=main，inputs=`data_dir`(path=input, required)/`mapping_dir`(path=mapping, required)，outputs=`output_dir`(path=output)。名称遵守 V1.4 三段式规范。
- E2E 验收（scripts/e2e_v15.py，单机 server+worker）：发布 1.0.0 → 上传 1 数据 + 4 映射 Artifact → 指定设备建 Task → Worker 拉包/建 venv/装依赖/下载输入/执行 Polars 流水线 → 产物回传下载校验（列增删正确）。二跑 3s（venv/包/数据三缓存全命中）。


- **决策只有三个动作**；`ask_user` 是一等公民结果而非错误（error=None，§47）。
- **LLM 永不轮询**：execute/retry/cancel 工具内部 `_wait_terminal` 等待终态（task_waiters 进程内事件即时唤醒 + DB 轮询兜底，上限 `AGENT_TOOL_WAIT_MAX`），返回带终态状态的 ToolResult。
- **观察即上下文**：observations 列表随状态跨停靠持久化，resume 后 LLM 仍看得见之前的工具事实。
- **运行时护栏**：evaluate 用单调时钟检查 `AGENT_MAX_RUNTIME`；超限置 AGENT_TIMEOUT 强制 finish（不依赖 LLM 自觉）。

### 18.4 风险控制：确认、恢复与注入防篡改

| 机制 | 实现 |
| --- | --- |
| 七闸门准入 | 存在 → enabled → args 严格校验（extra=forbid）→ 权限 → 确认 → 每工具/全局限额 → 执行；任何闸门拒绝 = 带 error_code 的观察 |
| 确认停靠 | requires_confirmation 工具未带 confirmed → `CONFIRMATION_REQUIRED`（data 携带 pending_args）→ ask_user 节点停靠 → AgentRun 置 `WAITING_USER`，序列化 AgentState 落 `state_json` |
| 恢复执行 | 用户回复 → resume()：反序列化状态 + 叠加补充 + 重建 ToolPolicy（`used_total` 续算全局预算）→ 重进图 |
| **确认防注入** | 肯定答复（是/确认/ok…）→ **跳过 LLM**，以 `confirmed=True` 直执行停靠参数（路由短路）；LLM 全程不再被咨询 —— 被提示注入的模型无法改参数（§57）。否定答复 → 取消该调用，作为观察回主循环 |
| 多轮入口 | 钉钉/`POST /api/agent/message`：同会话存在 WAITING_USER Run → 新消息自动恢复；API 也可显式 `POST /runs/{id}/message` |
| Run 取消 | `POST /runs/{id}/cancel` 只关 Run；**业务任务不自动取消**（用户须显式 cancel_task —— Run ≠ Task 边界，§90） |

已知限制（记录在 §15）：恢复时每工具账本重启（全局预算连续，state.tool_call_count 保持一致）。

### 18.5 服务层语义（AgentService）

- `handle_message`：message_id 幂等（Stream 重放/重试返回既有 run_id）→ 会话内可恢复 Run 优先 → 否则新建 Run 后台图执行（HTTP/WS 零阻塞）。
- `resume_run`：状态校验（非 WAITING_USER → 409）→ `reopen` WAITING_USER→RUNNING（原子：竞争恢复者失败）→ 图续跑 → `_finalize` 落终态（或再次停靠）。
- `tool_call_count` 随 finish 落库；`GET /api/agent/runs/{id}` 全量可观测（状态/最终回复/工具调用数）。

### 18.6 审计（agent_tool_calls，迁移 0005）

- **ToolPolicy 是唯一写入方**（工具 handler 永不触碰）：准入即 RUNNING，返回落 SUCCESS/FAILED，准入拒绝落 REJECTED。
- 大载荷写入时截断（存摘要不存巨 blob）；`run_id` 索引；Run 行 `tool_call_count` 为权威计数。
- 排障路径：run_id → agent_tool_calls 全序列（参数/结果/错误码/时延）+ task_events 业务侧事件，双侧对账。

### 18.7 AGENT_MODE 开关与装配（main.py）

```
AGENT_MODE=mvp        → MvpAgentService（§16 固定管线，默认，生产行为不变）
AGENT_MODE=tool_agent → AgentService（§18 Tool-Using Agent）
```

- 装配点唯一（main.py 启动时按模式二选一），钉钉消息与 `/api/agent/message` 共用装配；回滚 = 改环境变量重启。
- 两模式共享：AgentRun 表 / Dashboard / TaskService / DeviceLink / 钉钉 Stream 网关。

### 18.8 验收矩阵（tests/agenthub/test_acceptance_v12.py —— 10 场景全过，LLM 脚本化、工具零 mock）

| # | 场景 | 验证点 |
| --- | --- | --- |
| 1 | 运行命令 | execute_command → 真实 Task SUCCESS（FakeWorker 全链路），回复事实正确 |
| 2 | 查询结果 | get_task_detail 观察含 SUCCESS，tool_call_count=1 |
| 3 | 诊断失败 | 失败原因（EXECUTOR_FAILED/boom）进入回复 |
| 4 | Retry 确认 | WRITE 确认停靠 → 肯定答复恢复执行 → 任务 SUCCESS |
| 5 | 取消执行中任务 | SENT（派发后未接受）确认后 CANCELLED |
| 6 | 设备不存在 | DEVICE_NOT_FOUND 观察 → ask_user 停靠；**不创建必败任务** |
| 7 | 设备离线 Replan | 任务排队 PENDING + 查实时状态 + 事实性回复（不撒谎） |
| 8 | 迟到事件 | CANCELLED 后迟到 SUCCESS → `task.late_result` 审计，任务不复活 |
| 9 | 重复调用 | 每工具 max_calls=2 → 第 3 次 `MAX_CALLS_EXCEEDED` 观察，LLM 体面收尾 |
| 10 | 危险调用拒绝 | script_path 参数走私 → `INVALID_ARGS`；未注册命令 → `COMMAND_NOT_FOUND`；零任务产生 |

### 18.9 V1.2 里程碑记录（2026-09-08）

- 迁移 0005：`agent_tool_calls` 表 + `agent_runs` 扩列（state_json/tool_call_count）， downgrade 完整。
- 全量回归基线：**193 passed**（unit 契约/循环/确认/恢复/审计/服务 + agenthub 工具集成/Run API/10 场景验收）。
- `AGENT_MODE=tool_agent` 生产切换待实机验证（当前生产继续跑 mvp 模式，行为零变化）。

