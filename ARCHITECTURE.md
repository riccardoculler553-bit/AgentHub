# AgentHub V1.0 开发架构文档（当前实现快照）

> 版本：AgentHub V1.0 + MVP-Real-1.0 + Routing-1.0（底层 DeviceLink V1.0 已含 2026-09-05 公网架构调整）· 更新日期：2026-09-07（晚）
> 测试基线：`pytest tests/` → **64 passed**（unit / integration / agenthub 验收 / MVP / 影刀执行器 / 路由）
> 里程碑：**MVP 已全链路实机跑通**（钉钉群 @机器人 → GLM 意图分析 → 设备路由 → 子电脑影刀执行 → 日志级完成判定 → 群内回复），见 §16。

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
│   │   └── 0002_agenthub_tables.py    # AgentHub 5 张表（commands/capabilities/tasks…）
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
│       │   ├── service.py             # 生命周期 + 设备事件账本
│       │   ├── dispatcher.py          # 派发器（attempt 创建/信封组装）
│       │   ├── monitor.py             # 后台循环：PENDING 重派 + 超时看门狗
│       │   └── device_link.py         # AgentHub ↔ DeviceLink 唯一缝隙
│       ├── agent/                     # Main Agent（LangGraph，完整规划式，§10）
│       │   ├── state.py               # AgentState（图内飞行状态，≠ DB 任务状态）
│       │   ├── tools.py               # Agent 可调用工具（封装各 Service）
│       │   ├── planner.py             # LLM 规划 + 规则回退规划
│       │   ├── graph.py               # StateGraph 装配（节点/条件路由）
│       │   ├── service.py             # AgentService.run(astream 驱动)
│       │   └── prompts/               # planner_system.txt（LLM JSON 契约）
│       ├── agent/mvp/                 # MVP Agent（单命令意图链，已实机跑通，§16.1）
│       │   ├── schemas.py             # ExecutionIntent（意图+命令+点名设备）
│       │   ├── analyzer.py            # GLM function_calling 意图分析 + 规则兜底
│       │   ├── tools.py               # AgentToolRegistry（agent_tools.json 加载/缓存）
│       │   ├── graph.py               # analyze→resolve→create_task→dispatch→wait_result→build_reply
│       │   └── service.py             # MvpAgentService.handle_message（内存 run 表 + 后台图执行）
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
│       │   ├── agent.py               # POST /api/agent/run（admin，完整 Agent）
│       │   └── (mvp) POST /api/agent/message · GET /api/agent/runs  # MVP 链路（§16.1）
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
    ├── unit/                          # protocol/reconnect/registration/token
    ├── integration/                   # 注册流 / WebSocket 流
    └── agenthub/
        ├── _worker.py                 # FakeWorker（线程内 WS 会话 + 自动应答）
        ├── test_acceptance.py         # 验收：隔离/离线重派/幂等/重试账本
        ├── test_agent.py              # 规则规划器单测
        ├── test_tools.py              # AgentTool 注册表 + 意图分析（规则模式）
        ├── test_mvp.py                # MVP 验收：attempt_id/未知设备/离线/忙跃迁/不支持意图
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

## 5. 数据模型（10 张表）

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
| `tasks` | 任务：task_id/name/created_by/target_device_id/status/priority/max_attempts/timeout_at |
| `task_steps` | 步骤：task_id + order_no + command + params(JSON) + status；V1.0 继承任务 target_device_id（列保留给未来跨设备工作流） |
| `task_attempts` | 尝试账本：task_id + step_id + attempt_no + status + dispatch_message_id + error_code/message。每次派发/重试都新增一行 |
| `task_events` | 全生命周期事件流：task/step/attempt + event_type + payload。供 Dashboard Timeline / 审计 / Agent 状态恢复 |
| `agent_runs` | **AgentRun**（MVP）：一次用户请求一行 —— run_id/channel/conversation_id/message_id/input_text/status(RUNNING/SUCCESS/FAILED)/task_id/ack_reply/final_reply/reply_webhook/error/created_at/finished_at。Run ≠ Task：一轮对话一个 Run，业务执行一个 Task；Run 是会话轮次审计（可扇出多 Task），Task 活过服务重启。回复**先落库再推送**，sender 故障不丢记录 |

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

## 10. Main Agent（LangGraph）

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

## 14. 测试体系（64 passed）

| 层 | 文件 | 覆盖 |
| --- | --- | --- |
| unit | test_protocol / test_reconnect / test_registration_service / test_token | 信封解析、指数退避、一次性码生命周期、Token 哈希 |
| integration | test_registration_flow / test_websocket_flow | 注册→连接→心跳→撤销全流程 |
| agenthub | test_acceptance / test_agent | **验收**：多设备隔离（B 永远收不到 A 的任务）、离线重派、重复派发幂等、重试 attempts 账本与 409、规则规划器 |
| agenthub | test_tools | AgentTool 注册表（加载/校验/重载）+ 意图分析（规则模式：关键词、设备名最长匹配、不支持意图） |
| agenthub | test_mvp | **MVP 验收**：成功链路（ack/final 回复 + attempt_id 三元组）、未知设备、离线、忙设备、忙跃迁路由、不支持意图 |
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
| MVP 意图单轮、无对话记忆 | 对话式多轮 Agent（追问澄清、多任务编排） |
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
