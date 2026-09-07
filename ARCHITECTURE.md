# AgentHub V1.0 开发架构文档（当前实现快照）

> 版本：AgentHub V1.0（底层 DeviceLink V1.0 已含 2026-09-05 公网架构调整）· 更新日期：2026-09-07
> 测试基线：`pytest tests/` → **38 passed**（unit / integration / agenthub 验收）

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
│       ├── agent/                     # Main Agent（LangGraph）
│       │   ├── state.py               # AgentState（图内飞行状态，≠ DB 任务状态）
│       │   ├── tools.py               # Agent 可调用工具（封装各 Service）
│       │   ├── planner.py             # LLM 规划 + 规则回退规划
│       │   ├── graph.py               # StateGraph 装配（节点/条件路由）
│       │   ├── service.py             # AgentService.run(astream 驱动)
│       │   └── prompts/               # planner_system.txt（LLM JSON 契约）
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
│       │   └── agent.py               # POST /api/agent/run（admin）
│       └── static/index.html          # Dashboard v2（登录+设备/任务/命令/能力/Agent）
├── client/
│   ├── main.py                        # DeviceClient 入口（注册→连接→重连循环）
│   ├── identity.py / storage.py       # 本地身份持久化（DEVICELINK_HOME）
│   ├── registration.py / auth.py      # 注册流程 / TokenManager
│   ├── websocket.py / reconnect.py / heartbeat.py
│   ├── protocol.py                    # 与服务端同构的 Envelope
│   └── worker/
│       ├── manager.py                 # TaskManager（接收/去重/并发1/取消/回报）
│       ├── registry.py                # Executor Registry（capabilities.json）
│       ├── executor.py                # Executor 基类 + ExecutionError
│       ├── capabilities.json          # 本机能力声明
│       ├── executors/echo.py / executors/python_executor.py
│       └── scripts/python_demo.py     # 预注册脚本（约定映射）
└── tests/
    ├── conftest.py                    # 文件型 SQLite + TestClient 夹具
    ├── unit/                          # protocol/reconnect/registration/token
    ├── integration/                   # 注册流 / WebSocket 流
    └── agenthub/
        ├── _worker.py                 # FakeWorker（线程内 WS 会话 + 自动应答）
        ├── test_acceptance.py         # 验收：隔离/离线重派/幂等/重试账本
        └── test_agent.py              # 规则规划器单测
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
| `task.dispatch` | S→C | 派发：`{task_id, step_id, command, params, timeout}` |
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
- Worker 端：`(task_id, step_id)` 去重 —— 重复派发只回报当前状态（`task.running`），绝不二次执行（验收用例覆盖）。

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
| `OPENAI_API_KEY` / `OPENAI_API_BASE` / `AGENTHUB_MODEL` | 空 / 空 / `gpt-4o-mini` | LLM 规划（无 Key 自动回退规则规划） |
| `AGENT_POLL_INTERVAL` / `AGENT_MAX_WAIT` | `2` / `300` | Agent 等待轮询间隔 / 上限（秒） |
| `DEVICELINK_HOME` | 平台默认 | 客户端身份文件目录（测试重定向） |

## 13. 安全设计

- **凭证不落明文**：注册码 / Device Token 仅存 SHA256，明文只在创建时展示一次；撤销即时生效（4403 踢线 + 客户端停止重连）。
- **Admin Token 与 Device Token 分离**：管理面全部 `require_admin` 护栏；设备面 Bearer Token + 仅能回报派发给自己的 attempt。
- **命令白名单 + 双注册表**：Server 注册表定义允许项；Worker 只执行本地预注册脚本（命令名→约定脚本名，正则白名单 `^[A-Za-z0-9_]+\.py$`，拒绝路径注入）；**Agent 永远不能传递可执行路径**。
- **参数走私防护**：params_schema 严格校验（必填 + 类型 + 禁止额外键）。
- **幂等性**：重复派发不重复执行（Worker 去重）；能力整体替换幂等。
- **网络**：服务只绑 127.0.0.1，公网经 Tailscale Funnel（WireGuard 加密隧道）。

## 14. 测试体系（38 passed）

| 层 | 文件 | 覆盖 |
| --- | --- | --- |
| unit | test_protocol / test_reconnect / test_registration_service / test_token | 信封解析、指数退避、一次性码生命周期、Token 哈希 |
| integration | test_registration_flow / test_websocket_flow | 注册→连接→心跳→撤销全流程 |
| agenthub | test_acceptance / test_agent | **验收**：MVP 链路、多设备隔离（B 永远收不到 A 的任务）、离线重派、重复派发幂等、重试 attempts 账本与 409、规则规划器 |

测试基建要点（踩坑后固化）：
- **文件型 SQLite**：TestClient 的 WS 会话各自运行在线程/portal 上，共享内存连接（StaticPool）会跨会话破坏事务 → `conftest.py` 注入文件型 DB；`database.py` 仅对纯内存 SQLite 启用 StaticPool。
- **FakeWorker**：线程内 `websocket_connect` + 自动应答（success/fail/silent/caps_only 四种行为）；`stop()` 必须 `session.__exit__()`（starlette 的 `close()` 不会解除 `receive_json` 阻塞）。
- **时序确定性**：建任务前用 `wait_for_capabilities` 等能力上报落库，规避异步上报竞态。
- 诊断手段：`pytest --timeout-method=thread --capture=no` + `py-spy dump --pid`。

## 15. 已知限制与演进方向（V1.1+）

| 现状限制 | 演进方向 |
| --- | --- |
| 任务单目标设备（target_device_id 必填） | 跨设备多步工作流（task_steps.device_id 列已预留） |
| Worker 并发 = 1，执行中断线回报丢失 | 并发可配 + 本地结果缓存/重连补报 |
| 单步命令白名单（echo / python.demo） | 影刀 RPA（yingdao）执行器、更多预注册脚本 |
| 轮询等待（Agent / Monitor 均为周期扫描） | 事件驱动（DB notify / 内部总线） |
| LLM 规划单次调用、无记忆 | 对话式多轮 Agent、执行结果反馈学习 |
| Dashboard 轮询刷新 | WebSocket 推送实时刷新 |
