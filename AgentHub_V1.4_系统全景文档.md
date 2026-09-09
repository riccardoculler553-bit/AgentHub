# AgentHub V1.4 当前系统全景文档

> 本文档基于对当前源码的逐模块逆向审计撰写（2026-09-09）。目标：准确回答"截至当前源码，AgentHub 到底已经做到了什么、代码实际是怎么运行的"。只陈述代码中真实存在的行为，不混入规划性描述。

***

## 1. 系统定位

AgentHub 是一个"对话驱动的设备任务编排系统"：用户通过钉钉（或 HTTP API）用自然语言提出请求，服务端 LLM Agent 将请求翻译为对受管设备的任务/工作流调用，设备端 Worker 执行（影刀 RPA / Python / HTTP / 本地命令）并回传结果与产物文件。

四层平面架构：

| 平面                                   | 职责                             | 主要代码                                                                                                                                                                                                                                                                                |
| ------------------------------------ | ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Reasoning Plane 推理平面                 | LLM Agent：理解→上下文→计划→决策→工具循环→答复 | [server/app/agent/](file:///d:/Websocket/server/app/agent)                                                                                                                                                                                                                          |
| Control Plane 控制平面                   | 任务状态机、分发、工作流引擎、能力运行时、产物        | [server/app/task/](file:///d:/Websocket/server/app/task), [server/app/workflow/](file:///d:/Websocket/server/app/workflow), [server/app/capability\_runtime/](file:///d:/Websocket/server/app/capability_runtime), [server/app/artifact/](file:///d:/Websocket/server/app/artifact) |
| Communication Plane 通信平面（DeviceLink） | 设备注册/认证、WebSocket 长连接、消息协议、心跳  | [server/app/websocket/](file:///d:/Websocket/server/app/websocket), [server/app/device/](file:///d:/Websocket/server/app/device)                                                                                                                                                    |
| Execution Plane 执行平面                 | Worker 端：任务消费、执行账本、执行器、能力安装    | [client/worker/](file:///d:/Websocket/client/worker)                                                                                                                                                                                                                                |

***

## 2. 目录结构（关键部分）

```
d:\Websocket
├── server\                       # 服务端（FastAPI 单体）
│   ├── app\
│   │   ├── main.py               # 应用入口：路由注册、lifespan
│   │   ├── core\config.py        # 全部配置项（环境变量/.env）
│   │   ├── db\                   # database.py（engine/SessionLocal）、models.py（基类/utcnow）
│   │   ├── agent\                # 推理平面
│   │   │   ├── core\state.py     # AgentState TypedDict
│   │   │   ├── graph\graph.py    # LangGraph 主图
│   │   │   ├── tools\            # base/registry/policy + device/task/command/workflow/capability
│   │   │   └── service.py        # AgentService（MVP/完整模式选择）
│   │   ├── task\                 # state.py 状态机、service.py、dispatcher.py、monitor.py
│   │   ├── workflow\             # engine.py、monitor.py、resolver、registry、db_models
│   │   ├── capability_runtime\   # service.py、manifest.py、package_service.py、errors.py
│   │   ├── artifact\service.py   # 产物上传/下载/去重
│   │   ├── command\service.py    # 命令注册表（yingdao.*）
│   │   ├── device\               # 注册码、token、设备服务
│   │   ├── websocket\hub.py      # 内存连接中心
│   │   ├── dingtalk\             # Stream 模式接入、消息解析、回复
│   │   └── <各域>\api.py         # 各域 REST API
│   └── config\agent_tools.json   # 业务工具→命令→设备白名单
├── client\                       # Worker 端
│   ├── main.py                   # 注册→连接→消费 主循环
│   └── worker\
│       ├── manager.py            # TaskManager：intake/consumer/execute
│       ├── ledger.py             # ExecutionLedger（worker.db，attempt 幂等）
│       ├── capability\           # CapabilityManager、manifest 校验、包安装
│       └── executors\            # 影刀/命令/能力执行器
├── tests\                        # unit / integration / agenthub 三层
└── ARCHITECTURE.md               # V1.3 架构文档（历史版本对照）
```

***

## 3. 实际启动链路

### 3.1 Server（[server/app/main.py](file:///d:/Websocket/server/app/main.py)）

1. 创建 FastAPI app，注册各域路由 + 异常处理器（错误码→HTTP 状态映射在 errors.py）。
2. lifespan 启动：建表（SQLite/数据库）、启动后台协程——TaskMonitor（任务超时/离线补漏）、WorkflowMonitor（[monitor.py](file:///d:/Websocket/server/app/workflow/monitor.py)：每 `WORKFLOW_MONITOR_INTERVAL=5`s 扫描 PENDING/RUNNING 工作流并调用 `engine.recover_run` 修复，需补分发的任务交 TaskDispatcher）。
3. 若配置了钉钉凭证，启动钉钉 Stream 模式客户端（无需公网回调）。
4. 生产以 `AGENT_MODE=mvp` 运行（默认值），选择 MvpAgentService 路径。

### 3.2 Worker（[client/main.py](file:///d:/Websocket/client/main.py)）

1. 首次启动：用一次性注册码换取 device\_token（注册码 5 分钟过期、用后作废）。
2. 携 token 建立 WebSocket（`open_timeout=30s`，防中转网络握手超时）；断线指数退避重连。
3. 上报 `device.capabilities`（能力上报成功提交后才可被派单）。
4. 启动 TaskManager：intake 队列 + consumer 循环；本地 `~/.devicelink/worker.db` ExecutionLedger 记录 attempt，保证重复 dispatch 幂等。
5. INFO 级日志同时输出控制台与 `client/logs/worker.log`（自动轮转）。

***

## 4. Agent 子系统（推理平面）

### 4.1 输入通道

- **DingTalk**：Stream 模式接收私聊/群聊消息；群聊 @ 消息处理时不校验 atUsers 与 robotCode（有意放宽的 MVP 边界）。回复经机器人发送，节流 `DINGTALK_THROTTLE_SECONDS=30`。
- **HTTP API**：Agent Runs API（创建运行、回复等待中的运行、查询状态/轨迹）。

### 4.2 AgentState（[state.py](file:///d:/Websocket/server/app/agent/core/state.py)）

TypedDict 上下文，字段分五组：run identity（run\_id/user\_request/channel/conversation\_id/sender\_id/resumed/user\_reply）、context & plan（context/plan/current\_goal）、tool loop（tool\_name/tool\_args/tool\_result/observations/decision）、limits（tool\_call\_count/llm\_retry\_count/started\_at/confirmed）、errors & output（error/final\_answer/reply/paused）。

### 4.3 LangGraph 主图（[graph.py](file:///d:/Websocket/server/app/agent/graph/graph.py)）

节点：`understand → load_context → plan`；条件路由：

- plan 之后：直接进 `llm_decide` 或（已有明确工具意图时）直通 `execute_tool`；
- llm\_decide 之后：`execute_tool` / `ask_user` / `build_reply`；
- execute\_tool 之后：`observe → evaluate`，或 `ask_user`；
- evaluate 之后：回 `llm_decide`（继续循环）或 `build_reply`；
- `ask_user` → END（run 停在 WAITING\_USER，`paused=True`，等待用户回复后 resume，resume 时跳过 llm\_decide 直接执行已确认工具——`confirmed` 标志）。

### 4.4 限额与闸门（config.py 默认值）

| 配置                                             | 默认    | 含义                |
| ---------------------------------------------- | ----- | ----------------- |
| AGENT\_MAX\_TOOL\_CALLS                        | 8     | 单次 run 工具调用上限     |
| AGENT\_MAX\_LLM\_RETRIES                       | 2     | LLM 重试上限          |
| AGENT\_MAX\_RUNTIME                            | 120s  | run 运行时守卫         |
| AGENT\_CONFIRM\_ACTIONS                        | false | ACTION 级工具是否需用户确认 |
| AGENT\_RUN\_MAX\_WAIT / AGENT\_TOOL\_WAIT\_MAX | 1900s | 等待任务完成的上限         |
| AGENT\_POLL\_INTERVAL                          | 2s    | 任务结果轮询间隔          |

### 4.5 Tool 系统（[tools/](file:///d:/Websocket/server/app/agent/tools)）

- `AgentTool` 基类：name/description/args\_schema(pydantic)/risk\_level/requires\_confirmation/waits\_task 等；`validate_args` + `run`。
- `ToolRegistry` 注册；`ToolPolicy` 按 READ/WRITE/ACTION 分级，ACTION 可要求确认，确认后 resume 不再过 llm\_decide。
- 当前注册 **17 个工具**（V1.2 九工具 + V1.3 五个工作流工具 + V1.4 三个能力工具）：

| 工具                                                              | 级别     | 确认                        | 来源                                                                                              |
| --------------------------------------------------------------- | ------ | ------------------------- | ----------------------------------------------------------------------------------------------- |
| list\_devices / get\_device\_status / get\_device\_capabilities | READ   | 否                         | device.py                                                                                       |
| get\_recent\_tasks / get\_task\_detail / get\_task\_events      | READ   | 否                         | task.py                                                                                         |
| retry\_task / cancel\_task                                      | WRITE  | 是                         | task.py                                                                                         |
| execute\_command                                                | ACTION | 随 AGENT\_CONFIRM\_ACTIONS | command.py（pre-validate：require\_executable→COMMAND\_NOT\_FOUND、validate\_params→INVALID\_ARGS） |
| list\_workflows / get\_workflow / get\_workflow\_run            | READ   | 否                         | workflow\.py                                                                                    |
| run\_workflow                                                   | ACTION | 随 AGENT\_CONFIRM\_ACTIONS | workflow\.py                                                                                    |
| cancel\_workflow\_run                                           | WRITE  | 是                         | workflow\.py                                                                                    |
| list\_capabilities / get\_capability                            | READ   | 否                         | capability.py                                                                                   |
| run\_capability                                                 | ACTION | 随 AGENT\_CONFIRM\_ACTIONS | capability.py                                                                                   |

- `waits_task` 工具（execute\_command/run\_workflow/run\_capability/retry\_task）会阻塞在工具内轮询任务终态（`AGENT_TOOL_WAIT_MAX`），把任务结果作为 observation 喂回 LLM——这是"对话闭环"的关键机制。

### 4.6 业务工具映射（[agent\_tools.json](file:///d:/Websocket/server/config/agent_tools.json)）

MVP 业务配置仅一条：`audit`（label "Text1"）→ command `yingdao.audit`，设备白名单 `["办公室电脑02"]`。`AGENT_DEFAULT_DEVICE_NAME` 现在只是帮助文本示例，真实设备范围由 business→command→devices 白名单决定。

***

## 5. Task Engine（控制平面）

### 5.1 状态机（[task/state.py](file:///d:/Websocket/server/app/task/state.py)）

```
PENDING → DISPATCHING → SENT → ACCEPTED → RUNNING → SUCCESS/FAILED/CANCELLED/TIMEOUT
DISPATCHING 可回落 PENDING（dispatch 失败重排）
终态：SUCCESS | FAILED | CANCELLED | TIMEOUT（无出边）
Attempt 终态额外含 STALE；Attempt 状态机与 Task 状态机独立维护
```

`can_transition(kind, current, target)` 是所有状态变更的唯一闸门；DISPATCHING 只存在于 dispatcher 的 CAS 认领内部，对外可见的第一状态是 SENT。

### 5.2 服务与分发

- `TaskService.create`：创建 Task + TaskSteps + 首个 Attempt；source\_type 标记来源（AGENT/WORKFLOW/API）；execution\_type 区分 COMMAND / CAPABILITY（CAPABILITY 必须带 capability\_version）。
- `TaskDispatcher`：CAS 认领（PENDING→DISPATCHING）→ 经 Hub 找设备连接 → `task.dispatch` → SENT；设备无响应/掉线由 TaskMonitor 兜底（重排或超时）。
- `TaskEvent` 记录全生命周期事件（state\_changed/dispatch/accept/result/cancel…），是审计与 Agent `get_task_events` 的数据源。
- 超时/取消：CANCELLED/TIMEOUT 为终态；Worker 侧 cancel 通过 `task.cancel` 广播，运行中的执行器监听 cancel event，成功结果若在取消后到达会被改判 CANCELLED。

***

## 6. Workflow Engine（控制平面）

### 6.1 实体与执行链（[engine.py](file:///d:/Websocket/server/app/workflow/engine.py)）

`Workflow`（定义）→ `WorkflowStep`（串行步骤：command 或 capability\_version）→ `WorkflowRun`（实例，status: PENDING/RUNNING/SUCCESS/FAILED/CANCELLED）→ `WorkflowStepRun`（READY/RUNNING/SUCCESS/FAILED/SKIPPED…）→ 每步创建真实 Task（source\_type=WORKFLOW，回写 task\_id）。

- `create_run/start_run/start_step`：start\_step 用 CAS（READY→RUNNING）防并发重复启动，写 `workflow.step_started` 事件后提交并重读 run。
- 参数解析：`resolve_params(step.params, run.context_json, valid_steps)` 支持引用前序步骤产物/上下文；解析失败 → `WORKFLOW_PARAM_RESOLUTION_FAILED`，步骤失败。
- 设备选择：command 步骤若未指定 device\_id，则 `_pick_device(command)` 找上报过该能力的在线设备；找不到 → `WORKFLOW_TASK_CREATE_FAILED`。capability 步骤直接走 CAPABILITY 执行路径。
- `handle_task_result`：任务终态回填步骤；成功 → 推进下一步，失败 → 终止 run（当前为纯串行，无分支/并行/重试策略）。
- `WorkflowMonitor`（[monitor.py](file:///d:/Websocket/server/app/workflow/monitor.py)）：实时事件推进为主，5s 扫描兜底——修复 server 重启/崩溃遗漏：RUNNING run 的当前任务已终态→重同步；RUNNING 步骤无 task\_id→孤儿检测；PENDING run→补 start；无活动步骤但有未完成步骤→补 advance。启动时执行一次 `recover_from_restart`。

***

## 7. Worker 端（执行平面）

### 7.1 TaskManager（[client/worker/manager.py](file:///d:/Websocket/client/worker/manager.py)）

- `on_dispatch` / `on_capability_execute`：去重（ExecutionLedger 中已有该 attempt 且非终态则跳过）→ 入 intake 队列 → `_intake` 回 `task.accept` → consumer 循环取任务执行。
- `_execute`：mark\_running → 上报 `*.running` → 按 kind 分派 `_run_task`（命令）或 `_run_capability`（能力）→ 产出 `_Outcome(terminal, result, error)`。异常兜底：executor 崩溃不杀死 consumer，转为 `EXECUTOR_FAILED`。取消竞争：cancel 已置位时成功结果改判 cancelled。
- 上报：`*.progress`（进度回调）、`*.result`（success/cancelled/failed + error\_code/error\_message + ledger\_row\_id 供服务端对账）。
- 超时错误 `EXECUTOR_TIMEOUT` 在账本记为 TIMEOUT，其余失败记 FAILED。

### 7.2 ExecutionLedger（worker.db）

SQLite 本地账本：attempt 幂等（重复 dispatch 不重复执行）、状态落盘（重启后可识别 STALE）、结果对账依据。

### 7.3 执行器

- **影刀执行器**：以影刀日志文件的结束标记为**唯一**完成判据（不依赖进程退出码）——Windows 下进程终止是异步的，轮询 Popen.poll()。
- **命令执行器**：pre-validate 后执行注册命令。
- **能力执行器**：执行已安装 capability 包（见 §9）。

***

## 8. DeviceLink 通信平面

### 8.1 注册与认证

- 一次性注册码（5 分钟过期）→ 换 device\_token；token 持久化于 device\_tokens 表。
- WebSocket 握手后建立 `DeviceConnection`，登记进 Hub。

### 8.2 Hub（[hub.py](file:///d:/Websocket/server/app/websocket/hub.py)）

纯内存结构：`_connections: dict[connection_id → DeviceConnection]` + `_device_connections: dict[device_id → set[connection_id]]`，`threading.Lock` 保护。同一设备允许多连接。**无跨进程共享/持久化**——单进程架构的根源之一。

### 8.3 消息协议（服务端↔Worker）

- 心跳：`heartbeat` / `heartbeat_ack`
- 生命周期：`device.connected`、`device.capabilities`（能力上报）
- 任务族（`task.*`）：`task.dispatch` / `task.accept` / `task.running` / `task.progress` / `task.result` / `task.cancel`
- 能力任务族（`capability.*`）：`capability.execute` / `capability.running` / `capability.progress` / `capability.result`（与 task 族同构，Worker 按 kind 前缀上报）

***

## 9. Capability Runtime（V1.4 新增）

### 9.1 Manifest（[manifest.py](file:///d:/Websocket/server/app/capability_runtime/manifest.py)）

- `manifest.json` 是第一道校验：name 必须匹配 `^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*){2}$`（三段式）、version 语义化 `^\d+\.\d+\.\d+$`、runtime ∈ `{yingdao, python, http, local}`；可选 entrypoint/inputs/outputs/config。
- 服务端创建版本时校验一次；Worker 解压后用**自己的副本**再校验一次（纵深防御，server/client 独立部署）。

### 9.2 包管理（[package\_service.py](file:///d:/Websocket/server/app/capability_runtime/package_service.py)）

- 上传 ZIP：解压扫描→manifest 必须存在且可解析→name/version 与上传目标一致→记录 SHA256（§37，Worker 侧校验用）→存储 `storage/capability_packages/<package_id>.zip`。
- Worker 安装状态记录在 worker\_capabilities 表。

### 9.3 执行路径

Agent `run_capability` / Workflow capability 步骤 → TaskService（execution\_type=CAPABILITY）→ 分发 → Worker `on_capability_execute` → 能力执行器 → 结果/产物回传。

***

## 10. Artifact（产物）

[artifact/service.py](file:///d:/Websocket/server/app/artifact/service.py)：

- 存储：`storage/artifacts/YYYY/MM/art_<hex>.<ext>`（本地磁盘）。
- 文件名清洗：剥目录、替换不安全字符、限 200 字符。
- 幂等去重：同一 (task\_id, step\_run\_id, checksum) 已存在则返回既有记录，不重复落盘。
- 元数据：type/mime\_type/size/checksum/source\_worker\_id/task\_id/workflow\_run\_id/step\_run\_id。

***

## 11. 数据库模型（23 张表）

users, devices, device\_registration\_codes, device\_tokens, websocket\_connections, commands, device\_capabilities, tasks, task\_steps, task\_attempts, task\_events, agent\_runs, agent\_tool\_calls, workflows, workflow\_steps, workflow\_runs, workflow\_step\_runs, workflow\_events, capabilities, capability\_versions, capability\_packages, worker\_capabilities, artifacts

默认文件版 SQLite（锁等待 15s）；纯内存 SQLite 仅测试用且强制 StaticPool 单线程语义；非 SQLite 走 pool\_pre\_ping + 1h recycle（PostgreSQL 切换路径存在但未经生产验证）。

***

## 12. API 一览（按域）

| 域              | 内容                                             |
| -------------- | ---------------------------------------------- |
| Agent API      | 运行创建/回复（resume WAITING\_USER）/状态与轨迹查询          |
| Task API       | 任务创建/查询/重试/取消、步骤、事件、attempt                    |
| Workflow API   | 10 个端点：workflow CRUD、run 启动/查询/取消、step run 查询等 |
| Device API     | 注册码签发、设备列表/状态/能力查询                             |
| Capability API | 能力/版本生命周期、包上传（peek manifest 预检）、包下载            |
| Artifact API   | 上传/下载/删除/列表                                    |
| Admin API      | `X-Admin-Token`（AGENTHUB\_ADMIN\_TOKEN）保护的管理面  |
| Health API     | 存活/就绪探测                                        |

***

## 13. DingTalk 集成

- Stream 模式长连接（无需公网回调），凭证：DINGTALK\_CLIENT\_ID/SECRET/ROBOT\_CODE。
- 消息→AgentService 新建 run（channel=dingtalk）；run 结束/ask\_user→回复消息；WAITING\_USER 期间用户再发消息→resume run（user\_reply 拼回 user\_request）。
- 群聊 @ 消息：解析时不校验 atUsers 与 robotCode（已知放宽项）。
- 节流 30s 防刷屏。

***

## 14. 业务能力矩阵

### 14.1 Agent 可执行能力（17 工具，见 §4.5）

覆盖：设备可观测（3）→ 任务可观测/干预（5）→ 命令执行（1）→ 工作流编排（5）→ 能力运行（3）。闭环形态：LLM 决策 → 工具 → 等待任务终态 → observation 回灌 → 继续决策或答复。

### 14.2 真实业务场景（当前配置下可跑通的唯一业务）

"钉钉里对机器人说：跑一下审计" → Agent 理解 → execute\_command（或 run\_capability）映射 `yingdao.audit` → 白名单设备 `办公室电脑02` → Worker 影刀执行器跑 RPA 流程（以日志结束标记判完成）→ 结果回传 → LLM 组织事实性答复 → 钉钉回复。产物文件走 Artifact 链路可下载。

### 14.3 平台化能力（代码完成，业务内容待填充）

- Workflow：串行多步编排 + 参数引用 + 失败终止 + 崩溃恢复，但当前无已注册的业务工作流数据。
- Capability Runtime：manifest→包→版本→分发→Worker 安装→执行全链路代码就绪，RUNTIMES 声明 4 种（yingdao/python/http/local），当前业务仅 yingdao 有真实执行器。

### 14.4 完成度判定

| 能力                                     | 状态                               |
| -------------------------------------- | -------------------------------- |
| DingTalk 对话 Agent（理解/计划/工具循环/确认/恢复/答复） | **已完成**                          |
| HTTP API Agent 通道                      | **已完成**                          |
| 任务引擎（状态机/分发/重试/取消/超时/事件/幂等）            | **已完成**                          |
| 工作流引擎（串行/参数/恢复/Monitor）                | **已完成**（无分支并行，属设计内简化）            |
| Worker（账本/执行器/重连/心跳/能力上报）              | **已完成**                          |
| 能力运行时（manifest/包/版本/安装）                | **已完成**（http/local 运行时为预留）       |
| 产物管理                                   | **已完成**（本地盘，无对象存储/清理策略）          |
| 多副本水平扩展                                | **未做**（Hub 内存态 + 文件 SQLite）      |
| 多租户/权限体系                               | **未做**（仅 Admin Token + 设备 token） |

***

## 15. 测试真实结果（V1.4）

**第一轮：逐文件运行**（36 个测试文件，每文件独立 pytest + 超时保护）：

- `tests/agenthub/test_capability_acceptance.py` **挂起（HANG）**，其余 35 个文件全部在超时内跑完（无第二例挂起）。
- 该文件为 V1.4 新增能力验收场景；历史同类根因是 WS 会话未 `__exit__()` 导致 `receive_json` 永久阻塞 + teardown join 泄漏（见 §16-2）。

**第二轮：完整套件**（排除挂起文件，单进程串行，2026-09-09 实测）：

```
285 passed in 392.84s (0:06:32)
```

291 收集 − 挂起文件内 6 例 = 285 全部通过，零失败。

其他确认：

- 全量收集 **291** 个用例（V1.3 基线 232 passed；V1.4 新增 capability/workflow/worker capability runtime 等测试文件）。
- 工具注册数断言已随 V1.4 更新为 17（`test_default_registry_has_standard_tools`：V1.2 九工具 + 五工作流 + 三能力）。
- pytest 缓存中残留的 3 条 lastfailed（含旧测试名 `test_default_registry_has_nine_standard_tools`）为改名前的陈旧记录，非当前失败。

***

## 16. 架构问题与技术债（按影响排序）

1. **单进程假设贯穿全局**：Hub 连接表纯内存（hub.py `_connections` dict）、文件 SQLite、本地磁盘存储，三者共同决定系统只能单实例部署。WebSocket 断连后状态在 DB 有据可查（TaskMonitor/WorkflowMonitor 兜底），但无法水平扩展。
2. **测试套件存在挂起用例**：`test_capability_acceptance.py` 当前 HANG。历史上同类问题的根因多为 WS 会话未正确关闭（`session.__exit__()` 才能解除 receive\_json 阻塞）+ 文件 SQLite 并发竞争；该文件在 V1.4 新增能力验收场景，疑似新泄漏路径。
3. **影刀完成判据脆弱**：以日志文件结束标记为唯一完成判据，影刀版本升级/日志格式变化会直接破坏执行器（无第二信号源交叉验证）。
4. **钉钉权限边界宽松**：群聊消息不校验 atUsers/robotCode，任何能 @ 机器人的群成员都能触发 ACTION 级操作（当前 AGENT\_CONFIRM\_ACTIONS 默认 false，等于无确认门槛）。
5. **超时常量并存且单位语义不一**：AGENT\_MAX\_RUNTIME=120s（run 守卫）、AGENT\_RUN\_MAX\_WAIT=1900s（API 等待）、AGENT\_TOOL\_WAIT\_MAX=1900s（工具内轮询）、SQLite timeout 15s、WS open\_timeout 30s——理解成本高，调整需通盘。
6. **能力运行时的 http/local 两个 runtime 只有 manifest 声明，无对应执行器实现**（预留）。
7. **存储无生命周期管理**：capability\_packages 与 artifacts 只进不出，无 GC/配额；Artifact 去重仅按 (task, step, checksum)，跨任务重复文件不共享。
8. **Workflow 表达能力刻意最小化**：纯串行、失败即终止、无步骤级重试/分支/并行——作为 V1.4 设计取舍成立，但限制了对真实复杂流程的表达。
9. **测试基建脆弱性累积**：文件 SQLite 并发、WS teardown 死锁、attempt 幂等依赖 tmp\_path 隔离等约束只存在于团队记忆（lessons learned），未固化成测试工具层。
10. **DingTalk 节流 30s 全局固定**：多人并发对话时回复可能被吞/延迟，无按会话队列。

***

## 17. 结论

截至当前源码，AgentHub V1.4 是一个**功能闭环完整、面向单实例部署的对话驱动设备编排系统**：从钉钉自然语言进，到设备端影刀/能力执行，再到结果与产物回传、Agent 事实性答复，全链路代码均已落地并有测试覆盖；工作流与能力运行时把"命令执行"升级为"可编排、可分发的平台能力"。真正的边界在于：单进程架构（内存 Hub + SQLite + 本地盘）、唯一真实业务（yingdao.audit 审计）、以及测试套件中仍存在一处挂起用例待修复。
