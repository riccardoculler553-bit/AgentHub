# AgentHub 从 0 到 1 系统学习手册

> 编写日期：2026-09-19 · 对应代码：commit `1c5c0c6`（V1.5 Remote-Exec-1.0）
> 用法：**按阶段顺序读，每个阶段末尾的自测题答得出来再进入下一阶段**。全程约 6–8 小时。
> 本手册只做"导读与串联"，事实源永远是代码本身；文档与代码冲突时以代码为准（已知冲突见 §8）。
> **下一步产品方向（V1.6）**：[v1.6-product-roadmap.md](v1.6-product-roadmap.md)（多设备执行与运行管理：一次请求对应一次 Task / 一台 Worker；不是 SaaS 执行平面，长驻 Python 仅可选 P1）。

---

## 第 0 阶段 · 先用四个概念建立心智模型（30 分钟）

### 0.1 这个系统到底在解决什么问题

一句话：**把"群里的一句自然语言"变成"某台子电脑上真正跑起来的一段程序"，并且保证结果可追溯、失败可收敛、重复不重跑。**

它不是一个 RPA 工具，而是**任务控制与编排层**。真正干活的（影刀机器人、Python 脚本）在子电脑上，服务端负责"决定跑什么、在哪跑、跑得怎么样了"。

### 0.2 四元模型（V1.5 铁律，必须背下来）

| 概念 | 是什么 | 不是什么 |
| --- | --- | --- |
| **Capability** | 程序（一个 ZIP 包，含 manifest） | 不是"设备能执行什么命令"（那是旧的 device_capabilities） |
| **Artifact** | 数据（输入与产物） | 不是任务结果本身（结果是 JSON，产物的引用在结果里） |
| **Task** | 一次执行 | 不是流程（流程是 Workflow） |
| **Worker** | 执行地点 | 不是"连接"（一台设备可有多条连接） |

判断标准：**WebSocket 只传控制与引用，字节永远走 HTTP。** 记住这条，你就能预判几乎每一处设计。

### 0.3 三层决策的分工（最重要的一条设计哲学）

| 引擎 | 决策方式 | 为什么 |
| --- | --- | --- |
| Agent 引擎 | **LLM 动态决策**（选流程、填变量） | 不确定性只允许存在于最上层 |
| Workflow 引擎 | 纯确定性状态机，DB 为唯一事实 | 编排不能靠"模型心情" |
| 任务引擎 | CAS + 事件门序 | 可靠执行是数学问题，不是智能问题 |

**编排链路零 LLM 参与**——这句话是理解 V1.3 之后所有代码的钥匙。

### 0.4 四对"看起来一样但不是一回事"的概念

这是新人读代码时 90% 的困惑来源：

| 概念对 | 区别 | 代码体现 |
| --- | --- | --- |
| **Device ≠ Connection** | 长期身份 vs 一次 WebSocket 会话；一台设备可多连接 | `hub.py` 的 `_device_connections: dict[device_id → set[connection_id]]` |
| **Run ≠ Task** | 一轮消息一个 Run；一次业务执行一个 Task；Run 可扇出多 Task，Task 活过服务重启 | `agent_runs` vs `tasks` 两张表 |
| **Command ≠ Executor** | 服务端注册表说"允许做什么"，Worker 本地注册表说"本机能做什么"；**两侧一致才可执行** | `commands` 表 + `client/worker/capabilities.json` |
| **传输 ACK ≠ 任务 ACK** | `message_ack` 是信封层收到；`task.accept` 是业务层收下任务 | `websocket.py` 的 `_dispatch` vs `task/service.py` 的 `handle_device_event` |

⚠️ 还有一个**历史陷阱**：`GET /api/capabilities` 在 V1.4 之后语义**变了**——它现在返回"自动化能力包"，旧含义（设备命令能力）搬到了 `/api/device-capabilities`。读 API 代码时注意区分。

### 0.5 阶段自测

1. 为什么"任务只会被派发一次"不需要依赖单进程假设？
2. 一台子电脑断了一条 WebSocket，它算离线吗？依据是什么字段？
3. `capability.execute` 消息里为什么不带文件内容？

---

## 第 1 阶段 · 先让它跑起来（1 小时，动手）

**不要先读代码，先跑通。** 跑通之后再读代码，抽象概念会自动落地。

主参考：`docs/运行手册-V1.5.md`（这份文档质量最高，直接照做）

### 1.1 三进程最小闭环

```
① Server（主电脑）    cd server && python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
② 注册码              POST /api/device-registration（带 X-Admin-Token）→ DL-XXXX-XXXX
③ Worker（子电脑）    cd client && python main.py --server http://<server>:8000 --code DL-XXXX-XXXX --name "办公电脑02"
```

**成功标志（三个都要看到）**：
- Server 控制台：`agent mode: tool_agent` + `DeviceLink started on 127.0.0.1:8000` + `DingTalk connected via wss://...`
- Worker 控制台：`[ws] websocket established`
- `GET /api/devices` 中该设备 `status == online`

### 1.2 必须理解的第一个坑

Worker 的 `client/worker/capabilities.json` 里 `name` 必须**逐字对齐**服务端 `commands` 表的 `command_name`，否则：注册成功、连接成功、但任务创建时四重校验直接 422。这不是 bug，是"双侧注册表"设计。

另一个隐蔽坑：JSON 里的 Windows 路径**必须用双反斜杠或正斜杠**，单反斜杠会让 JSON 解析失败 → 能力上报为空 → **静默故障**（不报错，只是永远派不了单）。

### 1.3 观察点（学会看什么就知道系统在干什么）

| 想知道 | 看哪里 |
| --- | --- |
| 任务走到哪一步 | `GET /api/tasks/{id}` → `events`（每个状态变更都是一行） |
| Worker 这次执行的现场 | `~/.devicelink/work/executions/<attempt_id>/`（input / mapping / output / params.json / stdout.log） |
| 谁在等着它 | `attempts[]` 的 `status` + `progress` 快照 |
| Server 干了什么 | `server/logs/server.log`（钉钉帧、设备连接、派发全过程） |
| Worker 干了什么 | 控制台 + `client/logs/worker.log` |

### 1.4 阶段自测

1. 注册码为什么是一次性的、5 分钟过期、只存哈希？
2. `~/.devicelink/` 目录下都有什么？各自作用是什么？
3. 为什么 Worker 不需要公网 IP，也不需要装 Tailscale？

---

## 第 2 阶段 · 控制平面：任务引擎（2 小时，最核心）

**这是整个系统的心脏，值得花最多时间。**

阅读顺序（每个文件读完再读下一个）：

| 顺序 | 文件 | 读什么 |
| --- | --- | --- |
| 1 | `server/app/task/state.py` | 两张转移表 + `can_transition`。**终态集合从转移表推导**，所以天然一致 |
| 2 | `server/app/task/service.py` | `create`（四重校验）→ `handle_device_event`（五道闸门）→ `request_retry` / `timeout_running`（双 CAS） |
| 3 | `server/app/task/dispatcher.py` | CAS 抢单 + attempt 创建 + 信封组装 |
| 4 | `server/app/task/monitor.py` | 三个后台循环：离线重派 / 超时看门狗 / STALE 对账 |
| 5 | `server/app/task/events.py` + `waiters.py` | 终态广播与进程内等待器（加速器，DB 才是权威） |

### 2.1 必须理解的四个机制

**① 状态机的"终态封闭"**
`SUCCESS / FAILED / CANCELLED / TIMEOUT` 的出边是空集合。这不是"我们约定不去改它"，而是**结构上不存在改的路径**。所以"任务复活"这类 bug 不可能发生。

**② 事件门序（五道闸）**
任何设备回报都要按顺序过闸，**任何一闸拒绝都只记审计事件、绝不改状态**：

```
① 归属：attempt.device_id ≠ 上报设备 → 直接忽略（安全边界，连事件都不记）
② 过期：事件带的 attempt_id ≠ step.current_attempt_id → task.late_event
③ 终态：任务已终态 → task.late_result（任务不复活）
④ 状态机：can_transition 拒绝 → task.late_result{illegal_transition}
⑤ 落账：记 task_events → commit → 唤醒 waiter
```

**③ CAS 抢单（为什么不需要分布式锁）**
```sql
UPDATE tasks SET status='DISPATCHING' WHERE task_id=? AND status='PENDING'
```
条件更新是数据库层原子操作。三个调度器（Monitor / Agent / API）并发派发同一任务，恰好一个 `claimed == 1`，其余看到 0 行就安静退出。**同一个思想在全系统复用了 4 次**：抢单、超时看门狗、Workflow run 启动、Workflow step 启动。

**④ 结果与看门狗的竞速（单胜者）**
`timeout_running` 做两次 CAS：先尝试，再任务。结果先到 → 任务 SUCCESS，看门狗 CAS 影响 0 行，返回 `notify_device=False`；看门狗先到 → 任务 TIMEOUT，迟到的 SUCCESS 落入第 ③ 闸。**任何时刻恰有一个终态产生。**

### 2.2 一个容易忽略的细节

`service.py` 成功路径里有一句 `self.db.flush()`，注释写着"autoflush=False 时不让刚关闭的步骤对下一次查询可见，会导致多步误报 advance"。这类注释是理解项目的宝藏——**看到 `# V1.x §N` 或"实测/生产"字样的注释，都是踩过坑的地方，优先读**。

### 2.3 阶段自测

1. 任务在 `ACCEPTED` 状态卡住，可能是哪几层的原因？服务端最长容忍多久？
2. 一个任务被 `cancel` 后，Worker 才把 `task.result{success}` 发回来，会发生什么？
3. 服务端重启时，处于 `DISPATCHING` 的任务会怎样？

---

## 第 3 阶段 · 通信平面：DeviceLink（1 小时）

| 顺序 | 文件 | 读什么 |
| --- | --- | --- |
| 1 | `server/app/websocket/protocol.py` | Envelope 结构 `{id, type, version, timestamp, data}` + 消息类型全集 |
| 2 | `server/app/websocket/hub.py` | 纯内存路由表（`threading.Lock`）+ `send_to_device` 逐连接发送、失败剪枝 |
| 3 | `server/app/api/websocket.py` | 握手 → 认证 → 审计行 → hub 注册 → 读循环 → 清理 |
| 4 | `server/app/websocket/heartbeat.py` | 45s 阈值 / 5s 扫描 |

### 3.1 关键事实

- **在线判据是"hub 实时连接数 > 0"**，不是 DB 里的 `devices.status`。后者是心跳宽限期（45s）的**滞后视图**。`GET /api/devices` 的 online 字段来自 hub 聚合。
- **网络模型是"服务端公网可达，客户端不需要"**：Client 主动出站 WSS，Server→Client 复用这条已建立的连接。所以子电脑不需要公网 IP / 端口映射 / Tailscale。
- **三组地址不能混**：`192.168.x.x` 仅局域网测试；`100.x.x.x` 是 Tailscale Tailnet 内部；`https://xxx.ts.net` 才是给子电脑用的公网入口（`SERVER_PUBLIC_URL`）。
- **认证失败的分级**：4401 = 令牌无效（客户端可重连）；4403 = 设备/令牌被撤销（客户端**停止**自动重连）。这个区分很重要，否则撤销的设备会无限重连。

### 3.2 阶段自测

1. 为什么 `DeviceLinkService` 被设计成"AgentHub → DeviceLink 的唯一缝隙"？换传输层要改几个文件？
2. 业务心跳 15s、离线阈值 45s，这两个数字为什么不同？
3. 消息类型里 `capability.*` 和 `task.*` 是什么关系？

---

## 第 4 阶段 · 执行平面：Worker（1.5 小时）

| 顺序 | 文件 | 读什么 |
| --- | --- | --- |
| 1 | `client/main.py` | 注册 → 连接 → 上报能力 → 重连循环 |
| 2 | `client/worker/ledger.py` | 本地 SQLite 账本：`attempt_id` UNIQUE、状态落盘、断线补报 |
| 3 | `client/worker/manager.py` | `_intake`（去重/认领）→ `_consumer_loop`（单消费者）→ `_execute` |
| 4 | `client/worker/executors/python_executor.py` | 约定映射 + 正则白名单，**永不接受 Agent 传路径** |
| 5 | `client/worker/executors/yingdao.py` + `busy_check.py` | 影刀管线：忙检查 → settle → 关弹窗 → 启动确认 → **日志 end 标记判完成** |

### 4.1 三个必须理解的设计

**① 并发 = 1（单消费者队列）**
`asyncio.Queue` + 一个消费协程。为什么不用并发？因为 RPA 类任务抢鼠标键盘/抢同一条业务流水，并发只会互相破坏。代价是队列会堆积——V1.5 因此补了"队列中可取消"。

**② accept 的语义是"已接收"，不是"正在执行"**
Worker 收到派发后：写账本 claim → **上报 accept** → 入队。此时任务还没开始跑。这不是 bug，而是有意的诚实语义（`_echo_type` 会按真实阶段回显 accept/running）。但副作用是服务端可能看到"长期 ACCEPTED"——**这正是本次审计发现的遗留缺口**（服务端缺少 ACCEPTED 活性探测）。

**③ ExecutionLedger 为什么必需**
断线时结果发不出去怎么办？Worker 把结果落本地账本，重连后自动补报。所以**执行完成但回报丢失不会导致 RPA 重跑**。这是"至少一次投递 + 幂等消费"的经典组合：投递可以重复，消费必须幂等，账本提供幂等。

### 4.2 影刀执行器的教训（很值得读）

完成判据**不是进程退出码**，而是**影刀日志里晚于本次启动时间戳的 end 标记**。原因写得很清楚：`ShadowBot.exe` 在 launcher 模式下可能秒退，也可能常驻不退，**进程存活状态根本不可信**。这是从实战项目移植过来的经验。

### 4.3 阶段自测

1. Worker 重启后，上次没执行完的 attempt 会怎样？
2. 同一个 `task.dispatch` 信封被重复投递两次，子电脑会跑两遍 RPA 吗？为什么？
3. `python_executor` 是怎么防住"让 Agent 执行任意路径脚本"的？

---

## 第 5 阶段 · 推理平面：Agent 三形态（1.5 小时）

先搞清楚**三个 Agent 的存在关系**，否则会读得莫名其妙：

| 形态 | 目录 | 状态 | 什么时候被装配 |
| --- | --- | --- | --- |
| `mvp` | `agent/mvp/` | 固定管线（analyze→resolve→create_task→dispatch→wait_result→build_reply） | `AGENT_MODE=mvp` |
| `tool_agent` | `agent/core/` + `graph/` + `llm/` + `tools/` | **现役**（你的 `.env` 就是这个） | `AGENT_MODE=tool_agent` |
| `legacy` | `agent/legacy/` | V1.0 归档，仅测试覆盖 | 不再装配 |

装配点是唯一的：`server/app/main.py` 的 lifespan 里二选一。

### 5.1 Tool-Using Agent 的主循环

```
understand → load_context → plan → llm_decide → execute_tool → observe → evaluate ─┐
                                     ↑                                              │
                                     └──────────────── 循环 ─────────────────────────┘
                                     或 → ask_user（停靠） / finish → build_reply
```

- LLM 只能输出三种决策：`tool_call` / `ask_user` / `finish`。**`ask_user` 是一等公民结果，不是错误**（error=None）。
- **LLM 永不轮询**：`execute_command` / `run_capability` 工具内部自己等终态（`_wait_terminal`），把结果作为 observation 喂回。
- **观察即上下文**：observations 跨停靠持久化（存 `state_json`），resume 后 LLM 仍看得见之前的工具事实。

### 5.2 七闸门（`agent/core/policies.py`）——安全的核心

```
存在 → enabled → args 严格校验（extra=forbid）→ 权限 → 确认 → 每工具限额 → 全局限额 → 执行
```

LLM **无法绕过**它：执行工具的代码路径只有一条，且必然经过 `ToolPolicy.execute()`。

最精妙的一处是**确认防注入**：用户回"是/确认"后，路由**短路跳过 LLM**，直接以 `confirmed=True` 执行停靠时的参数。**被提示注入的模型也没有机会改参数。**

### 5.3 工具清单（14 个，按风险级分组）

| 级别 | 工具 | 确认 |
| --- | --- | --- |
| READ | `list_devices` `get_device_status` `get_device_capabilities` `get_recent_tasks` `get_task_detail` `get_task_events` | 否 |
| READ | `list_workflows` `get_workflow` `get_workflow_run` `list_capabilities` `get_capability` | 否 |
| WRITE | `retry_task` `cancel_task` `cancel_workflow_run` | **恒需确认** |
| ACTION | `execute_command` `run_workflow` `run_capability` | 随 `AGENT_CONFIRM_ACTIONS` |

⚠️ **注意风险倒挂**：ACTION 级的 `execute_command` 反而默认**不需要**确认（`AGENT_CONFIRM_ACTIONS` 默认 false），而 WRITE 级的取消恒需确认。你的 `.env` 未设置该变量 → 群里任何人 @ 机器人都能直接触发设备执行。**这是本次审计标记的 High 问题，学习时请务必意识到这是配置而非设计缺陷。**

### 5.4 阶段自测

1. 为什么"确认后跳过 LLM"能防提示注入？
2. 一次 run 的预算耗尽时，用户会收到什么？为什么不算系统故障？
3. `run_capability` 默认异步返回 `CONFIGURED`，那用户是怎么知道最终结果的？

---

## 第 6 阶段 · 数据面与能力运行时（1.5 小时）

这是 V1.4/V1.5 的主体，也是"平台化"的部分。

| 顺序 | 文件 | 读什么 |
| --- | --- | --- |
| 1 | `server/app/capability_runtime/manifest.py` | manifest 契约：三段式 name、semver、runtime 枚举、inputs/outputs |
| 2 | `server/app/capability_runtime/package_service.py` | 上传 ZIP → 解压扫描 → manifest 校验 → SHA256 记录 → 落盘 |
| 3 | `server/app/artifact/service.py` | 存储布局 `artifacts/YYYY/MM/art_<hex>.<ext>` + 按 (task, step, checksum) 去重 |
| 4 | `client/worker/capability/manager.py` | `ensure()` 是唯一入口：缓存命中 → READY；否则下载→校验→安装 |
| 5 | `client/worker/capability/cache.py` | 安装树布局 + checksum 标记复用 + **双侧 zip-slip 防御** |
| 6 | `client/worker/capability/{downloader,puller,uploader}.py` | 三重围栏：httpx 分阶段超时 / stall 90s / total 900s |
| 7 | `client/worker/capability/executors.py` | 三种 runtime（python / yingdao / http）+ venv 管理 + 产物收集 |

### 6.1 三份缓存（理解"为什么改版本号就够了"）

```
<work>/capabilities/<名>/<版>/    包缓存    命中条件：.installed.json 里的 checksum 一致
<work>/envs/<名>/<版>/            venv 缓存  命中条件：.deps_ok 里的 requirements 哈希一致
<work>/artifact_cache/<checksum>/ 数据缓存  命中条件：内容寻址（按 checksum 命名）
<work>/executions/<attempt_id>/   执行现场  天然隔离（execution_id == attempt_id）
```

**正常迭代不需要手动清缓存**：改代码 → 改 manifest 版本号 → 重新 build → 重新发布，三份缓存自然全部失效。这解释了为什么 manifest 的 version 是硬性 semver 要求。

### 6.2 数据面的三重围栏（V1.5 的核心修复）

这是从一次生产事故（子电脑永久冻死）里长出来的设计：

| 围栏 | 阈值 | 防御什么 |
| --- | --- | --- |
| httpx 分阶段超时 | connect/read/write 各自 | 明显断链 |
| **stall 看门狗** | 90s 无字节 | **滴漏死链**（TCP 活着但应用死了）——分阶段超时对此完全无效 |
| **total 总预算** | 900s（跨重试连续计算） | 恒速但极慢的连接 |

而且这三者与 **cancel 事件在同一次 `asyncio.wait` 里竞速**——停等在静默 socket 上的下载也能被立即取消。

### 6.3 阶段自测

1. 为什么 Worker 端的 `_validate_zip` 要和 Server 端"重复"实现一遍？
2. 输入 Artifact 的 `role` 和 manifest 里的 `inputs` 键名是什么关系？
3. 一个大文件传输时，为什么业务心跳不会断？

---

## 第 7 阶段 · 横向能力：Workflow / 通知 / 可观测（1 小时）

### 7.1 Workflow 引擎（`server/app/workflow/`）

五张表：`workflows`（定义）→ `workflow_steps`（步骤定义）→ `workflow_runs`（实例）→ `workflow_step_runs`（步骤实例，回链 `task_id`）→ `workflow_events`（事件流）。

四个设计铁律（背下来）：
1. **单一入口**：Agent Tools 与 Admin API 都走 `WorkflowService`，都不直写编排 SQL。
2. **永不绕过任务引擎**：步骤执行一律 `TaskService.create(source_type="WORKFLOW", ...)`，重试/超时归任务引擎管。
3. **Task 是事实源**：步骤状态只从 Task 终态**单向**同步；错过通知由 `WorkflowMonitor`（5s 慢速安全网）修复。
4. **双重 CAS**：run 启动与 step 启动都是条件更新，并发触发只有一个胜者。

参数解析（`resolver.py`）的关键规则：整值引用**保留原类型**；混入文本走字符串替换；引用不存在的变量/未知步骤/**未来步骤**/**失败步骤** → 编排期直接失败该步骤，**绝不猜值、绝不空跑**。

### 7.2 通知链路（V1.5 Phase 3）

```
Task 终态 → notify_task_terminal 广播
              ├─ workflow runtime（推进下一跳）
              └─ agent 通知层：反查 AgentRun → 钉钉推送
                    ├─ run 已结束 → 幂等推送（键 = task_id + status）
                    └─ run 仍打开 → 静默（agent 自己会带出结果）
```

**为什么要判断 run 是否已结束**：若 run 还在同步等待，结果会由 agent 自己回复；再推一次就是双重打扰。幂等键防止 SUCCESS 被重复播报。

### 7.3 可观测的三条排障路径

| 想看什么 | 路径 |
| --- | --- |
| Agent 决策全过程 | `agent_runs` → `agent_tool_calls`（每次工具调用一行，含被拒绝的） |
| 业务执行全过程 | `tasks` → `task_events`（全生命周期事件流） |
| 编排全过程 | `workflow_runs` → `workflow_events` |
| 人看的界面 | Dashboard（`server/app/static/index.html`，单文件无构建） |

**双侧对账**是排查复杂问题的关键：`agent_tool_calls` 看"agent 以为发生了什么"，`task_events` 看"系统实际发生了什么"。

### 7.4 阶段自测

1. Workflow 某步失败后，后续步骤是什么状态？为什么？
2. 如果一个 run 的终态通知丢了，谁会补上？
3. 用户说"昨天那个任务为什么失败"，你应该查哪两张表？

---

## 第 8 阶段 · 文档地图与已知冲突（30 分钟）

### 8.1 该信任哪份文档

| 文档 | 可信度 | 定位 |
| --- | --- | --- |
| `ARCHITECTURE.md`（1055 行） | ⭐⭐⭐⭐ 权威但局部过期 | **主事实源**，§1–§20 覆盖全部机制与设计取舍 |
| `docs/运行手册-V1.5.md` | ⭐⭐⭐⭐⭐ 最新最准 | 动手操作就看它 |
| `docs/V1.5_EXECUTION_LIFECYCLE.md` | ⭐⭐⭐⭐⭐ | 四条生命周期 + 总架构图，快速理解全貌最佳 |
| `docs/V1.5_RUNTIME_ISSUE_ROOT_CAUSE.md` | ⭐⭐⭐⭐⭐ | 8 个生产问题的源码级根因，**学设计思路的最佳读物** |
| `docs/V1.5_RUNTIME_FIX_REPORT.md` | ⭐⭐⭐⭐⭐ | 逐 Phase 修复与回归用例对照 |
| `AgentHub_V1.4_系统全景文档.md` | ⭐⭐⭐ 版本落后一轮 | 全景速览 + 架构问题清单，写得很诚实 |
| `README.md` | ⭐⭐ **V1.0 时期** | 只看"网络模型"那张表，其余已过时 |
| `docs/V1.1_PLAN.md`（3557 行） | ⭐⭐ 历史 | 只在追溯 CAS 设计缘由时读 |

### 8.2 已知冲突（读文档时请直接跳过这些过时点）

| 位置 | 文档说 | 实际 |
| --- | --- | --- |
| README / ARCHITECTURE 页头 / §14 | 38 / 299 / 193 个测试 | **实测 328 passed**（唯一准的是 V1.5 修复报告） |
| ARCHITECTURE §3 | 迁移到 `0005` | 实际到 `0009` |
| ARCHITECTURE §5 | "13 张表" | V1.4/V1.5 后至少 +5 张 |
| ARCHITECTURE §18.9 | "生产继续跑 mvp" | `.env` 已是 `tool_agent` |
| ARCHITECTURE §3 目录树 | 缺 `capability_runtime/` `artifact/` `agent/notify.py` 等 | 这些是 V1.4/V1.5 主力模块 |
| 全景文档 §3.1 | "生产以 AGENT_MODE=mvp 运行" | 同上，已切换 |

### 8.3 测试的现状（动手前必读）

```bash
# ✅ 正确姿势（必须排除挂起文件）
"C:\Program Files\Python311\python.exe" -m pytest tests -q \
  --ignore=tests/agenthub/test_capability_acceptance.py
# → 实测 328 passed，约 7 分 37 秒

# ❌ 直接跑全量会怎样
python -m pytest tests
# → 挂死在 test_capability_acceptance.py，--timeout 也杀不掉（thread 方法无法中断阻塞线程）
```

托管 Python 3.13 没有依赖，**用系统 Python 3.11**。

---

## 附录 A · 建议的读代码顺序（一张清单）

```
第 1 天：跑通系统（第 1 阶段）+ 读 task/ 四个文件（第 2 阶段）
第 2 天：websocket/ 三个文件 + client/worker/ 三个文件（第 3、4 阶段）
第 3 天：agent/core/policies.py → graph/nodes.py → tools/（第 5 阶段）
第 4 天：capability_runtime/ + client/worker/capability/（第 6 阶段）
第 5 天：workflow/engine.py（606 行，最长的模块）+ notify.py（第 7 阶段）
```

**读代码的技巧**：优先读带 `§N` / `实测` / `生产` / `Phase N` 字样的注释——那都是踩过坑的地方，信息密度最高。

---

## 附录 B · 术语表

| 术语 | 含义 | 代码位置 |
| --- | --- | --- |
| Envelope | 统一消息信封 `{id,type,version,timestamp,data}` | `websocket/protocol.py` |
| attempt | 一次派发尝试（一个 step 可多次 attempt） | `task_attempts` 表 |
| CAS | 条件更新抢占（Compare-And-Set） | 抢单/看门狗/run/step 四处 |
| gate 闸门 | 状态变更前的准入检查 | 七闸门（工具）/ 五闸（事件） |
| Lazy Pull | Worker 收到派发才去拉能力包 | `capability/manager.py` |
| lease/账本 | 本地 SQLite 幂等记录 | `worker/ledger.py` |
| manifest | 能力包清单，能力契约的唯一声明 | `capability_runtime/manifest.py` |
| role | Artifact 在本次任务中扮演的输入名 | `tasks.artifact_ids` |
| workspace | 本次执行的沙箱目录 | `<work>/executions/<attempt_id>/` |
| singleton | 工作流单例护栏（同流程同时只跑一个 run） | `workflows.active_singleton` |
| STALE | attempt 的"对账关闭"态（任务已终态它还活着） | `task/state.py` |

---

## 附录 C · 排障速查（现象 → 根因 → 看哪里）

| 现象 | 最可能原因 | 先看 |
| --- | --- | --- |
| 任务创建 422 | 设备未上报该命令能力 / 参数不合 schema | `task_validation_failed` 的 problems 数组 |
| 设备显示离线但进程在跑 | 只断了一条连接 / 心跳未发 | `GET /api/devices` 的 connection_count |
| 任务长期 ACCEPTED | Worker 队列堆积或卡在半死下载 | `attempts[].progress` + `client/logs/worker.log` |
| `ARTIFACT_CHECKSUM_MISMATCH` | 数据传输损坏 | 重新上传 artifact 再派发（**不会自动重试**，这是设计） |
| `DOWNLOAD_FAILED` / `INSTALL_FAILED` | 拉包失败 / 包损坏 | worker.log；重新 build+上传 |
| `PYTHON_ENV_CREATE_FAILED` / `DEPENDENCY_INSTALL_FAILED` | 子电脑 pip 源不通 | stderr 尾部 |
| `CAPABILITY_EXECUTION_FAILED` | 业务脚本失败 | `executions/<attempt_id>/stdout.log` + `stderr.log` |
| `DEVICE_BUSY` | 单并发限制（一台设备同时只能跑一个任务） | 换设备或等前一个完成 |
| 群里没收到完成通知 | run 仍在运行（通知层静默）/ sessionWebhook 过期 | `agent_runs` 状态 + `GET /api/agent/runs` |
| 重跑很慢 | 三份缓存未命中 | 检查是否改了版本号 |

---

## 附录 D · 30 道自测题（用来验收"摸清楚了"）

**概念层**
1. Capability / Artifact / Task / Worker 各是什么？各自的"不是什么"是什么？
2. Device 与 Connection 为什么要分开？
3. Run 与 Task 的边界在哪？取消 Run 会取消业务任务吗？
4. Command Registry 与 Executor Registry 的关系是什么？
5. `/api/capabilities` 在 V1.4 前后语义有什么变化？

**控制平面**
6. CAS 抢单为什么不需要分布式锁？
7. 事件门序的五道闸分别防什么？
8. 结果与看门狗竞速，为什么恰有一个胜者？
9. 终态为什么不能复活？靠什么机制保证？
10. `reconcile_terminal_attempts` 解决什么问题？三种情况分别是什么？
11. `pending_since` 为什么从 `created_at` 改过来？
12. 重试次数用尽后返回什么 HTTP 状态码？

**通信平面**
13. 在线状态的权威判据是什么字段？为什么不用 DB status？
14. 4401 与 4403 的处理差异是什么？为什么？
15. 心跳 15s 与离线阈值 45s 为什么不同？

**执行平面**
16. accept 的语义是什么？为什么可能"卡 ACCEPTED"？
17. ExecutionLedger 提供了什么保证？断线时结果怎么办？
18. 为什么 Worker 并发设为 1？
19. 影刀执行器为什么不看进程退出码？
20. 重复投递同一个 task.dispatch 会跑两遍吗？

**推理平面**
21. LLM 只能输出哪三种决策？
22. 七闸门分别是什么？
23. "确认后跳过 LLM"为什么能防提示注入？
24. LLM 为什么永不轮询？
25. 一次 run 的工具有哪两重预算？

**数据面与编排**
26. 三份缓存各自的命中条件是什么？
27. 数据面的三重围栏分别防什么？
28. Workflow 的参数引用规则中，"未来步骤"引用为什么直接失败？
29. 终态通知的幂等键是什么？为什么 run 还在跑时要静默？
30. V1.5 八个生产问题里，哪一个在代码中**没有**完全落地？（提示：与 ACCEPTED 有关）

---

## 附录 E · 五个不要踩的坑

1. **别在托管 Python 3.13 下跑测试** —— 没依赖。用系统 Python 3.11。
2. **别直接 `pytest tests`** —— 会挂死。必须 `--ignore=tests/agenthub/test_capability_acceptance.py`。
3. **验证完记得关掉本地 Server** —— server 启动即接管钉钉机器人，会和生产的抢消息。
4. **`capabilities.json` 的路径必须用双反斜杠或正斜杠** —— 单反斜杠会静默失败（不报错，只是永远派不了单）。
5. **改了 `.env` 必须重启 server** —— 配置只在启动时读一次。

---

*本手册为学习导读产物，未修改项目任何源码、配置、迁移或数据。事实源以代码为准。*
