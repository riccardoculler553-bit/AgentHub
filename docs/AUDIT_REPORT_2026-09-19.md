# AgentHub / DeviceLink 全量代码审计报告

> 审计日期：2026-09-19
> 审计对象：`D:\Websocket`，git commit `1c5c0c6`（Lifecycle-1.0，工作树干净，0 项未提交变更）
> 审计方式：**纯只读审计，未修改任何源码 / 配置 / 迁移文件**
> 审计人：WorkBuddy（AI 代码审计）

---

## 0. 审计范围与方法

| 项 | 内容 |
| --- | --- |
| 代码规模 | 220 个 `.py`；服务端 `server/app` 约 1.0 万行（100 个模块），客户端 `client/` 31 个模块，测试 46 个文件 |
| 覆盖模块 | 推理层（agent）、控制层（task/workflow/command/capability）、通信层（websocket）、执行层（client worker）、能力运行时（capability_runtime/artifact）、钉钉接入、迁移、脚本 |
| 方法 | ① 文档 ↔ 代码逐条交叉核对；② 鉴权面全端点枚举；③ 状态机 / CAS / 并发路径逐行走查；④ **实测验证**（跑全量测试、实测 SQLAlchemy 查询语义） |
| 未做 | 未修改任何文件；未启动生产服务；未执行 1GB 压力测试；未连接生产 MySQL |

### 实测验证记录

| 验证项 | 结果 |
| --- | --- |
| 全量测试套件 | **328 passed / 0 failed**，耗时 7 分 37 秒（命令：`pytest tests --ignore=tests/agenthub/test_capability_acceptance.py`） |
| 不排除挂起文件时 | **整个套件挂死**（实测 >12 分钟无任何输出，`--timeout=120 --timeout-method=thread` 也无法终止）——项目文档中"已知挂起"的技术债被实测确认 |
| `select().limit().where()` 语义 | 实测 **安全**：SQLAlchemy 2.0.51 正确生成 `WHERE ... ORDER BY ... LIMIT`，`list_tasks` / `list_artifacts` 的过滤不会被 limit 吞掉（曾疑似缺陷，经验证排除） |
| 管理令牌配置 | `.env` 中 `AGENTHUB_ADMIN_TOKEN` **已设置**（非开放模式），`AGENT_MODE=tool_agent` |

---

## 1. 结论速览

**整体评价：工程水位显著高于同规模项目。** 可靠性不靠"约定"而靠状态机与 CAS 强制、双侧幂等、事件门序、终态封闭——这是一套真正按生产标准写出来的任务引擎。安全性上，凭证只存哈希、路径白名单、参数 schema `extra=forbid`、Agent 永不能传递可执行路径，边界设计清晰。

**本次审计未发现"立刻会炸"的 P0 缺陷**，但发现 **5 项 High / 7 项 Medium / 6 项 Low**。其中真正值得优先处理的系统性风险有三个：

1. **管理面 fail-open**：`AGENTHUB_ADMIN_TOKEN` 为空即**完全关闭鉴权**，叠加 `register-local` 端点可读服务端任意文件 → 一次配置失误等于主机文件泄露。
2. **服务端大文件全量进内存**：客户端数据面已经做到流式落盘 + 增量哈希，**服务端反而没有**；结合生产库中已存在的 400–658MB xlsx（20 个），单次上传/注册即可造成 GB 级内存峰值。
3. **文档落后代码 1–2 代**：4 份文档给出 4 个不同的测试数（38 / 193 / 299 / 328），迁移号、`AGENT_MODE` 描述、表数量、目录结构多处过期。对新人（和未来的你）而言，这是最大的隐性成本。

---

## 2. 项目导读：它到底是什么

### 2.1 一句话定位

AgentHub 是架在 DeviceLink（多设备 WebSocket 长连接管理）之上的**任务控制与智能编排层**：把自然语言请求 → 意图/流程 → 经"命令注册表 × 能力注册表"双重校验 → 派发到指定子电脑执行 → 回报结果，支持多步串行、重试、超时看门狗、取消与离线重派。

### 2.2 四层平面架构

`Reasoning（LangGraph 主 Agent）→ Control（任务引擎/注册表）→ Communication（DeviceLink）→ Execution（Worker 执行器）`

关键边界（这几条是理解全部代码的钥匙）：
- **Agent 永不触碰 WebSocket / 数据库细节** —— 派发走 `TaskDispatcher`，等待是纯 DB 轮询。
- **业务代码永不直接碰 WebSocket** —— 唯一缝隙是 `DeviceLinkService`（换传输层只改这一个类）。
- **Command（系统允许做什么）与 Executor（本机实际能做什么）分离**，双侧一致才可执行。

### 2.3 三引擎分工（V1.3 确立的心智模型）

| 引擎 | 职责 | 决策方式 |
| --- | --- | --- |
| Agent 引擎 | 理解意图 → 选流程 / 填参数 | **LLM 动态决策**（仅此一处有 LLM） |
| Workflow 引擎 | 步骤推进 / 传参 / 失败策略 / 取消 / 重启恢复 | 纯确定性状态机，DB 为唯一事实 |
| 任务引擎 | 单命令可靠执行（派发 / 幂等 / 重试 / 看门狗） | CAS + 事件门序 |

编排链路**零 LLM 参与**——这是整个系统最正确的架构决策：把不确定性关进最上层的笼子里。

### 2.4 模块地图

| 层 | 目录 | 核心文件（行数） | 职责 |
| --- | --- | --- | --- |
| 推理 | `agent/core/` | `policies.py`(253) `runner.py`(180) `state.py` | 七闸门准入、调用账本、审计写入、状态契约 |
| 推理 | `agent/graph/` | `nodes.py`(233) `routing.py` `graph.py` | 8 节点图：understand→load_context→plan→llm_decide→execute_tool→observe→evaluate |
| 推理 | `agent/tools/` | `task.py`(295) `capability.py`(229) `workflow.py`(220) `base.py`(188) | 14 个标准工具，`args_schema` 一律 `extra=forbid` |
| 推理 | `agent/mvp/` · `agent/legacy/` | `graph.py`(287) `legacy/graph.py`(261) | 固定管线（MVP）与 V1.0 Agent 归档 |
| 控制 | `task/` | `service.py`(**682**) `dispatcher.py`(337) `monitor.py`(197) `state.py` | 任务生命周期、CAS 派发、看门狗、转移表 |
| 控制 | `workflow/` | `engine.py`(**606**) `registry.py`(187) `resolver.py`(100) | 确定性多步编排 + 上下文参数解析 |
| 控制 | `capability_runtime/` | `service.py`(195) `package_service.py`(130) `resolver.py` | 能力注册表三表 + 包上传/发布/选机 |
| 控制 | `artifact/` | `service.py`(165) | 数据平面：Artifact 上传/去重/下载 |
| 通信 | `websocket/` | `hub.py`(113) `protocol.py`(96) `heartbeat.py`(58) | 内存连接路由、Envelope 协议、45s 离线判定 |
| 执行 | `client/worker/` | `manager.py`(600+) `ledger.py` `capability/*` | 单消费者队列、执行账本、惰性拉包、venv 运行时 |
| 集成 | `integrations/dingtalk/` | `client.py`(212) `parser.py` | 裸 Stream 协议接入（不用官方 SDK，原因见 §16.2） |

### 2.5 一次任务的全链路（V1.5 实际路径）

```
用户 @钉钉机器人
  → 裸 Stream WSS 收帧 → 先 ACK → 群节流 → message_id 幂等
  → AgentService.handle_message → AgentRun(RUNNING) 落库 → 后台图执行（HTTP/WS 零阻塞）
  → LLM 决策 tool_call: run_capability（默认异步，立即返回 CONFIGURED）
  → TaskService.create 四重校验（设备 / 命令 / 参数 schema / 能力）
  → TaskDispatcher CAS 抢单 → 建 attempt → WS task.dispatch
  → Worker：账本 claim（attempt_id 幂等）→ 拉包(Lazy Pull) → 建 venv → 下载输入 Artifact
  → 执行 → 产物上传 → task.result 只带 artifact 引用
  → TaskService 事件门序 5 闸 → 终态落库 → notify_task_terminal 广播
  → agent/notify.py 反查 AgentRun → 钉钉主动推送"任务已完成 + 产物"
```

**Devcie ≠ Connection**：`device_id` 是长期身份，`connection_id` 是一次会话，一台设备可同时多条连接（在线状态按设备聚合）。

### 2.6 状态机（终态封闭）

见本轮对话中的状态机图。要点：
- `can_transition()` 是唯一的状态变更裁判，终态集合从转移表推导，天然一致。
- 设备回报按 5 道闸顺序过滤：**归属 → 过期 attempt → 终态 → 状态机 → 落账**，任何一闸拒绝都只记审计、绝不改状态。
- 看门狗与结果回报用**双 CAS 竞速**，恰有一方胜出；迟到的 SUCCESS 只会生成 `task.late_result` 审计事件。

---

## 3. 先行肯定：做得确实好的地方

1. **可靠性靠机制强制而非约定**：`Dispatcher` 的 `PENDING→DISPATCHING` 是条件 UPDATE（CAS），并发调度器只能有一个胜者；事件门序 + 终态封闭使"任务复活"在结构上不可能。
2. **双侧幂等**：服务端 attempt 账本 + Worker `ExecutionLedger`（本地 SQLite，`attempt_id` UNIQUE）——执行完成但回报未送达的结果在重连后补报，绝不重复跑 RPA 作业。
3. **安全边界干净**：`_SCRIPT_NAME = ^[A-Za-z0-9_]+\.py$` + `resolve()` + `parents` 校验（`python_executor.py:34`）；zip-slip 双侧防御；Agent 永远不能传路径或 shell；注册码/Token 只存 SHA256；`hmac.compare_digest` 做常量时间比较。
4. **数据面与控制面物理隔离**（V1.5 Phase 7）：大文件 IO 全部 `to_thread`，下载改为 1MB 流式 + 增量哈希 + stall/total 双看门狗 + cancel 竞速；**实测压测报告称 1GB 传输事件循环最大间隙 47ms**（未复现验证）。
5. **可追溯性极强**：`docs/V1.5_RUNTIME_ISSUE_ROOT_CAUSE.md`（8 个生产问题的源码级根因）+ `V1.5_RUNTIME_FIX_REPORT.md`（逐 Phase 修复与回归用例）——这种文档纪律很少见。我逐条核对了 8 个问题的修复，**7 个已在代码中确认落地**。
6. **测试基建踩坑固化**：文件型 SQLite（避免 StaticPool 跨会话破坏事务）、FakeWorker、`spawn()` 强引用管理、测试自有路由表 + autouse 还原、强制离线 LLM。这些注释里写着"踩坑后固化"的东西，正是最有价值的资产。

---

## 4. 审计发现

### 4.1 汇总

| ID | 级别 | 问题 | 位置 |
| --- | --- | --- | --- |
| H1 | High | 管理面 fail-open：token 为空即关闭全部鉴权，叠加任意文件读取 | `auth/admin.py:17-25` · `api/artifact.py:94-139` |
| H2 | High | 服务端大文件全量进内存（客户端已流式，服务端没有） | `api/artifact.py:68,138` · `api/capability.py:169` · `puller.py:89,133` |
| H3 | High | `ACCEPTED` 态无活性探测：最长挂 1800s 无反馈 | `task/monitor.py:88-103` |
| H4 | High | 单个脏 WS 事件撕裂整条设备连接（1011） | `api/websocket.py:101,105-107` |
| H5 | High | `execute_command`（ACTION 级）默认**不需要**用户确认 | `core/config.py:90` + `.env` 未设置 |
| M1 | Medium | Artifact / 能力包下载无归属校验，任一设备可下载任意资源 | `api/artifact.py:150-162` · `api/capability.py:85-96` |
| M2 | Medium | 注册码可在线爆破：40 bit + 5min TTL + 无限速无锁定 | `api/devices.py:31` · `core/security.py:11` |
| M3 | Medium | ZIP 解压无体积 / 条目数上限（zip 炸弹） | `package_service.py:103-117` · `client/.../cache.py:144-167` |
| M4 | Medium | `asyncio.to_thread` 不可取消 → 超时后线程 / socket 泄漏 | `uploader.py:92-102` · `executors.py:209` |
| M5 | Medium | 内存态集合只增不减（长稳内存缓慢增长） | `monitor.py:36` · `api/websocket.py:208` |
| M6 | Medium | 双重 schema 管理：Alembic + create_all + 硬编码 ALTER | `workflow/db_models.py:120-186` · `server/migrations/*` |
| M7 | Medium | 文档与代码不一致（见 §5） | README / ARCHITECTURE / 全景文档 |
| L1–L6 | Low | 硬编码业务设备名、无效代码、无界 sweep 等 | 见 4.3 |

---

### 4.2 High 级问题详述

#### H1 · 管理面 fail-open + 任意文件读取

**证据**

```python
# server/app/auth/admin.py:17-25
def require_admin(x_admin_token: str | None = Header(default=None)) -> None:
    expected = settings.admin_token
    if not expected:
        return                      # ← 为空即完全放行
```

```python
# server/app/api/artifact.py:94-106（admin-only，但受上面控制）
@admin_router.get("/scan")
def scan_directory(dir: str): ...   # 列出服务端任意绝对路径目录下的文件

# server/app/api/artifact.py:125-139
@admin_router.post("/register-local", ...)
def register_local_artifact(payload: RegisterLocalIn, db=Depends(get_db)):
    base = _resolve_local_dir(payload.dir)          # 只要求"绝对路径 + 是目录"
    path = base / Path(payload.name).name           # 防了目录穿越
    row = ArtifactService(db).create_artifact(name=path.name, content=path.read_bytes(), ...)
```

**影响**：`AGENTHUB_ADMIN_TOKEN` 一旦为空（默认值就是空），`scan?dir=C:\` + `register-local` + `/download` 三段式即可**读取服务端主机任意文件**。当前 `.env` 已设置 token（已实测确认），所以**当前不是漏洞**；但这是 fail-open 设计——忘了配、配错环境、容器里漏传环境变量，任何一种情况都会静默降级为"无鉴权 + 任意文件读"。

**建议**
1. 启动时断言：`APP_HOST` 非回环 或 `SERVER_PUBLIC_URL` 非 localhost 时，`admin_token` 为空 → **拒绝启动**（fail-closed）。
2. `register-local` / `scan` 增加目录白名单（如 `STORAGE_DIR` 与显式配置的数据源根），而非任意绝对路径。
3. 该组合端点建议收敛为单一"注册数据源"接口，`dir` 只接受白名单别名而非裸路径。

#### H2 · 服务端大文件全量进内存

**证据**

| 位置 | 代码 | 后果 |
| --- | --- | --- |
| `api/artifact.py:68` | `content = await file.read()` | 整个上传体进内存，再交给 `create_artifact` |
| `api/artifact.py:138` | `content=path.read_bytes()` | 本地注册读整个文件 |
| `api/capability.py:169` | `zip_bytes = await file.read()` | 整包进内存（同时用于 `peek_manifest` 与落盘） |
| `client/.../puller.py:89,133` | `chunks.append(chunk)` … `b"".join(chunks)` | 包下载全量驻留内存（**与 V1.5 修复报告 §7 声称的"流式"不一致**：downloader 确实流式落盘了，puller 只做了增量哈希仍全量缓冲） |

**影响**：`server/storage` 实测 **36GB**，其中 20 个文件 400–658MB。一次 658MB 上传：`file.read()` 的 bytes + `write_bytes` 期间的目标缓冲，单请求内存峰值可达 GB 级；并发两三个即 OOM。且这两个上传端点**无 Content-Length 限制**，构成 DoS 面。

**对照**：客户端 `ArtifactDownloader` 已经做得很好（1MB 分块 → `.part` 临时文件 → `os.replace` 原子落盘 → 增量 sha256）。服务端应当照抄这个模式。

**建议**：`UploadFile.file` 直接 `shutil.copyfileobj` 到目标 `.part` 再 rename，哈希边写边算；对 `Content-Length` 与最终落盘大小做上限；`register-local` 用 `shutil.copyfile` 而非 `read_bytes`；puller 改为写出 `.part` 文件后返回路径。

#### H3 · `ACCEPTED` 态无活性探测（计划项未落地）

**证据**

```python
# server/app/task/monitor.py:88  只有这三态受 timeout_at 约束
Task.status.in_(["SENT", "ACCEPTED", "RUNNING"]),
...
# monitor.py:98-103  只有 SENT 享有"离线有界等待"
for task in db.scalars(select(Task).where(Task.status == "SENT")):
    if task.target_device_id and not self.hub.is_device_online(task.target_device_id):
        ... if now - last_dispatch > max_wait: svc.timeout_running(task)
```

`docs/V1.5_RUNTIME_ISSUE_ROOT_CAUSE.md` 的"修改点"明确写了 *"Server：TaskMonitor 增加 ACCEPTED 活性探测（accepted 后 N 秒无 running → 重派或失败）"*，但 `V1.5_RUNTIME_FIX_REPORT.md` 的问题 2 修复清单里**没有这一项**，代码中也不存在（`grep ACCEPTED` 在 `task/` 下仅出现于状态集合与 docstring）。代码注释自己承认了缺口：*"TIMEOUT is retryable, an ACCEPTED-forever state is not"*（`monitor.py:96`）。

**影响**：Worker 半死（accept 后卡在下载/僵尸队列）时，任务会一直停在 `ACCEPTED` 直到 `timeout_at` 到期——能力任务默认 **1800 秒**。这正是 V1.5 生产故障的原始症状（地平线1号 17:45 永久 ACCEPTED），Phase 2 只堵住了"worker 侧 cancel 生效"，**服务端侧的活性探测没做**，所以同样的场景仍会挂 30 分钟。

**建议**：`attempts` 增列 `accepted_at`（已在 `task.accept` 分支写入 `attempt.accepted_at`，`service.py:359` 有值但 monitor 没用），monitor 增加：ACCEPTED 超过 N 秒（如 60s）仍无 `task.running` → 记 `task.accepted_stalled`，按策略重派或直接 FAILED。这比 1800s 的兜底有价值得多。

#### H4 · 单个脏 WS 事件撕裂整条设备连接

**证据**

```python
# server/app/api/websocket.py:101
await _dispatch(connection, envelope)      # 无 try/except
...
# :105-107
except Exception:
    logger.exception("connection %s crashed", connection.connection_id)
    close_code = 1011
```

`_dispatch` 内部调用 `TaskService.handle_device_event`（:180），而该函数在 `task_id` 未知时 `raise TaskNotFound`（`service.py:307,312`），`_apply_*` 与能力分支也可能抛异常。异常一路冒到外层 `except Exception` → **记 1011 并关闭整条 WebSocket**。

**影响**：设备侧表现为"莫名掉线 + 重连"。若 Worker 因重连补报（Ledger pending reports）反复上报同一条服务端已不认识的 `task_id`（例如 DB 被重建/清理后），会形成**重连风暴**。事件级错误不该升级成连接级故障。

**建议**：`_dispatch` 整体包一层 `try/except Exception`，回 `error` 信封（`{"code": "event_rejected"}`）并继续读循环；只有协议级/认证级错误才断链。

#### H5 · `execute_command` 默认免确认（配置风险）

**证据**

```python
# server/app/core/config.py:90
self.agent_confirm_actions = os.getenv("AGENT_CONFIRM_ACTIONS", "false").lower() in ("1","true","yes")
```

`.env` 实测**未设置** `AGENT_CONFIRM_ACTIONS` → 取默认 `false`。而 `.env` 中 `AGENT_MODE=tool_agent`，意味着推理层现役就是 Tool-Using Agent。

**影响**：`retry_task` / `cancel_task`（WRITE 级）**恒需确认**，但 `execute_command`（**ACTION 级，唯一真正在设备上跑东西的工具**）反而默认免确认。结合钉钉群入口，**任何能 @ 机器人的人都能直接触发子电脑上的命令执行**，无二次确认。风险级别与确认策略倒挂了。

**建议**：生产 `AGENT_CONFIRM_ACTIONS=true`；或引入按命令的 `risk_level` 分级（命令注册表已有 `requires_confirmation` 类字段可复用），高风险命令强制确认。

---

### 4.3 Medium / Low 级问题

| ID | 问题 | 证据与影响 | 建议 |
| --- | --- | --- | --- |
| M1 | **下载无归属校验** | `api/artifact.py:150-162` 与 `api/capability.py:85-96` 只验 token 有效，不校验资源是否属于该设备。任一已注册设备可下载任意 artifact / 能力包（ID 不可猜，但"不可猜"不是授权） | 校验 `artifact.task_id → task.target_device_id == 当前设备`；能力包按 capability 授权发布给设备 |
| M2 | **注册码可爆破** | `security.py:11` 8 字符 × 32 字母表 = 40 bit，TTL 300s，`/api/devices/register` **无限速、无失败锁定** | 加 IP/设备指纹限流 + 失败计数；码长提至 12 位 |
| M3 | **ZIP 炸弹** | `package_service.py:103-117`、`client/cache.py:144-167` 只防 zip-slip（`startswith("/")` 与 `".."` 分段），不限制 `sum(info.file_size)` 与条目数；Worker 端 `zf.extractall(staging)` 无配额 | 校验解压后总字节上限（如 2GB）与条目数上限 |
| M4 | **`to_thread` 不可取消** | `uploader.py:92-102` 的 `asyncio.wait_for` 超时只让协程返回，**线程仍在跑**（同步 httpx 不感知）；`executors.py:209` 同理。反复超时会堆积线程/Socket | 用 `Semaphore` 限制并发线程数；依赖底层 socket 超时收敛；在 runbook 记录该行为 |
| M5 | **内存集合只增不减** | `TaskMonitor._stop_sent`（`monitor.py:36`，按 `(task_id, attempt_id)` 累积）；`api/websocket.py:208` 的 `_LAST_TOUCH`（设备删除也不清理） | 定期裁剪或改用 `WeakValueDictionary` / 带 TTL 的结构 |
| M6 | **双重 schema 管理** | Alembic `0001–0009` 与运行时 `Base.metadata.create_all` + 硬编码 `ALTER TABLE`（`workflow/db_models.py:120-186`）+ 手工脚本 `scripts/sync_mysql_schema.py` 并存。`ensure_*_columns` 的 DDL 无版本记录，漂移难以察觉 | 收敛为"迁移唯一事实源"；`ensure_*` 仅保留给测试直建表场景，并在其中显式标注（目前已这样注释，建议再加启动校验：迁移版本 vs 代码期望版本不一致则告警） |
| M7 | **文档滞后** | 见 §5 | 见 §5 |
| L1 | 硬编码业务设备名 | `config.py:73` `AGENT_DEFAULT_DEVICE_NAME` 默认 `"办公室电脑02"`（应用层配置被环境细节污染） | 移入 `agent_tools.json` 或彻底删除（注释已说明仅文案用途） |
| L2 | 无效代码 | `client/.../executors.py:482,493`：`validate` 内 `import httpx` 后立刻 `del httpx` | 删除 |
| L3 | 无谓对象构造 | `api/websocket.py:132`：为取时间戳而 `Envelope(type=...).timestamp` 构造一次性对象 | 直接用 `int(time.time()*1000)` |
| L4 | 兼容别名残留 | `task/service.py:32-33` `TERMINAL_TASK_STATES = TASK_TERMINAL_STATES` | 确认无引用后清理 |
| L5 | 过宽异常吞噬 | `dispatcher.py:40-42` `except Exception: return False` 会吞掉 DB 故障等真实错误，上层只看到"没派发" | 收窄为 `TaskNotFound` |
| L6 | `sweep` 中的 N+1 与丢弃返回值 | `monitor.py:98-103` 对每个 SENT 任务再查一次 attempts；`:103` 在 SENT-离线分支判超时后返回值被丢弃（不发送 cancel，也无审计事件） | 合并查询；该分支显式记事件 |

---

## 5. 文档 ↔ 代码一致性核对（M7 展开）

| 文档位置 | 文档说 | 实际代码 | 判定 |
| --- | --- | --- | --- |
| `README.md:133` | "38 个测试全部通过" | 实测 328 | **落后 ~2 代**（V1.0 时期） |
| `ARCHITECTURE.md:4`（页头） | "299 passed" | 实测 328 | 落后 |
| `ARCHITECTURE.md:475`（§14） | "193 passed" | 实测 328 | 落后（与同文件页头自相矛盾） |
| `docs/V1.5_RUNTIME_FIX_REPORT.md:4` | "328 passed / 0 failed" | **实测 328 / 0** | ✅ 准确 |
| `ARCHITECTURE.md:65`（§3） | 迁移列到 `0005` | 实际 `0001–0009` | 落后 4 个迁移 |
| `ARCHITECTURE.md:196`（§5） | "13 张表" | V1.4/V1.5 后至少 +5 表（`artifacts`/`capability_packages`/`capability_versions`/`automation_capabilities`/`worker_capabilities`） | 落后 |
| `ARCHITECTURE.md:1054`（§18.9） | "当前生产继续跑 mvp 模式，tool_agent 待实机验证" | `.env`：`AGENT_MODE=tool_agent` | **反向落后**（生产已切换） |
| `ARCHITECTURE.md` §3 目录树 | 未列 `capability_runtime/`、`artifact/`、`agent/notify.py`、`agent/tools/{task,capability,artifact,context,workflow}.py`、`core/background.py`、`api/{capability,artifact}.py` | 这些模块都存在且是 V1.4/V1.5 主力 | 结构性缺失 |
| `ARCHITECTURE.md` §14 测试表 | 未列 Phase 5/6/8 等新测试文件 | 存在 `test_phase5_event_lifecycle.py` 等 8 个 | 落后 |
| `README.md` API 表 | 无 capability / artifact / workflow 端点 | 实际存在 10+ 个 | 落后 |
| `AgentHub_V1.4_系统全景文档.md` | V1.4 | 代码已 V1.5 | 版本落后 |

**结论**：`docs/` 下的 V1.5 修复类文档新鲜且准确，但三个"门面文档"（README / ARCHITECTURE / 全景文档）都没跟上。**建议二选一**：要么把 README 精简为"30 秒上手 + 指向 ARCHITECTURE"，把版本细节全部收敛到 ARCHITECTURE 单一来源并在每次发版时强制更新；要么在 README 顶部加一行"本文档描述 V1.0 基线，当前实现见 ARCHITECTURE.md"。

---

## 6. 工程卫生

| 项 | 状态 |
| --- | --- |
| Git 树 | ✅ 干净（0 项未提交变更），9 个提交，里程碑命名清晰（`Reliable-1.0` / `Tool-Agent-1.0` / `Capability-1.0` / `Lifecycle-1.0`） |
| 敏感文件入库 | ✅ `.env`、`server/storage/`、`client.zip` 均**未**被 git 跟踪（`.gitignore` 正确） |
| 测试隔离 | ✅ `conftest.py` 已正确屏蔽 `dingtalk_*` 与 `openai_api_key`（`.pytest_audit_result.txt` 里留存的旧"测试期间真连钉钉"痕迹是修复前的） |
| 大体积数据 | ⚠️ `server/storage/` **36GB** 位于仓库目录内（20 个 400–658MB 的 xlsx）。未入 git，但会随目录拷贝/备份整体膨胀，且与代码同盘竞争 IO |
| 残留文件 | ⚠️ 工作目录存在 `client.zip`(160KB，与 `client/` 重复)、`.pt2–.pt9.txt`、`.pytest_audit_result.txt`(71KB)、`.pytest_byfile.txt`、`server/logs/`(6.9MB)。均已被 `.gitignore` 覆盖（未污染仓库），但属可清理噪音 |
| 依赖声明 | ⚠️ `pyproject.toml` 描述仍写 "V1.0"，`dingtalk` extra 声明 `dingtalk-stream>=0.24` 但代码明确**不用官方 SDK**（用裸 Stream 协议）→ 该 optional 依赖是死配置 |

---

## 7. 建议的处理优先顺序（仅建议，未改动任何代码）

| 顺序 | 动作 | 理由 |
| --- | --- | --- |
| 1 | `AGENT_CONFIRM_ACTIONS=true`（或按命令分级） | 一行环境变量，消除"群里任何人可无确认触发设备执行"（H5） |
| 2 | 启动时校验 `admin_token` 非空（非本地部署） | 把 fail-open 变 fail-fast，成本极低（H1） |
| 3 | 服务端上传/注册改为流式落盘 + 大小上限 | 当前 36GB/658MB 数据规模下最现实的 OOM 风险（H2） |
| 4 | 补 `ACCEPTED` 活性探测（用已有的 `attempt.accepted_at`） | 直接消除"卡 30 分钟"的原始生产症状（H3） |
| 5 | `_dispatch` 异常降级为 error 信封而非断链 | 防止重连风暴（H4） |
| 6 | 修 `test_capability_acceptance.py` 挂起 | 现在整个测试套件**必须靠 `--ignore` 才能跑完**，等于失去了"一条命令验证全量"的能力 |
| 7 | 统一文档事实源（测试数 / 版本 / 表数） | 新人成本；也是本次审计花最多时间核对的地方 |
| 8 | M1–M6、L1–L6 按需排期 | 多为加固与整洁性 |

---

## 8. 附：本次审计的原始证据

- `pytest tests -q --ignore=tests/agenthub/test_capability_acceptance.py` → `328 passed`，7m37s
- `pytest tests`（不排除）→ 挂死 >12 分钟无输出（`--timeout=120 --timeout-method=thread` 亦无法终止），与项目自留的 `.pytest_byfile.txt` 中 `test_capability_acceptance.py => HANG :: ..F` 一致
- SQLAlchemy 2.0.51 实测 `select().limit().where()` → 正确生成 `WHERE … ORDER BY … LIMIT`（排除疑似缺陷）
- 全端点鉴权枚举：管理面路由均带 `Depends(require_admin)`；免鉴权端点仅 4 个且为设计使然（`devices/register` 凭一次性码、artifact 上传/下载与能力包下载凭设备 Bearer、WS 凭 Bearer）
- 危险调用扫描：全库**无** `eval` / `exec` / `shell=True` / `os.system` / `pickle.loads`；`subprocess` 全部为 list 参数形式且脚本路径经白名单校验
- V1.5 根因文档 8 个问题的修复逐条比对：7 项确认落地（通知层、异步 `run_capability`、进度 seq、去 shield + `spawn()`、下载看门狗、to_thread 迁移、对账清扫），1 项（ACCEPTED 活性探测）未落地且修复报告中未显式记录取舍

---

*本报告为只读审计产物，未对项目任何源码、配置、迁移或数据做修改。*
