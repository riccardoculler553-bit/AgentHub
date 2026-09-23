# AgentHub

AgentHub 是构建在 DeviceLink（多设备 WebSocket 注册与连接管理平台）之上的**任务控制与智能编排层**：Admin/主 Agent 把自然语言或结构化请求转化为任务（Task），经命令注册表（Command Registry）与能力注册表（Capability Registry）双重校验后，通过 DeviceLink 长连接派发到指定子电脑的 Worker 执行，回报结果、支持多步串行、自动重试、超时看门狗、取消与离线重派。

技术栈：Python 3.11+ / FastAPI / WebSocket / SQLAlchemy 2.0 / MySQL 8 / Alembic / LangGraph。

```
Internet ──HTTPS/WSS──> 公网暴露层 ──> DeviceLink/AgentHub Server ──WS──> Device A / B / C ...
                        (Funnel /        FastAPI :8000（仅绑 127.0.0.1）
                         Tunnel /         │
                         ngrok /        MySQL（持久状态） + Memory Hub（实时连接）
                         Nginx)
```

详细架构见 [ARCHITECTURE.md](ARCHITECTURE.md)。

**V1.6 范围：Fleet Execution & Operationalization（多设备执行与运行管理）。** 硬规则：一次用户请求 → 一次 Capability Execution → 一个 Task → 一台 Worker。SaaS 多租户、计费、MSP 配额、Job 扇出不在该版本内。范围与 `5022651` 审计见 [docs/v1.6-product-roadmap.md](docs/v1.6-product-roadmap.md)；P0 实现在其后的 Fleet-1.0，不在本文件。

## 网络模型（核心架构原则）

> **Server 公网可达，Client 无需公网可达。**

- Server 绑定 `127.0.0.1:8000`，公网暴露由独立的**暴露层**完成：Tailscale Funnel / Cloudflare Tunnel / ngrok / FRP / Nginx+VPS 任选其一。
- 核心代码不感知暴露层实现，未来切换暴露方案只需改 `SERVER_PUBLIC_URL`，协议与代码零修改。
- Client 主动发起 WSS 出站连接，因此不需要公网 IP、不需要端口映射、不需要安装 Tailscale；Server → Client 的消息复用已建立的 WebSocket。

三种地址的用途区分：

| 地址形态 | 用途 |
| --- | --- |
| `192.168.x.x` | 仅局域网测试 |
| `100.x.x.x` | Tailscale Tailnet 内部地址 |
| `https://xxxxx.ts.net` | Tailscale Funnel 公网入口（子电脑统一用这个） |

服务端 `.env` 中的 `SERVER_PUBLIC_URL` 记录当前公网 Base URL，供 `--server` 参数使用：

```env
SERVER_PUBLIC_URL=https://your-host.ts.net   # Funnel 暴露
SERVER_PUBLIC_URL=http://127.0.0.1:8000      # 本地开发
```

## 核心概念

- **四层平面**：Reasoning（LangGraph 主 Agent）→ Control（任务引擎/注册表）→ Communication（DeviceLink）→ Execution（Worker 执行器）。
- **Device ≠ Connection**：`device_id` 是长期身份，`connection_id` 是一次 WebSocket 会话；一台设备允许同时存在多个连接。
- **注册码**：一次性、5 分钟有效、SHA256 哈希落库（`DL-XXXX-XXXX`）。
- **Token**：`dl_dev_...`，明文只在注册响应中出现一次，数据库只存哈希；可撤销。
- **Command vs Executor**：服务端命令注册表定义"允许做什么"，Worker 本地注册表声明"能做什么"，双侧一致才可执行。
- **在线状态**：连接 + 业务心跳（15s）+ `last_seen_at` 共同判断；断线不立即 OFFLINE，经过 45s 宽限期才标记。
- **协议**：统一 Envelope `{id, type, version, timestamp, data}`；V1.0 消息类型：`heartbeat / heartbeat_ack / device.connected / device.disconnected / message / message_ack / error / device.capabilities / task.dispatch / task.accept / task.running / task.progress / task.result / task.cancel`。
- **重连**：指数退避（1,2,4,8,16,30s）+ jitter；认证失效（4401/4403）停止重连。
- **状态分离**：MySQL 是持久状态权威来源；WebSocket/Hub 是实时状态，不落库。

## 快速开始

```bash
# 1. 配置数据库与管理令牌（.env，参考 .env.example）
DB_HOST=127.0.0.1
DB_PORT=3306
DB_USER=datapilot
DB_PASSWORD=...
DB_NAME=datapilot
AGENTHUB_ADMIN_TOKEN=your-admin-token   # 留空 = 开放模式（仅限本地开发）

# 2. 安装依赖
pip install -e ".[agent]"     # 服务端（含 LangGraph 主 Agent）

# 3. 迁移建表
cd server
alembic upgrade head

# 4. 启动服务器（仅绑 127.0.0.1，公网访问走暴露层）
python server/main.py          # http://127.0.0.1:8000

# 5. 开启公网暴露（任选一种暴露层，本机当前使用 Tailscale Funnel）
tailscale funnel --bg 8000     # 需管理员权限；tailscale funnel status 查看公网 URL
```

浏览器打开 `http://127.0.0.1:8000` 进入 Dashboard v2（Admin 登录）：设备 / 任务 / 命令 / 能力 / Agent 五个页签，任务详情含步骤与尝试 Timeline。

### 设备端

```bash
# 首次运行（注册）：--server 填公网 Base URL（本地开发可填 http://127.0.0.1:8000）
python client/main.py --server https://your-host.ts.net --code DL-XXXX-XXXX --name "办公室电脑01"

# 之后直接运行：自动读取本地身份文件恢复身份并重连
python client/main.py --server https://your-host.ts.net
```

```bash
pip install httpx websockets
```

客户端只接收 `--server` 一个地址参数，内部自动推导 WebSocket 地址（`https://` → `wss://`、`http://` → `ws://` + `/api/ws/device`）。

身份默认存放在 `~/.devicelink/device.json`，可用环境变量 `DEVICELINK_HOME` 指定其他目录（一台机器跑多设备时使用）。

Worker 能力声明在 `client/worker/capabilities.json`（当前：`echo` / `python.demo`），连接建立后自动上报 `device.capabilities`。

## API

| 方法   | 路径                                  | 说明                     |
| ------ | ------------------------------------- | ------------------------ |
| POST   | `/api/device-registration`            | 创建一次性注册码（5min） |
| POST   | `/api/devices/register`               | 设备注册，返回 token     |
| GET    | `/api/devices`                        | 设备列表（含在线状态）   |
| GET    | `/api/devices/{device_id}`            | 设备详情                 |
| POST   | `/api/devices/{device_id}/revoke`     | 撤销设备（token 失效）   |
| POST   | `/api/devices/{device_id}/messages`   | 向设备发消息             |
| GET    | `/api/devices/{device_id}/capabilities` | 设备能力上报结果       |
| GET    | `/api/capabilities`                   | 全部设备能力（按设备分组） |
| POST   | `/api/tasks`                          | 创建任务（四重校验）     |
| GET    | `/api/tasks/{task_id}`                | 任务详情（steps/attempts/events） |
| POST   | `/api/tasks/{task_id}/cancel`         | 取消任务                 |
| POST   | `/api/tasks/{task_id}/retry`          | 重试任务（attempts 账本） |
| GET    | `/api/commands`                       | 命令注册表               |
| POST   | `/api/agent/run`                      | 自然语言 → 主 Agent 闭环运行 |
| WS     | `/api/ws/device`                      | 设备 WebSocket（Bearer 认证） |
| GET    | `/api/health`                         | 健康检查                 |

管理接口需 `X-Admin-Token` 请求头（对应 `AGENTHUB_ADMIN_TOKEN`）；WebSocket 认证使用 `Authorization: Bearer <device_token>`，认证失败关闭码 4401，token/device 被撤销为 4403（客户端收到后停止自动重连）。

## 数据库表

- DeviceLink：`users` / `devices` / `device_registration_codes` / `device_tokens` / `websocket_connections`（连接审计）
- AgentHub：`commands`（命令注册表）/ `device_capabilities`（能力注册表）/ `tasks` / `task_steps` / `task_attempts`（尝试账本）/ `task_events`（全生命周期事件流）

## 测试

```bash
python -m pytest tests -q
```

38 个测试全部通过：

- 单元测试：TokenService、RegistrationService、重连退避、协议解析
- 集成测试：注册全流程、WebSocket 认证/心跳/多连接/消息下发/撤销关闭 4403
- AgentHub 验收：多设备隔离、离线重派、重复派发幂等、重试 attempts 账本、规则规划器

测试使用文件型 SQLite（多线程 TestClient 会话安全）；生产使用 MySQL（由 `DATABASE_URL` 或 `DB_*` 环境变量决定）。

## V1.0 之后

跨设备多步工作流（`task_steps.device_id` 已预留）→ Worker 并发与断线补报 → 事件驱动派发 → 对话式多轮 Agent。
