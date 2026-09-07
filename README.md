# AgentHub
AgentHub 是构建在 DeviceLink（多设备 WebSocket 注册与连接管理平台）之上的**任务控制与智能编排层**：Admin/主 Agent 把自然语言或结构化请求转化为任务（Task），经命令注册表（Command Registry）与能力注册表（Capability Registry）双重校验后，通过 DeviceLink 长连接派发到指定子电脑的 Worker 执行，回报结果、支持多步串行、自动重试、超时看门狗、取消与离线重派。
