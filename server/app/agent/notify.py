"""Task terminal -> user notification (Phase 3).

问题 3 的修复：agent run（含同步等待）先于任务终态结束时，任务真正完成/
失败/超时/取消的那一刻，用户在群里收不到任何消息。

本模块订阅 Task Engine 的 notify_task_terminal 广播（task/events.py）：
- 通过 AgentRun.task_id 找到发起任务的会话（run_capability 在创建任务时用
  set_task 打通 Task -> AgentRun，Phase 4）；
- 仅当 run 已经结束（agent 先于任务放弃）时才主动推送——run 仍在进行说明
  agent 正在同步等待，结果由 agent 自己带出，避免双重打扰；
- 幂等键 (task_id, status)：一个终态只通知一次；
- 回调在事件循环里只做 create_task（task/events.py 契约：cheap + safe），
  DB 查询与钉钉推送全部在后台任务里，失败只记日志。
"""

import asyncio
import logging

from sqlalchemy import select

logger = logging.getLogger(__name__)

_sender = None
# (task_id, status) -> notified。终态不可逆，理论上不会重复；集合兜底防御
# notify_task_terminal 的多次触发（如 watchdog 与 result 竞态的另一侧）。
_dedupe: set[tuple[str, str]] = set()
_DEDUPE_CAP = 10000


def install_agent_task_notifier(sender) -> None:
    """Wire the terminal-notification listener to a ReplySender (main.py)."""
    global _sender
    from app.task.events import subscribe_task_terminal

    _sender = sender
    subscribe_task_terminal(_on_terminal)


def _on_terminal(task_id: str) -> None:
    """Sync, cheap, exception-safe (task/events.py contract)."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return  # no running loop (script/startup paths) - nothing to send to
    loop.create_task(_notify(task_id))


async def _notify(task_id: str) -> None:
    from app.agent.db_models import AgentRun
    from app.db.database import SessionLocal
    from app.task.db_models import Task, TaskAttempt

    sender = _sender
    if sender is None:
        return
    try:
        with SessionLocal() as db:
            task = db.scalars(select(Task).where(Task.task_id == task_id)).first()
            if task is None:
                return
            run = db.scalars(
                select(AgentRun).where(AgentRun.task_id == task_id).order_by(AgentRun.id.desc())
            ).first()
            if run is None:
                return  # not agent-initiated (API/workflow) - no channel to push
            if run.status not in ("SUCCESS", "FAILED", "CANCELLED"):
                return  # agent still running: it will report the result itself
            key = (task_id, task.status)
            if key in _dedupe:
                return
            if len(_dedupe) > _DEDUPE_CAP:
                _dedupe.clear()
            _dedupe.add(key)
            text = _render(db, task)
            channel, conversation_id = run.channel, run.conversation_id
            webhook = run.reply_webhook
    except Exception:  # noqa: BLE001 - notification must never break tasks
        logger.exception("task terminal notification failed for %s", task_id)
        return
    try:
        await sender.send_reply(
            channel=channel, conversation_id=conversation_id, text=text, webhook=webhook
        )
    except Exception:  # noqa: BLE001 - e.g. expired sessionWebhook
        logger.exception("terminal notification push failed for task %s", task_id)


def _render(db, task) -> str:
    """User-facing fact sheet (§139: facts, never internals)."""
    from app.task.db_models import TaskAttempt

    name = task.name or task.task_id
    if task.status == "SUCCESS":
        lines = [f"任务已完成：{name}（{task.task_id}）"]
        arts = task.artifact_ids or []
        if isinstance(arts, list) and arts:
            lines.append(f"产物 {len(arts)} 个，已入库。")
            lines.append(f"如需下载到指定目录，回复：把 {task.task_id} 下载到 <目录>")
        return "\n".join(lines)
    if task.status == "CANCELLED":
        return f"任务已取消：{name}（{task.task_id}）"
    # FAILED / TIMEOUT: carry the last attempt's error tail (V1.5 keeps tails)
    attempt = db.scalars(
        select(TaskAttempt)
        .where(TaskAttempt.task_id == task.task_id)
        .order_by(TaskAttempt.id.desc())
        .limit(1)
    ).first()
    detail = (attempt.error_message or attempt.error_code or "") if attempt else ""
    header = "任务执行超时" if task.status == "TIMEOUT" else "任务执行失败"
    lines = [f"{header}：{name}（{task.task_id}）"]
    if detail:
        lines.append(f"原因：{detail[:500]}")
    lines.append("可在群里说「重试这个任务」重新派发。")
    return "\n".join(lines)
