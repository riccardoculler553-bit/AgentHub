"""Task terminal-state observer (V1.3 §28/§121).

TaskService fires notify_task_terminal() when a task reaches a terminal
state; interested layers (Workflow Engine) subscribe without the Task Engine
importing them (§203: workflow -> task one direction in code, notification
via this tiny observer keeps Task Engine workflow-agnostic).

Callbacks run synchronously in the caller's thread and MUST be cheap and
exception-safe: a broken subscriber can never break task bookkeeping.
"""

import logging
import threading
from typing import Callable

logger = logging.getLogger(__name__)

_listeners: list[Callable[[str], None]] = []
_lock = threading.Lock()


def subscribe_task_terminal(callback: Callable[[str], None]) -> None:
    with _lock:
        _listeners.append(callback)


def reset_task_terminal_listeners() -> None:
    """Test isolation helper."""
    with _lock:
        _listeners.clear()


def notify_task_terminal(task_id: str) -> None:
    with _lock:
        listeners = list(_listeners)
    for callback in listeners:
        try:
            callback(task_id)
        except Exception:  # noqa: BLE001 - observer boundary
            logger.exception("task terminal listener failed for %s", task_id)
