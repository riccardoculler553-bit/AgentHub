"""YingdaoExecutor pipeline unit tests (ported from dingtalk-xbot-audit):
busy gate (log mode), wait-for-exit success, launch confirmation, timeout."""

import asyncio
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from worker.executor import ExecutionError
from worker.executors import busy_check
from worker.executors.yingdao import YingdaoExecutor


def _wait_no_alive_processes(timeout: float = 10.0) -> None:
    """terminate() on Windows is async: give the OS a moment to reap them."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if all(p.poll() is not None for p in busy_check._ACTIVE_PROCESSES):
            return
        time.sleep(0.1)


def _write_log_line(log_dir: Path, text: str, ts: datetime | None = None) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = (ts or datetime.now()).strftime("%Y-%m-%d %H:%M:%S,%f")[:23]
    path = busy_check.today_log_path(str(log_dir))
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(f"{stamp} {text}\n")


def _make_executor(tmp_path: Path, **overrides) -> YingdaoExecutor:
    executor = YingdaoExecutor()
    config = {
        "robot_uuid": "test-uuid",
        "shadowbot_path": sys.executable,  # any runnable exe; args do the work
        "check_mode": "off",
        "log_dir": str(tmp_path / "xbot-log"),
        "settle_after_end_seconds": 0,
        "launch_verify_seconds": 0,
        "launch_max_retries": 0,
        "close_success_box": False,
    }
    config.update(overrides)
    executor.configure(config)
    return executor


async def _run(executor: YingdaoExecutor, params=None, config=None):
    progress_calls: list[tuple[int, str]] = []

    async def progress(pct, message):
        progress_calls.append((pct, message))

    result = await executor.execute(params or {}, config or {}, progress, asyncio.Event())
    return result, progress_calls


def test_busy_log_mode_blocks_launch(tmp_path):
    """ShadowBot log shows a running task -> EXECUTOR_BUSY, nothing launched."""
    log_dir = tmp_path / "xbot-log"
    _write_log_line(log_dir, "INFO xbot engine running with task 123")  # start, no end
    executor = _make_executor(tmp_path, check_mode="log")
    with pytest.raises(ExecutionError) as excinfo:
        asyncio.run(_run(executor))
    assert excinfo.value.code == "EXECUTOR_BUSY"


def test_log_mode_idle_after_end_allows_launch(tmp_path):
    log_dir = tmp_path / "xbot-log"
    _write_log_line(log_dir, "INFO new task started")
    _write_log_line(log_dir, "INFO task exit code 0")  # latest marker: ended
    executor = _make_executor(
        tmp_path,
        check_mode="log",
        args=["-c", "import time; time.sleep(0.2)"],
        timeout=60,
    )
    result, _ = asyncio.run(_run(executor))
    assert result["exit_code"] == 0
    assert result["launch_confirmed"] is True  # verify=0 -> skip confirmation


def test_launch_confirmed_via_log_marker(tmp_path):
    """verify>0: the fake robot appends a start marker to the ShadowBot log;
    the executor must see a NEW start and proceed."""
    log_dir = tmp_path / "xbot-log"
    old = datetime.now() - timedelta(minutes=5)
    _write_log_line(log_dir, "INFO new task started", ts=old)
    _write_log_line(log_dir, "INFO task exit", ts=old)
    log_path = busy_check.today_log_path(str(log_dir))

    script = (
        "import time\n"
        f"time.sleep(0.3)\n"
        "stamp = __import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:23]\n"
        f"open(r'{log_path}', 'a', encoding='utf-8').write(stamp + ' INFO new task started\\n')\n"
        "time.sleep(0.2)\n"
    )
    executor = _make_executor(
        tmp_path,
        check_mode="log",
        launch_verify_seconds=5,
        args=["-c", script],
        timeout=60,
    )
    result, _ = asyncio.run(_run(executor))
    assert result["exit_code"] == 0
    assert result["launch_confirmed"] is True


def test_launch_unconfirmed_fails_after_retries(tmp_path):
    """verify>0 but the process never writes a start marker -> retries then
    EXECUTOR_LAUNCH_FAILED, and the zombie process is terminated."""
    log_dir = tmp_path / "xbot-log"
    old = datetime.now() - timedelta(minutes=5)
    _write_log_line(log_dir, "INFO new task started", ts=old)
    _write_log_line(log_dir, "INFO task exit", ts=old)
    executor = _make_executor(
        tmp_path,
        check_mode="log",
        launch_verify_seconds=1,
        launch_max_retries=1,
        args=["-c", "import time; time.sleep(30)"],
        timeout=60,
    )
    with pytest.raises(ExecutionError) as excinfo:
        asyncio.run(_run(executor))
    assert excinfo.value.code == "EXECUTOR_LAUNCH_FAILED"
    # both retry attempts must have been killed, none left running
    _wait_no_alive_processes()
    assert all(p.poll() is not None for p in busy_check._ACTIVE_PROCESSES)


def test_timeout_terminates_long_robot(tmp_path):
    executor = _make_executor(
        tmp_path,
        args=["-c", "import time; time.sleep(30)"],
        timeout=1,
    )
    with pytest.raises(ExecutionError) as excinfo:
        asyncio.run(_run(executor))
    assert excinfo.value.code == "EXECUTOR_TIMEOUT"
    _wait_no_alive_processes()
    assert all(p.poll() is not None for p in busy_check._ACTIVE_PROCESSES)


def test_missing_robot_uuid_rejected(tmp_path):
    executor = _make_executor(tmp_path)
    executor.robot_uuid = None
    with pytest.raises(ValueError):
        asyncio.run(_run(executor))
