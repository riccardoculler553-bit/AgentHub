"""ShadowBot busy detection / launch confirmation / success-box cleanup.

Ported from the proven dingtalk-xbot-audit project (audit.py) so the Worker
handles "电脑正在运行其他程序" scenarios:

- busy check: read ShadowBot's daily log for start/end markers (default) or
  match process image names via tasklist
- settle: wait after a task just ended so 影刀 tears down cleanly
- launch confirm: poll the log until a NEW start marker appears, retrying
- success box: close the "运行成功" toast via WM_CLOSE or a new run is swallowed
"""

import asyncio
import csv
import io
import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_WM_CLOSE = 0x0010
_WM_SYSCOMMAND = 0x0112
_SC_CLOSE = 0xF060

_ACTIVE_PROCESSES: list[subprocess.Popen] = []


def launch_robot(argv: list[str]) -> subprocess.Popen:
    """Start the robot process (fire and forget, like the reference impl).

    Returns the Popen so the caller can wait/terminate. Popen objects are
    kept alive so OS handles get reaped after exit."""
    process = subprocess.Popen(argv, shell=False)
    _ACTIVE_PROCESSES[:] = [p for p in _ACTIVE_PROCESSES if p.poll() is None]
    _ACTIVE_PROCESSES.append(process)
    return process


async def wait_robot(pid: int) -> int | None:
    """Wait for the launched process to exit; returns its exit code (or None
    if the pid is unknown to this module)."""
    process = next((p for p in _ACTIVE_PROCESSES if p.pid == pid), None)
    if process is None:
        return None
    while process.poll() is None:
        await asyncio.sleep(0.5)
    return process.returncode


def terminate_robot(pid: int) -> None:
    process = next((p for p in _ACTIVE_PROCESSES if p.pid == pid), None)
    if process is not None and process.poll() is None:
        try:
            process.terminate()
        except OSError:
            pass


# ---------------------------------------------------------------- busy check


def is_process_running(process_name: str, extra_names=None) -> bool:
    """Exact image-name check via tasklist. Failure counts as "not running"
    so a launch still goes ahead (same behaviour as the reference project)."""
    if os.name != "nt":
        return False

    exact = set()
    for name in [process_name, *(extra_names or [])]:
        name = Path(name).name.strip()
        if name:
            exact.add(name.lower())

    command = ["tasklist", "/FO", "CSV", "/NH"]
    kwargs = {"capture_output": True, "text": True, "timeout": 10}
    kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    try:
        result = subprocess.run(command, **kwargs)
    except (subprocess.TimeoutExpired, OSError) as exc:
        logger.warning("process check failed: %s", exc)
        return False

    if result.returncode != 0:
        logger.warning("tasklist returned %s", result.returncode)
        return False

    for image in _tasklist_images(result.stdout):
        if image in exact:
            logger.info("process check: found running process %s", image)
            return True
    logger.info("process check: no running process matches %s", sorted(exact))
    return False


def _tasklist_images(output: str):
    images = []
    for row in csv.reader(io.StringIO(output or "")):
        if row and row[0].strip():
            images.append(row[0].strip().lower())
    return images


def is_task_running(log_dir: str) -> bool:
    """True when today's ShadowBot log shows a task currently running.

    Start markers: `new task` / `xbot engine running` / `robot task ... started`
    End markers:   `task end` / `task exit` / `xbot engine exited` /
                   `python engine exited` / `robot task ... exited/closed`
    The latest marker wins; missing log = not running."""
    tail = read_log_tail(log_dir)
    if tail is None:
        logger.info("task check: no ShadowBot log file found")
        return False
    last_start, last_end, _, _ = scan_markers(tail)
    running = last_start > last_end
    logger.info("task check: %s (last_start=%s last_end=%s)", "task running" if running else "idle", last_start, last_end)
    return running


def scan_markers(tail: str):
    """Return (last_start_idx, last_end_idx, last_start_time, last_end_time)."""
    last_start, last_end = -1, -1
    last_start_time = last_end_time = None
    for index, line in enumerate(tail.splitlines()):
        kind = _classify_line(line.lower())
        if kind == "start":
            last_start = index
            last_start_time = _line_timestamp(line) or last_start_time
        elif kind == "end":
            last_end = index
            last_end_time = _line_timestamp(line) or last_end_time
    return last_start, last_end, last_start_time, last_end_time


def _classify_line(lower: str) -> str | None:
    if (
        "new task" in lower
        or "xbot engine running" in lower
        or ("robot task" in lower and "started" in lower)
    ):
        return "start"
    if (
        "task end" in lower
        or "task exit" in lower
        or "xbot engine exited" in lower
        or "python engine exited" in lower
        or ("robot task" in lower and ("exited" in lower or "closed" in lower))
    ):
        return "end"
    return None


def _line_timestamp(line: str):
    try:
        return datetime.strptime(line[:23], "%Y-%m-%d %H:%M:%S,%f")
    except ValueError:
        return None


def today_log_path(log_dir: str) -> Path | None:
    expanded = os.path.expandvars(str(log_dir or ""))
    if not expanded:
        return None
    return Path(expanded) / (datetime.now().strftime("%Y%m%d") + ".log")


def read_log_tail(log_dir: str, max_bytes: int = 65536) -> str | None:
    log_file = today_log_path(log_dir)
    if log_file is None or not log_file.is_file():
        return None
    with open(log_file, "r", encoding="utf-8", errors="replace") as handle:
        handle.seek(0, os.SEEK_END)
        size = handle.tell()
        start = max(0, size - max_bytes)
        handle.seek(start)
        if start > 0:
            handle.readline()
        return handle.read()


# ------------------------------------------------------------------ lifecycle


async def settle_after_task_end(log_dir: str, settle_seconds: int) -> None:
    """Wait briefly when the previous task just ended, so 影刀 finishes
    teardown and accepts a new run."""
    if not settle_seconds or settle_seconds <= 0:
        return
    tail = read_log_tail(log_dir)
    if tail is None:
        return
    last_start, last_end, _, last_end_time = scan_markers(tail)
    if last_end_time is None or last_end <= last_start:
        return
    age = (datetime.now() - last_end_time).total_seconds()
    if age < settle_seconds:
        wait = settle_seconds - age
        logger.info("task check: previous task ended %.1fs ago, waiting %.1fs", age, wait)
        await asyncio.sleep(wait)


async def confirm_launch(log_dir: str, baseline_start_time, verify_seconds: int) -> bool:
    """Poll the log until a NEW start marker appears (or timeout)."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + verify_seconds
    while True:
        tail = read_log_tail(log_dir)
        if tail is not None:
            _, _, last_start_time, _ = scan_markers(tail)
            if last_start_time is not None and (
                baseline_start_time is None or last_start_time > baseline_start_time
            ):
                return True
        remaining = deadline - loop.time()
        if remaining <= 0:
            return False
        await asyncio.sleep(min(0.5, remaining))


def build_busy_checker(check_mode: str, log_dir: str, process_names, exe_name: str):
    """check_mode: "log" (default) | "process" | "off"."""
    mode = (check_mode or "log").lower()
    if mode == "off":
        return lambda: False
    if mode == "process":
        names = [n for n in (process_names or ()) if n] or [exe_name]
        return lambda: is_process_running(names[0], names[1:])
    return lambda: is_task_running(log_dir)


def task_end_seen(log_dir: str, started_after) -> bool:
    """True when the log shows a task END that happened after `started_after`
    (the start timestamp of OUR run). This is the reliable completion signal:
    ShadowBot.exe may exit immediately as a launcher or stay open after the
    task - only the log's end marker proves the robot actually finished."""
    tail = read_log_tail(log_dir)
    if tail is None:
        return False
    last_start, last_end, _, last_end_time = scan_markers(tail)
    if last_end <= last_start or last_end_time is None:
        return False
    return started_after is None or last_end_time > started_after


# ------------------------------------------------------------- success box


def _windows_api():
    import ctypes
    from ctypes import wintypes

    user32 = ctypes.windll.user32
    kernel32 = ctypes.windll.kernel32
    enum_windows_proc = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)

    def enum_windows(callback):
        proc = enum_windows_proc(callback)
        return bool(user32.EnumWindows(proc, 0))

    def is_window_visible(hwnd):
        return bool(user32.IsWindowVisible(hwnd))

    def get_window_text(hwnd):
        length = user32.GetWindowTextLengthW(hwnd)
        if not length:
            return ""
        buffer = ctypes.create_unicode_buffer(length + 1)
        user32.GetWindowTextW(hwnd, buffer, length + 1)
        return buffer.value

    def post_close(hwnd):
        if user32.PostMessageW(hwnd, _WM_CLOSE, 0, 0):
            return True
        return bool(user32.PostMessageW(hwnd, _WM_SYSCOMMAND, _SC_CLOSE, 0))

    def pid_of(hwnd):
        pid = wintypes.DWORD()
        user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
        return pid.value

    def process_image_name(pid):
        process = kernel32.OpenProcess(0x1000, False, pid)  # QUERY_LIMITED_INFORMATION
        if not process:
            return ""
        try:
            buffer = ctypes.create_unicode_buffer(1024)
            size = wintypes.DWORD(len(buffer))
            if kernel32.QueryFullProcessImageNameW(process, 0, buffer, ctypes.byref(size)):
                return buffer.value
            return ""
        finally:
            kernel32.CloseHandle(process)

    return {
        "enum_windows": enum_windows,
        "is_window_visible": is_window_visible,
        "get_window_text": get_window_text,
        "post_close": post_close,
        "pid_of": pid_of,
        "process_image_name": process_image_name,
    }


def _is_success_box(api, hwnd, title, keywords) -> bool:
    """影刀 completion toast: title keyword, or RobotRunnerView owned by a
    ShadowBot.* process."""
    if any(keyword in title for keyword in keywords):
        return True
    if "robotrunner" not in title.lower():
        return False
    image = api["process_image_name"](api["pid_of"](hwnd)) or ""
    return os.path.basename(image).lower().startswith("shadowbot")


def close_success_box(keywords=("运行成功", "执行成功")):
    """Close 影刀's "运行成功" toast so a new run is not swallowed.

    Returns the list of (hwnd, title) pairs that accepted the close."""
    if os.name != "nt":
        return []
    api = _windows_api()
    found: list[tuple[int, str]] = []

    def _enum(hwnd, _lparam):
        if api["is_window_visible"](hwnd):
            title = api["get_window_text"](hwnd)
            if title and _is_success_box(api, hwnd, title, keywords):
                found.append((hwnd, title))
        return True

    api["enum_windows"](_enum)

    closed = []
    for hwnd, title in found:
        logger.info("closing success box: title=%r hwnd=%s", title, hwnd)
        if api["post_close"](hwnd):
            closed.append((hwnd, title))
        else:
            logger.warning("failed to close success box: title=%r hwnd=%s", title, hwnd)
    return closed
