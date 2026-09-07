"""YingdaoExecutor: launch the local 影刀 (ShadowBot) RPA robot (PDF §42-§47).

The real command lives HERE and only here - the Agent only ever says
"yingdao.audit". Configuration comes from the worker's capabilities.json:

    {
      "name": "yingdao.audit",
      "version": "1.0",
      "executor_type": "yingdao",
      "robot_uuid": "xxxxxxxx",
      "shadowbot_path": "C:\\Program Files\\ShadowBot\\ShadowBot.exe",
      "args": ["shadowbot:Run?robot-uuid={robot_uuid}"],
      "wait_for_exit": true,
      "check_mode": "log",
      "log_dir": "%LOCALAPPDATA%/ShadowBot/log",
      "process_names": [],
      "settle_after_end_seconds": 3,
      "launch_verify_seconds": 4,
      "launch_max_retries": 2,
      "close_success_box": true,
      "success_box_keywords": ["运行成功", "执行成功"]
    }

Execution pipeline (ported from the proven dingtalk-xbot-audit project):
busy check -> settle after previous task -> close success toast -> launch
-> confirm via log marker (retry) -> optionally wait for process exit.
Busy runs fail with EXECUTOR_BUSY so the Agent can answer
"当前正在运行其他程序，请稍后再试" and/or route to another device.
"""

import asyncio
import logging
import time
from pathlib import Path

from worker.executor import ExecutionError, Executor
from worker.executors import busy_check

logger = logging.getLogger(__name__)

DEFAULT_ARGS = ["shadowbot:Run?robot-uuid={robot_uuid}"]


class YingdaoExecutor(Executor):
    name = "yingdao"

    def __init__(self) -> None:
        self.robot_uuid: str | None = None
        self.shadowbot_path: str | None = None
        self.args: list[str] = list(DEFAULT_ARGS)
        self.default_timeout: int = 1800
        self.wait_for_exit: bool = True
        # busy/launch pipeline settings
        self.check_mode: str = "log"
        self.log_dir: str = "%LOCALAPPDATA%/ShadowBot/log"
        self.process_names: tuple[str, ...] = ()
        self.settle_after_end_seconds: int = 3
        self.launch_verify_seconds: int = 4
        self.launch_max_retries: int = 2
        self.close_success_box: bool = True
        self.success_box_keywords: tuple[str, ...] = ("运行成功", "执行成功")

    def configure(self, config: dict) -> None:
        self.robot_uuid = config.get("robot_uuid") or None
        self.shadowbot_path = config.get("shadowbot_path") or None
        args = config.get("args")
        if isinstance(args, list) and args:
            self.args = [str(a) for a in args]
        self.default_timeout = int(config.get("timeout") or self.default_timeout)
        self.wait_for_exit = bool(config.get("wait_for_exit", True))
        self.check_mode = str(config.get("check_mode") or "log")
        self.log_dir = str(config.get("log_dir") or self.log_dir)
        names = config.get("process_names")
        self.process_names = tuple(str(n) for n in names) if isinstance(names, list) else ()
        self.settle_after_end_seconds = int(config.get("settle_after_end_seconds", 3))
        self.launch_verify_seconds = int(config.get("launch_verify_seconds", 4))
        self.launch_max_retries = int(config.get("launch_max_retries", 2))
        self.close_success_box = bool(config.get("close_success_box", True))
        keywords = config.get("success_box_keywords")
        if isinstance(keywords, list) and keywords:
            self.success_box_keywords = tuple(str(k) for k in keywords)

    def validate(self, params: dict) -> None:
        if not self.robot_uuid:
            raise ValueError("yingdao.audit is not configured: robot_uuid missing in capabilities.json")
        if not self.shadowbot_path:
            raise ValueError("yingdao.audit is not configured: shadowbot_path missing in capabilities.json")

    @property
    def _exe_name(self) -> str:
        return Path(self.shadowbot_path or "ShadowBot.exe").name

    async def execute(self, params, config, progress, cancel) -> dict:
        self.validate(params)
        timeout = int(config.get("timeout") or self.default_timeout)
        busy_checker = busy_check.build_busy_checker(
            self.check_mode, self.log_dir, self.process_names, self._exe_name
        )

        # 1) busy gate: ShadowBot already working on something else?
        await progress(2, "检查影刀是否空闲")
        try:
            already_running = await asyncio.to_thread(busy_checker)
        except Exception:
            logger.exception("busy check failed; continuing launch")
            already_running = False
        if already_running:
            raise ExecutionError("EXECUTOR_BUSY", "影刀正在运行其他任务")

        # 2) settle: previous task just ended -> let 影刀 tear down first
        try:
            await busy_check.settle_after_task_end(self.log_dir, self.settle_after_end_seconds)
        except Exception:
            logger.exception("settle wait failed; continuing launch")

        # 3) baseline for launch confirmation
        tail_before = busy_check.read_log_tail(self.log_dir)
        baseline_start_time = None
        if tail_before is not None:
            baseline_start_time = busy_check.scan_markers(tail_before)[2]
        can_verify = self.launch_verify_seconds > 0 and tail_before is not None

        # 4) launch with confirmation + retry (close success toast first)
        pid: int | None = None
        confirmed = can_verify is False
        attempts = max(1, self.launch_max_retries + 1)
        for attempt in range(1, attempts + 1):
            if self.close_success_box:
                try:
                    closed = await asyncio.to_thread(
                        busy_check.close_success_box, self.success_box_keywords
                    )
                    if closed:
                        logger.info("closed %d success box window(s)", len(closed))
                        await asyncio.sleep(1)
                except Exception:
                    logger.exception("close success box failed; continuing launch")

            await progress(5, f"启动影刀机器人 {self.robot_uuid}")
            try:
                argv = [self.shadowbot_path] + [
                    a.format(robot_uuid=self.robot_uuid, **params) for a in self.args
                ]
                process = await asyncio.to_thread(busy_check.launch_robot, argv)
                pid = process.pid
            except OSError as exc:
                raise ExecutionError("EXECUTOR_START_FAILED", f"无法启动 ShadowBot: {exc}") from exc
            logger.info("yingdao process launched (pid=%s, uuid=%s)", pid, self.robot_uuid)

            if not can_verify:
                confirmed = True
                break
            if await busy_check.confirm_launch(
                self.log_dir, baseline_start_time, self.launch_verify_seconds
            ):
                confirmed = True
                break
            logger.warning("yingdao launch not confirmed on attempt %s/%s", attempt, attempts)
            busy_check.terminate_robot(pid)
            if attempt < attempts:
                await asyncio.sleep(1)

        if not confirmed:
            raise ExecutionError(
                "EXECUTOR_LAUNCH_FAILED",
                f"影刀启动未确认（重试 {attempts} 次），请检查该电脑影刀/日志配置",
            )

        # 5) wait for task end (default) or return right after confirmation.
        # Completion signal is the LOG's end marker when available: the
        # ShadowBot.exe process may exit immediately (launcher pattern) while
        # the robot engine keeps running, or stay open after the task ended.
        # Process exit is only trusted when no log can be read at all.
        if not self.wait_for_exit:
            return {
                "process_started": True,
                "pid": pid,
                "launch_confirmed": confirmed,
                "message": "审单已启动",
            }

        await progress(30, "影刀执行中")
        started = time.monotonic()
        deadline = started + timeout
        use_log = self.check_mode == "log"
        our_start_time = None
        if use_log:
            tail_now = busy_check.read_log_tail(self.log_dir)
            if tail_now is not None:
                our_start_time = busy_check.scan_markers(tail_now)[2]

        end_source = None
        while True:
            if cancel.is_set():
                busy_check.terminate_robot(pid)
                raise ExecutionError("EXECUTOR_CANCELLED", "任务已取消")
            if time.monotonic() > deadline:
                busy_check.terminate_robot(pid)
                raise ExecutionError("EXECUTOR_TIMEOUT", f"影刀执行超过 {timeout}s，已终止")

            if use_log:
                tail = busy_check.read_log_tail(self.log_dir)
                if tail is not None:
                    # log is the source of truth; ignore process lifetime
                    if busy_check.task_end_seen(self.log_dir, our_start_time):
                        end_source = "log"
                        break
                elif process.poll() is not None:
                    # no log file at all -> fall back to process exit
                    end_source = "process"
                    break
            elif process.poll() is not None:
                end_source = "process"
                break
            await asyncio.sleep(1)

        duration = round(time.monotonic() - started, 1)
        code = process.poll()
        result = {
            "process_started": True,
            "launch_confirmed": confirmed,
            "exit_code": code,
            "end_source": end_source,
            "duration": duration,
            "robot_uuid": self.robot_uuid,
        }
        if end_source == "process" and code not in (0, None):
            raise ExecutionError("EXECUTOR_FAILED", f"影刀进程退出码 {code}")
        result["message"] = "审单执行完成"
        await progress(100, "影刀执行完成")
        return result
