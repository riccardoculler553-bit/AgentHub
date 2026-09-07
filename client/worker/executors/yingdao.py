"""YingdaoExecutor: launch the local 影刀 (ShadowBot) RPA robot (PDF §42-§47).

The real command lives HERE and only here - the Agent only ever says
"yingdao.audit". Configuration comes from the worker's capabilities.json:

    {
      "name": "yingdao.audit",
      "version": "1.0",
      "executor_type": "yingdao",
      "robot_uuid": "xxxxxxxx",
      "shadowbot_path": "C:\\Program Files\\ShadowBot\\ShadowBot.exe",
      "args": ["shadowbot:Run?robot-uuid={robot_uuid}"]
    }

`args` is optional; the default invokes the ShadowBot URL protocol handler.
Completion is judged by process exit code + duration (recorded in the result);
returncode alone is not treated as business success (PDF §45).
"""

import asyncio
import logging
import time

from worker.executor import ExecutionError, Executor

logger = logging.getLogger(__name__)

DEFAULT_ARGS = ["shadowbot:Run?robot-uuid={robot_uuid}"]


class YingdaoExecutor(Executor):
    name = "yingdao"

    def __init__(self) -> None:
        self.robot_uuid: str | None = None
        self.shadowbot_path: str | None = None
        self.args: list[str] = list(DEFAULT_ARGS)
        self.default_timeout: int = 1800

    def configure(self, config: dict) -> None:
        self.robot_uuid = config.get("robot_uuid") or None
        self.shadowbot_path = config.get("shadowbot_path") or None
        args = config.get("args")
        if isinstance(args, list) and args:
            self.args = [str(a) for a in args]
        self.default_timeout = int(config.get("timeout") or self.default_timeout)

    def validate(self, params: dict) -> None:
        if not self.robot_uuid:
            raise ValueError("yingdao.audit is not configured: robot_uuid missing in capabilities.json")
        if not self.shadowbot_path:
            raise ValueError("yingdao.audit is not configured: shadowbot_path missing in capabilities.json")

    async def execute(self, params, config, progress, cancel) -> dict:
        self.validate(params)
        argv = [self.shadowbot_path] + [
            a.format(robot_uuid=self.robot_uuid, **params) for a in self.args
        ]
        timeout = int(config.get("timeout") or self.default_timeout)
        await progress(5, f"启动影刀机器人 {self.robot_uuid}")

        started = time.monotonic()
        try:
            proc = await asyncio.create_subprocess_exec(
                *argv,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
        except OSError as exc:
            raise ExecutionError("EXECUTOR_START_FAILED", f"无法启动 ShadowBot: {exc}") from exc

        logger.info("yingdao process started (pid=%s, uuid=%s)", proc.pid, self.robot_uuid)
        try:
            await asyncio.wait_for(self._wait_cancelable(proc, cancel), timeout=timeout)
        except asyncio.TimeoutError:
            await self._terminate(proc)
            raise ExecutionError(
                "EXECUTOR_TIMEOUT", f"影刀执行超过 {timeout}s，已终止"
            ) from None
        if cancel.is_set():
            await self._terminate(proc)
            raise ExecutionError("EXECUTOR_CANCELLED", "任务已取消")

        duration = round(time.monotonic() - started, 1)
        code = await proc.wait() if proc.returncode is None else proc.returncode
        result = {
            "process_started": True,
            "exit_code": code,
            "duration": duration,
            "robot_uuid": self.robot_uuid,
        }
        if code != 0:
            raise ExecutionError("EXECUTOR_FAILED", f"影刀进程退出码 {code}")
        result["message"] = "审单执行完成"
        await progress(100, "影刀执行完成")
        return result

    # ------------------------------------------------------------------ helpers

    @staticmethod
    async def _wait_cancelable(proc: asyncio.subprocess.Process, cancel: asyncio.Event) -> None:
        while proc.returncode is None:
            if cancel.is_set():
                return
            await asyncio.sleep(0.5)

    @staticmethod
    async def _terminate(proc: asyncio.subprocess.Process) -> None:
        if proc.returncode is not None:
            return
        try:
            proc.terminate()
        except ProcessLookupError:
            return
        try:
            await asyncio.wait_for(proc.wait(), timeout=10)
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
