"""CapabilityResult: normalized result contract (V1.4 §44/§45).

Success:
    {"success": true, "data": {}, "message": "执行成功", "artifacts": [], "metrics": {}}
Failure:
    {"success": false, "error_code": "CAPABILITY_EXECUTION_FAILED",
     "message": "订单下载失败", "retryable": true, "artifacts": []}

`artifact_files` is worker-internal: (name, path) pairs the TaskManager must
upload before reporting; they are replaced by artifact references
(artifact_id/name/type) in the reported payload (§45: Agent only sees IDs).
"""

from pathlib import Path
from typing import Any


class CapabilityResult:
    def __init__(
        self,
        success: bool,
        data: dict | None = None,
        message: str = "",
        error_code: str | None = None,
        retryable: bool = False,
        artifacts: list[dict] | None = None,
        metrics: dict | None = None,
        artifact_files: list[tuple[str, Path]] | None = None,
    ) -> None:
        self.success = success
        self.data = data or {}
        self.message = message
        self.error_code = error_code
        self.retryable = retryable
        self.artifacts = artifacts or []
        self.metrics = metrics or {}
        self.artifact_files = artifact_files or []

    # ---------------------------------------------------------------- factories

    @classmethod
    def ok(cls, data: dict | None = None, message: str = "执行成功",
           artifact_files: list[tuple[str, Path]] | None = None,
           metrics: dict | None = None) -> "CapabilityResult":
        return cls(True, data=data, message=message, artifact_files=artifact_files, metrics=metrics)

    @classmethod
    def fail(cls, error_code: str, message: str, retryable: bool = False) -> "CapabilityResult":
        return cls(False, error_code=error_code, message=message, retryable=retryable)

    # ------------------------------------------------------------------ payload

    def to_payload(self) -> dict[str, Any]:
        if self.success:
            return {
                "success": True,
                "data": self.data,
                "message": self.message,
                "artifacts": self.artifacts,
                "metrics": self.metrics,
            }
        return {
            "success": False,
            "error_code": self.error_code or "CAPABILITY_EXECUTION_FAILED",
            "message": self.message,
            "retryable": self.retryable,
            "artifacts": self.artifacts,
        }
