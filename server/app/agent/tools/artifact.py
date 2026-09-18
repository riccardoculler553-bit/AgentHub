"""Artifact agent tools (V1.5): 结果产物落盘到服务器本地目录。

save_artifact: run_capability 成功后产物停在 Artifact 存储（只有 artid）；
用户在对话里给出目标目录，agent 调用本工具把文件写到服务器本地 —— server
直接读自己的存储，不经钉钉/浏览器中转（与数据源注册的 register-local 对称）。
"""

from typing import Any
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.agent.tools.base import AgentTool, RiskLevel, ToolResult
from app.agent.tools.registry import ToolRegistry
from app.artifact.service import ArtifactNotFound, ArtifactService, safe_artifact_name


class SaveArtifactArgs(BaseModel):
    model_config = {"extra": "forbid"}

    artifact_id: str = Field(min_length=1, max_length=64, description="要保存的 artifact_id")
    dir: str = Field(
        min_length=1, max_length=500,
        description="目标目录（服务器本地绝对路径，如 D:\\结果）",
    )


def register_artifact_tools(registry: ToolRegistry, hub: Any = None) -> None:
    async def save_artifact(db: Session, args: dict) -> ToolResult:
        out_dir = Path(args["dir"].strip().strip('"'))
        if not out_dir.is_absolute():
            return ToolResult.fail("INVALID_ARGS", "dir 必须是服务器本地绝对路径，如 D:\\结果")
        try:
            row, content = ArtifactService(db).read_artifact_bytes(args["artifact_id"])
        except ArtifactNotFound as exc:
            return ToolResult.fail("ARTIFACT_NOT_FOUND", str(exc))

        out_dir.mkdir(parents=True, exist_ok=True)
        target = out_dir / safe_artifact_name(row.name)
        if target.exists():  # 同名不覆盖：加时间戳后缀
            target = out_dir / f"{Path(safe_artifact_name(row.name)).stem}_{datetime.now().strftime('%H%M%S')}{target.suffix}"
        target.write_bytes(content)
        return ToolResult.ok({
            "artifact_id": row.artifact_id,
            "saved_to": str(target),
            "size": len(content),
        })

    registry.register(
        AgentTool(
            name="save_artifact",
            description=(
                "把一个结果 Artifact（产物文件）保存到服务器本地目录。"
                "run_capability 成功后其 result.artifacts 里有 artifact_id，"
                "用户给了保存路径就调用本工具落盘。"
            ),
            handler=save_artifact,
            args_schema=SaveArtifactArgs,
            risk_level=RiskLevel.WRITE,
            requires_confirmation=False,
            max_calls=4,
        )
    )
