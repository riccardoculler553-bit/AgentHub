"""ToolPolicy: the admission gate between the LLM and business services.

Every tool call - no exceptions - walks the chain (PDF §25/§85):

    exists -> enabled -> args valid -> permission -> confirmation -> limits -> execute

The LLM cannot bypass it: the execute_tool node is the only code path that
runs tools, and it always goes through ToolPolicy.execute().
"""

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from sqlalchemy.orm import Session

from app.agent.tools.base import ToolErrorCodes, ToolResult
from app.agent.tools.registry import ToolRegistry

logger = logging.getLogger(__name__)

# V1.2 hook reserved for a future PermissionService (PDF §59): receives
# (tool, args) and answers whether the caller may invoke it. None = allow-all.
PermissionFn = Callable[[object, dict], bool] | Callable[[object, dict], Awaitable[bool]]

# PDF §124: agent_tool_calls stores a bounded summary, never unbounded blobs.
_AUDIT_LIMIT = 4000

# Audit statuses (PDF §123). PENDING is reserved for a future async model.
_AUDIT_RUNNING = "RUNNING"
_AUDIT_SUCCESS = "SUCCESS"
_AUDIT_FAILED = "FAILED"
_AUDIT_REJECTED = "REJECTED"


def _dump_bounded(value: Any, limit: int = _AUDIT_LIMIT) -> str:
    import json

    try:
        text = json.dumps(value, ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        text = str(value)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _audit_start(run_id: str, call_id: str, tool_name: str, args: dict | None) -> int | None:
    """Insert the RUNNING audit row; returns its id (None on failure - audit
    is best-effort and must never break the tool call itself)."""
    from app.agent.db_models import AgentToolCall
    from app.db.database import SessionLocal

    try:
        with SessionLocal() as db:
            row = AgentToolCall(
                run_id=run_id,
                tool_call_id=call_id,
                tool_name=tool_name,
                arguments=_dump_bounded(args or {}),
                status=_AUDIT_RUNNING,
            )
            db.add(row)
            db.commit()
            return row.id
    except Exception:  # noqa: BLE001
        logger.warning("tool call audit (start) failed for %s %s", call_id, tool_name, exc_info=True)
        return None


def _audit_finish(
    row_id: int | None, *, status: str, result: ToolResult | None = None
) -> None:
    from app.agent.db_models import AgentToolCall
    from app.db.database import SessionLocal
    from app.db.models import utcnow

    if row_id is None:
        return
    try:
        with SessionLocal() as db:
            row = db.get(AgentToolCall, row_id)
            if row is not None and row.status == _AUDIT_RUNNING:
                row.status = status
                row.result = None if result is None else _dump_bounded(result.model_dump())
                row.error_code = result.error_code if result is not None else None
                row.finished_at = utcnow()
                db.commit()
    except Exception:  # noqa: BLE001
        logger.warning("tool call audit (finish) failed for row %s", row_id, exc_info=True)


def _audit_rejected(
    run_id: str, call_id: str, tool_name: str, args: dict | None, error_code: str
) -> None:
    """One-shot REJECTED row for calls the policy chain turns away."""
    from app.agent.db_models import AgentToolCall
    from app.db.database import SessionLocal
    from app.db.models import utcnow

    try:
        with SessionLocal() as db:
            db.add(
                AgentToolCall(
                    run_id=run_id,
                    tool_call_id=call_id,
                    tool_name=tool_name,
                    arguments=_dump_bounded(args or {}),
                    status=_AUDIT_REJECTED,
                    error_code=error_code,
                    finished_at=utcnow(),
                )
            )
            db.commit()
    except Exception:  # noqa: BLE001
        logger.warning("tool call audit (reject) failed for %s", call_id, exc_info=True)


class ToolPolicy:
    """Per-AgentRun policy instance: owns the call ledger and call-id sequence."""

    def __init__(
        self,
        registry: ToolRegistry,
        *,
        max_total_calls: int = 8,
        permission_fn: PermissionFn | None = None,
        used_total: int = 0,
        run_id: str | None = None,
    ) -> None:
        self.registry = registry
        self.max_total_calls = max_total_calls
        self.permission_fn = permission_fn
        # agent_tool_calls audit (PDF §122) is enabled only when the runner
        # hands over the run identity; bare policies (tests) skip it.
        self.run_id = run_id
        self._per_tool: dict[str, int] = {}
        # carried across resume (PDF §41 is per AgentRun): the global budget
        # a parked run already spent. Per-tool ledgers restart on resume -
        # a documented V1.2 limitation, the global cap is the safety-critical
        # one and state.tool_call_count keeps it coherent.
        self._total = max(0, used_total)
        self._seq = 0

    # ----------------------------------------------------------------- ids

    def next_call_id(self) -> str:
        """Sequential per-run tool_call_id (PDF §102: call_001, call_002...)."""
        self._seq += 1
        return f"call_{self._seq:03d}"

    @property
    def total_calls(self) -> int:
        return self._total

    def calls_of(self, tool_name: str) -> int:
        return self._per_tool.get(tool_name, 0)

    # ------------------------------------------------------------- chain

    async def execute(
        self,
        db: Session,
        *,
        tool_name: str,
        args: dict | None,
        confirmed: bool = False,
    ) -> ToolResult:
        """Admit (or reject) one tool call. Always returns a ToolResult and
        never raises - policy rejections are observations for the LLM (§37)."""
        call_id = self.next_call_id()

        def _reject(code: str, message: str, *, data: dict | None = None) -> ToolResult:
            if self.run_id:
                _audit_rejected(self.run_id, call_id, tool_name, args, code)
            return ToolResult.fail(
                code, message, tool_call_id=call_id, tool_name=tool_name, data=data
            )

        # 1. exists  2. enabled (unknown == not found for the LLM)
        tool = self.registry.get(tool_name)
        if tool is None or not tool.enabled:
            return _reject(
                ToolErrorCodes.TOOL_NOT_FOUND,
                f"tool '{tool_name}' does not exist or is not enabled",
            )

        # 3. args valid (strict schema; extra keys rejected)
        try:
            validated = tool.validate_args(args)
        except ValueError as exc:
            return _reject(ToolErrorCodes.INVALID_ARGS, str(exc))

        # 4. permission
        if self.permission_fn is not None:
            allowed = self.permission_fn(tool, validated)
            if hasattr(allowed, "__await__"):
                allowed = await allowed  # type: ignore[misc]
            if not allowed:
                return _reject(
                    ToolErrorCodes.PERMISSION_DENIED,
                    f"permission denied for tool '{tool_name}'",
                )

        # 5. confirmation (PDF §56-§57): WRITE/ACTION tools may require an
        # explicit user yes before execution; the graph turns this rejection
        # into ask_user + pending_confirmation and resumes with confirmed=True.
        if tool.requires_confirmation and not confirmed:
            return _reject(
                ToolErrorCodes.CONFIRMATION_REQUIRED,
                f"tool '{tool_name}' requires user confirmation",
                data={"pending_args": validated},
            )

        # 6. limits: per-tool then per-run (PDF §41/§87-§88)
        if self.calls_of(tool_name) >= tool.max_calls:
            return _reject(
                ToolErrorCodes.MAX_CALLS_EXCEEDED,
                f"tool '{tool_name}' reached its call limit ({tool.max_calls})",
            )
        if self._total >= self.max_total_calls:
            return _reject(
                ToolErrorCodes.MAX_CALLS_EXCEEDED,
                f"agent reached the tool call limit ({self.max_total_calls})",
            )

        # 7. execute (AgentTool.run converts handler errors to ToolResult)
        audit_row = _audit_start(self.run_id, call_id, tool_name, validated) if self.run_id else None
        result = await tool.run(db, validated)
        result.with_call(call_id, tool_name)
        self._per_tool[tool_name] = self.calls_of(tool_name) + 1
        self._total += 1
        if self.run_id:
            _audit_finish(
                audit_row,
                status=_AUDIT_SUCCESS if result.success else _AUDIT_FAILED,
                result=result,
            )
        logger.info(
            "tool call %s %s args=%s success=%s code=%s",
            call_id,
            tool_name,
            validated,
            result.success,
            result.error_code,
        )
        return result
