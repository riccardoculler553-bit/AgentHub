"""Main Agent run APIs (admin protected)."""

from fastapi import APIRouter, Depends, HTTPException, Request

from app.agent.mvp.schemas import (
    AgentMessageIn,
    AgentMessageOut,
    AgentRunRecordOut,
    ResumeMessageIn,
)
from app.agent.mvp.service import MvpAgentService
from app.agent.runs import STATUS_WAITING_USER, AgentRunService
from app.agent.legacy.service import AgentService as LegacyAgentService
from app.auth.admin import require_admin
from app.db.database import SessionLocal
from app.task import models as schemas

router = APIRouter(prefix="/api/agent", tags=["agent"], dependencies=[Depends(require_admin)])


@router.post("/run", response_model=schemas.AgentRunOut)
async def run_agent(payload: schemas.AgentRunIn, request: Request):
    """Natural language -> plan -> task -> dispatch -> wait -> evaluate.

    Synchronous run: returns when the agent loop reaches a decision
    (success, retries exhausted, or replan budget spent).
    """
    service = LegacyAgentService(request.app.state.hub)
    result = await service.run(payload.request)
    return schemas.AgentRunOut(**result)


@router.post("/message", response_model=AgentMessageOut)
async def agent_message(payload: AgentMessageIn, request: Request):
    """One user message -> one AgentRun (PDF §94).

    Returns immediately; the active agent (AGENT_MODE: mvp | tool_agent) runs
    in the background and replies are recorded on the AgentRun (and pushed to
    DingTalk when configured). Poll GET /api/agent/runs/{run_id} for replies."""
    service = _message_agent(request)
    run_id = service.handle_message(
        text=payload.text,
        channel=payload.channel,
        message_id=payload.message_id,
        conversation_id=payload.conversation_id,
        sender_id=payload.sender_id,
        sender_name=payload.sender_name,
    )
    return AgentMessageOut(run_id=run_id)


@router.get("/runs", response_model=list[AgentRunRecordOut])
def list_agent_runs(limit: int = 50):
    with SessionLocal() as db:
        runs = AgentRunService(db).list_runs(limit)
        return [_run_out(r) for r in runs]


@router.get("/runs/{run_id}", response_model=AgentRunRecordOut)
def get_agent_run(run_id: str):
    with SessionLocal() as db:
        try:
            r = AgentRunService(db).get(run_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="run not found")
        return _run_out(r)


@router.post("/runs/{run_id}/message", response_model=AgentRunRecordOut)
async def resume_agent_run(run_id: str, payload: ResumeMessageIn, request: Request):
    """User follow-up on a WAITING_USER run (PDF §91/§133).

    The graph continues in the background on the restored AgentState (the row
    reopens to RUNNING once the resume takes over); poll
    GET /api/agent/runs/{run_id} for the final reply."""
    service = _resume_agent(request)
    with SessionLocal() as db:
        runs = AgentRunService(db)
        try:
            row = runs.get(run_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="run not found")
        if row.status != STATUS_WAITING_USER:
            raise HTTPException(
                status_code=409, detail=f"run is {row.status}, not WAITING_USER"
            )
        out = _run_out(row)
    service.schedule_resume(run_id, payload.text)
    return out


@router.post("/runs/{run_id}/cancel", response_model=AgentRunRecordOut)
def cancel_agent_run(run_id: str):
    """Stop an open AgentRun (V1.2 §90: business tasks are NOT auto-cancelled)."""
    with SessionLocal() as db:
        runs = AgentRunService(db)
        try:
            row = runs.cancel(run_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="run not found")
        return _run_out(row)


def _run_out(r) -> AgentRunRecordOut:
    return AgentRunRecordOut(
        run_id=r.run_id,
        channel=r.channel,
        conversation_id=r.conversation_id,
        sender_id=r.sender_id,
        sender_name=r.sender_name,
        message_id=r.message_id,
        input_text=r.input_text,
        status=r.status,
        task_id=r.task_id,
        ack_reply=r.ack_reply,
        final_reply=r.final_reply,
        error=r.error,
        tool_call_count=r.tool_call_count,
        created_at=r.created_at,
        finished_at=r.finished_at,
    )


def _message_agent(request: Request):
    """The active message-facing agent; a missing lifespan attribute (tests,
    older app instances) falls back to a locally built MVP service."""
    service = getattr(request.app.state, "agent_service", None)
    if service is None:
        service = MvpAgentService(request.app.state.hub)
    return service


def _resume_agent(request: Request):
    """An AgentService for resumes. In tool_agent mode this is the live one;
    mvp mode never parks runs, but a WAITING_USER row left over from an earlier
    tool_agent deployment still deserves a working resume path."""
    service = getattr(request.app.state, "agent_service", None)
    if service is not None and hasattr(service, "resume_run"):
        return service
    from app.agent.service import AgentService

    return AgentService(request.app.state.hub)
