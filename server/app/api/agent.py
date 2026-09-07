"""Main Agent run APIs (admin protected)."""

from fastapi import APIRouter, Depends, HTTPException, Request

from app.agent.mvp.schemas import AgentMessageIn, AgentMessageOut, AgentRunRecordOut
from app.agent.mvp.service import MvpAgentService
from app.agent.runs import AgentRunService
from app.agent.service import AgentService
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
    service = AgentService(request.app.state.hub)
    result = await service.run(payload.request)
    return schemas.AgentRunOut(**result)


@router.post("/message", response_model=AgentMessageOut)
async def agent_message(payload: AgentMessageIn, request: Request):
    """MVP entry (PDF §94): one user message -> one AgentRun.

    Returns immediately; the graph runs in the background and replies are
    recorded on the AgentRun (and pushed to DingTalk when configured).
    Poll GET /api/agent/runs/{run_id} for ack/final replies."""
    service = getattr(request.app.state, "mvp_agent", None)
    if service is None:
        service = MvpAgentService(request.app.state.hub)
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
        return [
            AgentRunRecordOut(
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
                created_at=r.created_at,
                finished_at=r.finished_at,
            )
            for r in runs
        ]


@router.get("/runs/{run_id}", response_model=AgentRunRecordOut)
def get_agent_run(run_id: str):
    with SessionLocal() as db:
        try:
            r = AgentRunService(db).get(run_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="run not found")
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
            created_at=r.created_at,
            finished_at=r.finished_at,
        )
