"""Main Agent run APIs (admin protected)."""

from fastapi import APIRouter, Depends, Request

from app.agent.service import AgentService
from app.auth.admin import require_admin
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
