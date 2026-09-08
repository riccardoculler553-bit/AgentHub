"""Pydantic schemas for AgentHub task/command/capability/agent APIs."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class StepIn(BaseModel):
    command: str = Field(min_length=1, max_length=128)
    params: dict = Field(default_factory=dict)


class TaskCreateIn(BaseModel):
    name: str = Field(default="", max_length=200)
    target_device_id: str | None = Field(default=None, max_length=36)
    steps: list[StepIn] = Field(min_length=1)
    # V1.3 task provenance (§32/§33): API | AGENT | WORKFLOW | MANUAL
    source_type: str | None = Field(default=None, max_length=32)
    workflow_run_id: str | None = Field(default=None, max_length=64)
    workflow_step_run_id: str | None = Field(default=None, max_length=64)


class StepOut(BaseModel):
    step_id: str
    order_no: int
    command: str
    params: dict
    status: str
    started_at: datetime | None = None
    finished_at: datetime | None = None


class AttemptOut(BaseModel):
    attempt_id: str
    step_id: str
    device_id: str | None
    attempt_no: int
    status: str
    error_code: str | None
    error_message: str | None
    created_at: datetime
    finished_at: datetime | None


class EventOut(BaseModel):
    id: int
    task_id: str
    step_id: str | None
    attempt_id: str | None
    event_type: str
    payload: dict
    created_at: datetime


class TaskOut(BaseModel):
    task_id: str
    name: str
    target_device_id: str | None
    status: str
    priority: int
    max_attempts: int
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    timeout_at: datetime | None


class TaskDetailOut(TaskOut):
    steps: list[StepOut]
    attempts: list[AttemptOut]
    events: list[EventOut]


class CommandOut(BaseModel):
    command_name: str
    version: str
    description: str
    executor_type: str
    executor_config: dict
    params_schema: dict
    timeout: int
    enabled: bool


class CapabilityOut(BaseModel):
    device_id: str
    capabilities: list[dict]


class AgentRunIn(BaseModel):
    request: str = Field(min_length=1, max_length=2000)


class AgentRunOut(BaseModel):
    request: str
    execution_plan: dict | None = None
    task_id: str | None = None
    task_status: str | None = None
    task_result: dict | None = None
    error: dict | None = None
    decision: str | None = None
    message: str = ""
    retry_count: int = 0
    replan_count: int = 0
