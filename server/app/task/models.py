"""Pydantic schemas for AgentHub task/command/capability/agent APIs."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class StepIn(BaseModel):
    command: str = Field(min_length=1, max_length=128)
    params: dict = Field(default_factory=dict)


class InputArtifactIn(BaseModel):
    """V1.5 §15: a Task stores artifact REFERENCES, never local paths."""

    artifact_id: str = Field(min_length=1, max_length=64)
    # role = the manifest input name the artifact feeds (e.g. data_dir)
    role: str = Field(default="input", max_length=128)


class TaskCreateIn(BaseModel):
    name: str = Field(default="", max_length=200)
    target_device_id: str | None = Field(default=None, max_length=36)
    steps: list[StepIn] = Field(min_length=1)
    # V1.3 task provenance (§32/§33): API | AGENT | WORKFLOW | MANUAL
    source_type: str | None = Field(default=None, max_length=32)
    workflow_run_id: str | None = Field(default=None, max_length=64)
    workflow_step_run_id: str | None = Field(default=None, max_length=64)
    # V1.4 §32/§73: LEGACY_COMMAND (default) | CAPABILITY
    execution_type: str | None = Field(default=None, max_length=16)
    # CAPABILITY tasks: pinned version (None = capability current_version)
    capability_version: str | None = Field(default=None, max_length=32)
    # V1.5 §15: CAPABILITY task inputs (artifact references)
    input_artifacts: list[InputArtifactIn] = Field(default_factory=list)
    # V1.5: per-task timeout override in seconds (CAPABILITY tasks; None =
    # CAPABILITY_DEFAULT_TIMEOUT). Big jobs set e.g. 7200.
    timeout_seconds: int | None = Field(default=None, ge=60, le=86400)


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
    # Phase 8: latest monotonic progress snapshot (never stale/reordered)
    progress: dict | None = None


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
    timeout_seconds: int | None = None
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
