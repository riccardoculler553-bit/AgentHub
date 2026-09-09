"""Workflow Pydantic contracts (V1.3 §8/§47/§74/§115/§191).

Definition = business process template (immutable per (name, version));
Run/StepRun shapes are API output models. Template syntax lives in step
params and is resolved by workflow.resolver at READY time (§80).
"""

import re
from typing import Literal

from pydantic import BaseModel, Field

# step name is also the context key ("{{ steps.<name>.result.* }}"), so it
# must be a safe identifier (V1.3 §38: explicit path resolution only).
NAME_PATTERN = r"^[a-z][a-z0-9_]{0,63}$"
VERSION_PATTERN = r"^\d+\.\d+\.\d+$"


class RetryPolicyIn(BaseModel):
    """Step retry policy (V1.3 §47). retry_on empty = retry any error code."""

    max_attempts: int = Field(default=1, ge=1, le=10)
    retry_on: list[str] = Field(default_factory=list)


class WorkflowStepDefinition(BaseModel):
    name: str = Field(pattern=NAME_PATTERN, description="步骤名，也是上下文引用键")
    command: str = Field(min_length=1, max_length=128, description="命令注册表中的命令名")
    params: dict = Field(default_factory=dict, description="支持 {{ variables.x }} / {{ steps.<name>.result.x }} 模板")
    device_id: str | None = Field(default=None, max_length=36, description="固定设备；为空则由引擎选择有能力的在线设备")
    # V1.4 §24: set -> capability step (command doubles as capability_name,
    # params holds capability_params). None keeps the V1.3 command step.
    capability_version: str | None = Field(
        default=None, max_length=32, pattern=VERSION_PATTERN,
        description="设置后该步骤为 Capability 步骤（command 即 capability 名称，固定执行版本）；为空表示 V1.3 命令步骤",
    )
    on_failure: Literal["stop", "retry"] = "stop"
    retry_policy: RetryPolicyIn = Field(default_factory=RetryPolicyIn)
    enabled: bool = True


class WorkflowDefinitionIn(BaseModel):
    name: str = Field(pattern=NAME_PATTERN, max_length=64)
    version: str = Field(pattern=VERSION_PATTERN, max_length=32)
    description: str = Field(default="", max_length=500)
    active_singleton: bool = Field(default=False, description="已有进行中的 Run 时禁止再次启动 (§138)")
    risk_level: Literal["READ", "WRITE", "ACTION"] = "ACTION"
    requires_confirmation: bool = Field(default=False, description="Agent run_workflow 是否需要用户确认 (§74)")
    enabled: bool = Field(default=False, description="false=DRAFT；只有 ENABLED 才能运行 (§108)")
    steps: list[WorkflowStepDefinition] = Field(min_length=1)


class WorkflowStepOut(BaseModel):
    step_id: str
    name: str
    order_no: int
    command: str
    params: dict
    device_id: str | None = None
    on_failure: str
    retry_policy: dict
    enabled: bool


class WorkflowOut(BaseModel):
    workflow_id: str
    name: str
    version: str
    description: str
    status: str
    active_singleton: bool
    risk_level: str
    requires_confirmation: bool
    created_at: str
    steps: list[WorkflowStepOut] = Field(default_factory=list)


class WorkflowStepRunOut(BaseModel):
    step_run_id: str
    name: str
    order_no: int
    command: str
    capability_version: str | None = None
    status: str
    task_id: str | None
    retry_count: int
    result: dict | None = None
    error_code: str | None = None
    error_message: str | None = None
    started_at: str | None = None
    finished_at: str | None = None


class WorkflowRunOut(BaseModel):
    run_id: str
    workflow_id: str
    workflow_name: str
    workflow_version: str
    status: str
    trigger_type: str
    created_by: str
    current_step: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    context: dict = Field(default_factory=dict)
    steps: list[WorkflowStepRunOut] = Field(default_factory=list)


class RunWorkflowIn(BaseModel):
    version: str | None = Field(default=None, max_length=32, description="不指定则使用该 Workflow 的 active 版本")
    variables: dict = Field(default_factory=dict)
