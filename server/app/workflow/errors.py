"""Workflow domain errors (V1.3 §82: canonical workflow error codes).

HTTP-mappable like TaskError so Admin API / Agent Tools can surface the
canonical code (WORKFLOW_*) instead of a wrapped generic error.
"""

from app.core.exceptions import DeviceLinkError


class WorkflowError(DeviceLinkError):
    status_code = 500
    code = "workflow_error"


class WorkflowNotFound(WorkflowError):
    status_code = 404
    code = "workflow_not_found"

    def __init__(self, workflow: str) -> None:
        self.workflow = workflow
        super().__init__(f"workflow not found: {workflow}")


class WorkflowVersionNotFound(WorkflowError):
    status_code = 404
    code = "workflow_version_not_found"

    def __init__(self, name: str, version: str) -> None:
        self.name = name
        self.version = version
        super().__init__(f"workflow version not found: {name}@{version}")


class WorkflowAlreadyExists(WorkflowError):
    status_code = 409
    code = "workflow_already_exists"

    def __init__(self, name: str, version: str) -> None:
        self.name = name
        self.version = version
        super().__init__(f"workflow {name}@{version} already exists")


class WorkflowDisabled(WorkflowError):
    status_code = 409
    code = "workflow_disabled"

    def __init__(self, name: str, version: str) -> None:
        self.name = name
        self.version = version
        super().__init__(f"workflow {name}@{version} is not ENABLED")


class WorkflowInvalid(WorkflowError):
    status_code = 422
    code = "workflow_invalid"

    def __init__(self, problems: list[str]) -> None:
        self.problems = problems
        super().__init__("; ".join(problems))


class WorkflowRunNotFound(WorkflowError):
    status_code = 404
    code = "workflow_run_not_found"

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        super().__init__(f"workflow run not found: {run_id}")


class InvalidWorkflowState(WorkflowError):
    status_code = 409
    code = "invalid_workflow_state"

    def __init__(self, run_id: str, current: str, action: str) -> None:
        self.run_id = run_id
        self.current = current
        super().__init__(f"cannot {action} workflow run {run_id} in state {current}")


class WorkflowAlreadyRunning(WorkflowError):
    status_code = 409
    code = "workflow_already_running"

    def __init__(self, name: str, run_id: str) -> None:
        self.name = name
        self.run_id = run_id
        super().__init__(f"workflow {name} already has an active run: {run_id}")


class WorkflowParamResolutionFailed(WorkflowError):
    status_code = 422
    code = "workflow_param_resolution_failed"

    def __init__(self, template: str, reason: str) -> None:
        self.template = template
        self.reason = reason
        super().__init__(f"cannot resolve '{template}': {reason}")
