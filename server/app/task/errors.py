"""Task domain errors (HTTP-mappable, structured codes for the Main Agent)."""

from app.core.exceptions import DeviceLinkError


class TaskError(DeviceLinkError):
    status_code = 500
    code = "task_error"


class TaskNotFound(TaskError):
    status_code = 404
    code = "task_not_found"

    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        super().__init__(f"task not found: {task_id}")


class InvalidTaskState(TaskError):
    status_code = 409
    code = "invalid_task_state"

    def __init__(self, task_id: str, current: str, action: str) -> None:
        self.task_id = task_id
        self.current = current
        super().__init__(f"cannot {action} task {task_id} in state {current}")


class TaskValidationFailed(TaskError):
    status_code = 422
    code = "task_validation_failed"

    def __init__(self, problems: list[str]) -> None:
        self.problems = problems
        super().__init__("; ".join(problems))
