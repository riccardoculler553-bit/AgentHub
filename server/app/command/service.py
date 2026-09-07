"""CommandService: Command Registry CRUD + seed + param validation.

The Registry defines WHAT the system may execute. The actual implementation
lives in Worker-side Executors - never here.
"""

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.command.db_models import Command
from app.core.config import settings

# Simple scalar types accepted in params_schema
_SCALARS = {
    "string": str,
    "number": (int, float),
    "integer": int,
    "boolean": bool,
    "object": dict,
    "array": list,
}


class CommandError(Exception):
    status_code = 500
    code = "command_error"

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.code


class CommandNotFound(CommandError):
    status_code = 404
    code = "command_not_found"

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"command not found: {name}")


class CommandDisabled(CommandError):
    status_code = 409
    code = "command_disabled"

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"command is disabled: {name}")


class InvalidParams(CommandError):
    status_code = 422
    code = "invalid_params"

    def __init__(self, command: str, problems: list[str]) -> None:
        self.command = command
        self.problems = problems
        super().__init__(f"invalid params for {command}: {'; '.join(problems)}")


# Seed commands: prove the chain first (echo), then whitelisted python scripts.
SEED_COMMANDS = [
    {
        "command_name": "echo",
        "version": "1.0",
        "description": "回显测试命令：原样返回 message，用于验证 Agent->Task->Dispatcher->Worker 全链路",
        "executor_type": "echo",
        "executor_config": {},
        "params_schema": {"message": "string"},
        "timeout": 30,
    },
    {
        "command_name": "python.demo",
        "version": "1.0",
        "description": "在目标电脑上运行预注册的演示 Python 脚本",
        "executor_type": "python",
        "executor_config": {"script": "demo_hello.py"},
        "params_schema": {},
        "timeout": 120,
    },
]


class CommandService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def ensure_seed_commands(self) -> None:
        for spec in SEED_COMMANDS:
            exists = self.db.scalars(
                select(Command).where(Command.command_name == spec["command_name"])
            ).first()
            if exists is None:
                self.db.add(Command(**spec, enabled=True))
        self.db.commit()

    def list_commands(self, include_disabled: bool = True) -> list[Command]:
        stmt = select(Command).order_by(Command.command_name)
        if not include_disabled:
            stmt = stmt.where(Command.enabled.is_(True))
        return list(self.db.scalars(stmt))

    def get_command(self, name: str) -> Command:
        row = self.db.scalars(select(Command).where(Command.command_name == name)).first()
        if row is None:
            raise CommandNotFound(name)
        return row

    def require_executable(self, name: str) -> Command:
        """Existence + enabled check (Validator step 1)."""
        row = self.get_command(name)
        if not row.enabled:
            raise CommandDisabled(name)
        return row

    def validate_params(self, command: Command, params: dict) -> None:
        """Strict validation: every schema key must be present with the right
        type, and no extra keys are allowed (prevents parameter smuggling)."""
        schema: dict = command.params_schema or {}
        params = params or {}
        problems: list[str] = []
        for key, type_name in schema.items():
            if key not in params:
                problems.append(f"missing required param: {key}")
                continue
            expected = _SCALARS.get(str(type_name).lower())
            if expected is not None and not isinstance(params[key], expected):
                problems.append(f"param {key} must be {type_name}")
            elif isinstance(params[key], bool) and str(type_name).lower() == "integer":
                problems.append(f"param {key} must be {type_name}")
        allowed = set(schema.keys())
        for key in params:
            if key not in allowed:
                problems.append(f"unknown param: {key}")
        if problems:
            raise InvalidParams(command.command_name, problems)

    def task_timeout(self, command: Command) -> int:
        return int(command.timeout or settings.task_offline_max_wait)
