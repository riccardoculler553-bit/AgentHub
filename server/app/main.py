"""DeviceLink server application factory and entrypoint."""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import FileResponse
from pathlib import Path

from app.api import devices, registration
from app.api import websocket as ws_api
from app.api import tasks as tasks_api
from app.api import agent as agent_api
from app.api import workflows as workflows_api
from app.api import capability as capability_api
from app.api import artifact as artifact_api
from app.command.db_models import Command  # noqa: F401 - AgentHub tables
from app.agent.db_models import AgentRun  # noqa: F401 - MVP agent_runs table
from app.command.service import CommandService
from app.integrations.dingtalk.client import start_dingtalk_bot
from app.integrations.dingtalk.sender import DingTalkSender
from app.agent.mvp.service import MvpAgentService
from app.core.config import settings
from app.db.database import Base, SessionLocal, engine
from app.db.models import User  # noqa: F401 - ensure models are registered
from app.capability.db_models import DeviceCapability  # noqa: F401
from app.capability_runtime.db_models import (  # noqa: F401 - V1.4 Capability Runtime
    AutomationCapability,
    CapabilityPackage,
    CapabilityVersion,
    WorkerCapability,
)
from app.artifact.db_models import Artifact  # noqa: F401 - V1.4 Artifact plane
from app.task.db_models import Task, TaskAttempt, TaskEvent, TaskStep  # noqa: F401
from app.task.events import subscribe_task_terminal
from app.task.monitor import TaskMonitor
from app.websocket.heartbeat import HeartbeatMonitor
from app.websocket.hub import ConnectionHub
from app.workflow.db_models import (  # noqa: F401 - V1.3 workflow tables
    Workflow,
    WorkflowEvent,
    WorkflowRun,
    WorkflowStep,
    WorkflowStepRun,
    ensure_capability_columns,
    ensure_task_source_columns,
)
from app.workflow.monitor import WorkflowMonitor
from app.workflow.runtime import workflow_runtime

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "server.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).resolve().parent / "static"


def ensure_default_user() -> None:
    from app.api.registration import get_or_create_default_user

    with SessionLocal() as db:
        get_or_create_default_user(db)
        db.commit()


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        Base.metadata.create_all(bind=engine)
        ensure_default_user()
        with SessionLocal() as db:
            CommandService(db).ensure_seed_commands()
            db.commit()
        monitor = HeartbeatMonitor(app.state.hub)
        app.state.monitor_task = asyncio.create_task(monitor.run())
        task_monitor = TaskMonitor(app.state.hub)
        task_monitor.recover_stuck_dispatching()  # V1.1 §42: restart recovery
        app.state.task_monitor_task = asyncio.create_task(task_monitor.run())
        # V1.3 §54/§121: workflow recovery + task-terminal advancement wiring.
        ensure_task_source_columns(engine)
        # V1.4 §59: minimal-invasive column ensure for pre-V1.4 tables.
        ensure_capability_columns(engine)
        workflow_monitor = WorkflowMonitor(app.state.hub)
        workflow_monitor.recover_from_restart()
        app.state.workflow_monitor_task = asyncio.create_task(workflow_monitor.run())
        workflow_runtime.bind(app.state.hub, asyncio.get_running_loop())
        subscribe_task_terminal(workflow_runtime.on_task_terminal)
        # V1.2 §97: AGENT_MODE switches the message-facing agent. Both expose
        # the same handle_message intake the DingTalk client calls.
        if settings.agent_mode == "tool_agent":
            from app.agent.service import AgentService
            from app.agent.notify import install_agent_task_notifier

            app.state.agent_service = AgentService(app.state.hub, sender=DingTalkSender())
            # Phase 3: task-terminal proactive DingTalk notification (fires
            # only when the agent run already gave up waiting).
            install_agent_task_notifier(DingTalkSender())
            logger.info("agent mode: tool_agent (V1.2 LangGraph tool-using agent)")
        else:
            app.state.agent_service = MvpAgentService(app.state.hub, sender=DingTalkSender())
            logger.info("agent mode: mvp")
        app.state.dingtalk_task = await start_dingtalk_bot(app.state.hub, app.state.agent_service)
        logger.info("%s started on %s:%s", settings.app_name, settings.host, settings.port)
        yield
        background = [app.state.monitor_task, app.state.task_monitor_task]
        workflow_monitor_task = getattr(app.state, "workflow_monitor_task", None)
        if workflow_monitor_task is not None:
            background.append(workflow_monitor_task)
        dingtalk_task = getattr(app.state, "dingtalk_task", None)
        if dingtalk_task is not None:
            background.append(dingtalk_task)
        for task in background:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        logger.info("server stopped")

    app = FastAPI(title=settings.app_name, version="1.0.0", lifespan=lifespan)
    app.state.hub = ConnectionHub()

    app.include_router(registration.router)
    app.include_router(devices.router)
    app.include_router(ws_api.router)
    app.include_router(tasks_api.router)
    app.include_router(agent_api.router)
    app.include_router(workflows_api.router)
    # V1.4 Capability Runtime + Artifact plane
    app.include_router(capability_api.router)
    app.include_router(capability_api.worker_router)
    app.include_router(artifact_api.upload_router)
    app.include_router(artifact_api.download_router)
    app.include_router(artifact_api.admin_router)

    @app.get("/", include_in_schema=False)
    async def dashboard():
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/api/health", tags=["meta"])
    async def health():
        return {"status": "ok", "app": settings.app_name}

    return app


app = create_app()
