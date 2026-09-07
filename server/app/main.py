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
from app.task.db_models import Task, TaskAttempt, TaskEvent, TaskStep  # noqa: F401
from app.task.monitor import TaskMonitor
from app.websocket.heartbeat import HeartbeatMonitor
from app.websocket.hub import ConnectionHub

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
        # MVP: DingTalk -> Main Agent -> yingdao.audit (PDF §62-§64)
        app.state.mvp_agent = MvpAgentService(app.state.hub, sender=DingTalkSender())
        app.state.dingtalk_task = await start_dingtalk_bot(app.state.hub, app.state.mvp_agent)
        logger.info("%s started on %s:%s", settings.app_name, settings.host, settings.port)
        yield
        background = [app.state.monitor_task, app.state.task_monitor_task]
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

    @app.get("/", include_in_schema=False)
    async def dashboard():
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/api/health", tags=["meta"])
    async def health():
        return {"status": "ok", "app": settings.app_name}

    return app


app = create_app()
