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
from app.core.config import settings
from app.db.database import Base, SessionLocal, engine
from app.db.models import User  # noqa: F401 - ensure models are registered
from app.command.db_models import Command  # noqa: F401 - AgentHub tables
from app.capability.db_models import DeviceCapability  # noqa: F401
from app.task.db_models import Task, TaskAttempt, TaskEvent, TaskStep  # noqa: F401
from app.command.service import CommandService
from app.task.monitor import TaskMonitor
from app.websocket.heartbeat import HeartbeatMonitor
from app.websocket.hub import ConnectionHub

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
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
        app.state.task_monitor_task = asyncio.create_task(task_monitor.run())
        logger.info("%s started on %s:%s", settings.app_name, settings.host, settings.port)
        yield
        for task in (app.state.monitor_task, app.state.task_monitor_task):
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
