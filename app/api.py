"""HTTP layer: routes, error-to-HTTP mapping and lifespan wiring."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.errors import TaskOrchestratorError
from app.models import (
    MessageResponse,
    SubmitTaskRequest,
    SubmitTaskResponse,
    TaskView,
)
from app.service import TaskService
from app.store import TaskRecord

logger = logging.getLogger("app.api")


def build_task_view(record: TaskRecord) -> TaskView:
    return TaskView(
        id=record.id,
        client_token=record.client_token,
        status=record.status,
        spec=record.spec,
        cancel_requested=record.cancel_requested,
        result=record.result,
        error=record.error,
        created_at=record.created_at,
        started_at=record.started_at,
        finished_at=record.finished_at,
    )


def create_app(service: TaskService | None = None) -> FastAPI:
    if service is None:
        service = TaskService(
            max_workers=int(os.environ.get("TASK_MAX_WORKERS", "2")),
            max_queue_size=int(os.environ.get("TASK_MAX_QUEUE", "100")),
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        service.start()
        logger.info(
            "service ready workers=%d queue_capacity=%d",
            service.max_workers, service.max_queue_size,
        )
        try:
            yield
        finally:
            service.shutdown()

    app = FastAPI(
        title="Task Orchestrator",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.service = service

    @app.exception_handler(TaskOrchestratorError)
    async def handle_orchestrator_error(_request, exc: TaskOrchestratorError):
        return JSONResponse(
            status_code=exc.http_status,
            content={"error": {"code": exc.code, "message": exc.message}},
        )

    @app.get("/healthz", response_model=MessageResponse, tags=["system"])
    async def healthz():
        return MessageResponse(message="ok")

    @app.get("/stats", tags=["system"])
    async def stats():
        return {
            "workers": service.max_workers,
            "queue_capacity": service.max_queue_size,
            **service.stats(),
        }

    @app.post(
        "/tasks",
        response_model=SubmitTaskResponse,
        status_code=201,
        tags=["tasks"],
    )
    async def submit_task(request: SubmitTaskRequest):
        return service.submit(request)

    @app.get("/tasks/{task_id}", response_model=TaskView, tags=["tasks"])
    async def get_task(task_id: str):
        view = build_task_view(service.get(task_id))
        view.position_in_queue = service.store.queue_position(task_id)
        return view

    @app.post(
        "/tasks/{task_id}/cancel",
        response_model=TaskView,
        tags=["tasks"],
    )
    async def cancel_task(task_id: str):
        view = build_task_view(service.cancel(task_id))
        view.position_in_queue = service.store.queue_position(task_id)
        return view

    return app
