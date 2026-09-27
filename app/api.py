"""FastAPI 路由层：提交（幂等）、查询、取消。"""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.errors import (
    InvalidStateError,
    OrchestratorError,
    QueueFullError,
    TaskNotFoundError,
    TokenConflictError,
)
from app.models import Task
from app.service import TaskService


class SubmitRequest(BaseModel):
    token: str = Field(min_length=1, description="客户端幂等令牌")
    payload: str = Field(default="", description="任务内容，JSON 字符串")


def serialize(task: Task) -> dict:
    return {
        "id": task.id,
        "token": task.token,
        "status": task.status.value,
        "result": task.result,
        "error": task.error,
        "error_kind": task.error_kind,
        "created_at": task.created_at,
        "finished_at": task.finished_at,
    }


def _error(status_code: int, exc: OrchestratorError) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": exc.code, "message": str(exc)}},
    )


def create_app(service: TaskService) -> FastAPI:
    app = FastAPI(title="task-orchestrator")

    @app.exception_handler(TokenConflictError)
    async def _(req: Request, exc: TokenConflictError) -> JSONResponse:
        return _error(409, exc)

    @app.exception_handler(QueueFullError)
    async def _(req: Request, exc: QueueFullError) -> JSONResponse:
        return _error(429, exc)

    @app.exception_handler(TaskNotFoundError)
    async def _(req: Request, exc: TaskNotFoundError) -> JSONResponse:
        return _error(404, exc)

    @app.exception_handler(InvalidStateError)
    async def _(req: Request, exc: InvalidStateError) -> JSONResponse:
        return _error(409, exc)

    @app.post("/tasks", status_code=201)
    def submit(req: SubmitRequest) -> JSONResponse:
        task, created = service.submit(req.token, req.payload)
        return JSONResponse(
            status_code=201 if created else 200,
            content={**serialize(task), "deduplicated": not created},
        )

    @app.get("/tasks/{task_id}")
    def get(task_id: str) -> dict:
        return serialize(service.get(task_id))

    @app.post("/tasks/{task_id}/cancel")
    def cancel(task_id: str) -> dict:
        return serialize(service.cancel(task_id))

    return app
