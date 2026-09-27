"""Domain types: task status machine, API payloads and read views."""

from __future__ import annotations

import enum
from typing import Any

from pydantic import BaseModel, Field


class TaskStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        return self in TERMINAL_STATES


TERMINAL_STATES = frozenset(
    {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELLED}
)

# Allowed task state transitions. Any edge not listed here is rejected.
ALLOWED_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset(
        {TaskStatus.RUNNING, TaskStatus.CANCELLED}
    ),
    TaskStatus.RUNNING: frozenset(
        {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELLED}
    ),
    TaskStatus.SUCCEEDED: frozenset(),
    TaskStatus.FAILED: frozenset(),
    TaskStatus.CANCELLED: frozenset(),
}


class FailureCategory(str, enum.Enum):
    EXECUTION_ERROR = "execution_error"  # task body raised an exception
    TASK_TIMEOUT = "task_timeout"        # task exceeded its timeout
    CANCELLED = "cancelled"              # cancellation won the race
    INTERNAL_ERROR = "internal_error"    # orchestrator-side failure


class TaskSpec(BaseModel):
    """Declarative description of a demo task.

    The demo runner understands two optional payload knobs:
      * ``sleep``: busy duration in seconds (supports cancellation);
      * ``raise``: when true the task raises ``RuntimeError(message)``.
    """

    payload: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: float | None = None


class TaskErrorInfo(BaseModel):
    category: FailureCategory
    message: str
    exception_type: str | None = None


class TaskView(BaseModel):
    id: str
    client_token: str
    status: TaskStatus
    spec: TaskSpec
    cancel_requested: bool
    result: Any | None = None
    error: TaskErrorInfo | None = None
    position_in_queue: int | None = None
    created_at: float
    started_at: float | None = None
    finished_at: float | None = None


class SubmitTaskRequest(BaseModel):
    client_token: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: float | None = None


class SubmitTaskResponse(BaseModel):
    task_id: str
    status: TaskStatus
    duplicate: bool  # True when an existing task was returned for the token


class MessageResponse(BaseModel):
    message: str
