"""任务领域模型与状态机定义。"""
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL_STATES: frozenset[TaskStatus] = frozenset(
    {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELLED}
)

# 合法状态流转表
ALLOWED_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset(
        {TaskStatus.RUNNING, TaskStatus.CANCELLED, TaskStatus.FAILED}
    ),
    TaskStatus.RUNNING: frozenset({TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELLED}),
    TaskStatus.SUCCEEDED: frozenset(),
    TaskStatus.FAILED: frozenset(),
    TaskStatus.CANCELLED: frozenset(),
}


class IllegalTransitionError(Exception):
    """非法状态流转。"""

    def __init__(self, current: TaskStatus, target: TaskStatus) -> None:
        super().__init__(f"illegal transition: {current.value} -> {target.value}")
        self.current = current
        self.target = target


@dataclass
class Task:
    token: str
    payload: str
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    status: TaskStatus = TaskStatus.PENDING
    result: str | None = None
    error: str | None = None
    error_kind: str | None = None
    created_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    cancel_event: threading.Event = field(default_factory=threading.Event)

    def transition(self, target: TaskStatus) -> None:
        """校验并执行状态流转，非法流转抛 IllegalTransitionError。"""
        if target not in ALLOWED_TRANSITIONS[self.status]:
            raise IllegalTransitionError(self.status, target)
        self.status = target
        if target in TERMINAL_STATES:
            self.finished_at = time.time()
