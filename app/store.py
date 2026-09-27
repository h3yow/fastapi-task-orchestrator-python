"""线程安全的内存任务存储，负责幂等键（客户端令牌）管理。"""
from __future__ import annotations

import threading

from app.errors import TaskNotFoundError, TokenConflictError
from app.models import Task, TaskStatus


class TaskStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._by_id: dict[str, Task] = {}
        self._id_by_token: dict[str, str] = {}

    def get_or_create(self, token: str, payload: str) -> tuple[Task, bool]:
        """按令牌幂等获取或创建任务。

        返回 (task, created)。令牌已存在且内容一致时返回已有任务；
        令牌存在但内容不同则抛 TokenConflictError。
        整个判定与写入在同一把锁内完成，并发提交同一令牌只会创建一个任务。
        """
        with self._lock:
            existing_id = self._id_by_token.get(token)
            if existing_id is not None:
                task = self._by_id[existing_id]
                if task.payload != payload:
                    raise TokenConflictError(
                        f"token {token!r} already used with different payload"
                    )
                return task, False
            task = Task(token=token, payload=payload)
            self._by_id[task.id] = task
            self._id_by_token[token] = task.id
            return task, True

    def get(self, task_id: str) -> Task:
        with self._lock:
            task = self._by_id.get(task_id)
            if task is None:
                raise TaskNotFoundError(f"task {task_id!r} not found")
            return task

    def transition(self, task_id: str, target: TaskStatus, **fields: object) -> Task:
        """在锁内完成状态流转校验与字段更新，保证终态唯一。

        取消与完成竞态时，先拿到锁的一方完成流转，另一方因非法流转被拒绝。
        """
        with self._lock:
            task = self._by_id.get(task_id)
            if task is None:
                raise TaskNotFoundError(f"task {task_id!r} not found")
            task.transition(target)
            for key, value in fields.items():
                setattr(task, key, value)
            return task
