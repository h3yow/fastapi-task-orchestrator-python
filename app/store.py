"""In-memory task store with a bounded waiting queue.

All mutations are guarded by one lock; state transitions are validated and
performed through :meth:`TaskStore.transition` so illegal edges cannot leak.
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field, replace
from typing import Callable

from app.errors import (
    IdempotencyConflictError,
    InvalidStateTransitionError,
    QueueFullError,
    TaskNotFoundError,
)
from app.models import ALLOWED_TRANSITIONS, TaskSpec, TaskStatus


@dataclass
class TaskRecord:
    id: str
    client_token: str
    spec: TaskSpec
    status: TaskStatus = TaskStatus.PENDING
    cancel_event: threading.Event = field(default_factory=threading.Event)
    cancel_requested: bool = False
    result: object = None
    error: object = None  # app.models.TaskErrorInfo
    created_at: float = 0.0
    started_at: float | None = None
    finished_at: float | None = None

    def snapshot(self) -> "TaskRecord":
        """Shallow copy safe to read outside the store lock."""
        return replace(self)


def fingerprint(spec: TaskSpec) -> str:
    """Canonical fingerprint of a task request payload.

    Same token + same fingerprint  -> idempotent replay.
    Same token + different fingerprint -> IdempotencyConflictError.
    """
    return json.dumps(
        spec.model_dump(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )


def new_record(token: str, spec: TaskSpec) -> TaskRecord:
    return TaskRecord(
        id=uuid.uuid4().hex,
        client_token=token,
        spec=spec,
        created_at=time.time(),
    )


class TaskStore:
    def __init__(self, max_queue_size: int, max_workers: int = 1) -> None:
        if max_queue_size < 1:
            raise ValueError("max_queue_size must be >= 1")
        if max_workers < 1:
            raise ValueError("max_workers must be >= 1")
        self.max_queue_size = max_queue_size
        self.max_workers = max_workers
        # Hard cap on in-flight work: tasks currently RUNNING or PENDING.
        self.max_inflight = max_workers + max_queue_size
        self._inflight = 0
        self._by_id: dict[str, TaskRecord] = {}
        # client token -> (task id, request fingerprint)
        self._tokens: dict[str, tuple[str, str]] = {}
        self._queue: deque[str] = deque()
        self._lock = threading.RLock()

    # -- idempotent submission ------------------------------------------------

    def submit(
        self, record: TaskRecord, spec: TaskSpec
    ) -> tuple[TaskRecord, bool]:
        """Atomically dedupe / register / enqueue a task.

        Returns ``(record, created)`` where ``created=False`` marks an
        idempotent replay of the same client token.
        Raises :class:`IdempotencyConflictError` on same token/new payload and
        :class:`QueueFullError` when in-flight capacity is saturated, so work
        can never accumulate without bound or be silently dropped.
        """
        fp = fingerprint(spec)
        with self._lock:
            existing = self._tokens.get(record.client_token)
            if existing is not None:
                existing_id, existing_fp = existing
                if existing_fp != fp:
                    raise IdempotencyConflictError(record.client_token)
                return self._by_id[existing_id], False

            # Drop queue slots held by tasks cancelled while still waiting so
            # bookkeeping reflects genuinely active work.
            self._reap_dead_queue_entries()

            if self._inflight >= self.max_inflight:
                raise QueueFullError(self.max_inflight)

            self._by_id[record.id] = record
            self._tokens[record.client_token] = (record.id, fp)
            self._queue.append(record.id)
            self._inflight += 1
            return record, True

    def _reap_dead_queue_entries(self) -> None:
        """Remove queue entries whose task was cancelled before dispatch."""
        alive = deque()
        reaped = 0
        for task_id in self._queue:
            if self._by_id[task_id].status == TaskStatus.PENDING:
                alive.append(task_id)
            else:
                reaped += 1
        self._queue = alive
        self._inflight = max(0, self._inflight - reaped)

    # -- queue operations -----------------------------------------------------

    def dequeue(self) -> TaskRecord | None:
        """Pop the head waiting task and move it PENDING -> RUNNING.

        Entries cancelled while waiting are discarded. Returns ``None`` only
        when no runnable task remains in the queue.
        """
        with self._lock:
            while self._queue:
                task_id = self._queue.popleft()
                record = self._by_id[task_id]
                if record.status != TaskStatus.PENDING:
                    continue  # cancelled before a worker got to it
                if self.transition(
                    task_id,
                    TaskStatus.RUNNING,
                    lambda r: setattr(r, "started_at", time.time()),
                ):
                    return record.snapshot()
            return None
    #-- reads -----------------------------------------------------------------

    def get(self, task_id: str) -> TaskRecord:
        with self._lock:
            record = self._by_id.get(task_id)
            if record is None:
                raise TaskNotFoundError(f"task {task_id!r} does not exist")
            return record.snapshot()

    def find_by_token(self, token: str) -> TaskRecord | None:
        with self._lock:
            entry = self._tokens.get(token)
            if entry is None:
                return None
            return self._by_id[entry[0]].snapshot()

    def queue_position(self, task_id: str) -> int | None:
        with self._lock:
            for position, queued_id in enumerate(self._queue, start=1):
                if (
                    queued_id == task_id
                    and self._by_id[task_id].status == TaskStatus.PENDING
                ):
                    return position
            return None

    def stats(self) -> dict[str, int]:
        with self._lock:
            counts = {status.value: 0 for status in TaskStatus}
            for record in self._by_id.values():
                counts[record.status.value] += 1
            counts["queue_depth"] = sum(
                1 for tid in self._queue
                if self._by_id[tid].status == TaskStatus.PENDING
            )
            counts["inflight"] = self._inflight
            counts["max_inflight"] = self.max_inflight
            counts["total"] = len(self._by_id)
            return counts

    #-- state transitions -----------------------------------------------------

    def transition(
        self,
        task_id: str,
        target: TaskStatus,
        mutate: Callable[[TaskRecord], None] | None = None,
        *,
        only_if: Callable[[TaskRecord], bool] | None = None,
    ) -> bool:
        """Validate and apply a state edge.

        Returns ``True`` when applied, ``False`` when the edge is illegal for
        the record's current state (or the record is gone). When ``only_if`` is
        given it is evaluated inside the same lock immediately before the edge
        is applied, enabling a deterministic CAS-style completion/cancel race.
        """
        with self._lock:
            record = self._by_id.get(task_id)
            if record is None:
                return False
            if target not in ALLOWED_TRANSITIONS[record.status]:
                return False
            if only_if is not None and not only_if(record):
                return False
            if mutate is not None:
                mutate(record)
            record.status = target
            if target.is_terminal:
                record.finished_at = time.time()
                self._inflight = max(0, self._inflight - 1)
            return True

    def request_cancel(self, task_id: str) -> TaskRecord:
        """Request cancellation.

        * PENDING  -> CANCELLED immediately (never runs, never yields result);
        * RUNNING  -> signaled; the runner/worker race decides the terminal
          state, exactly one CAS-like transition wins;
        * CANCELLED -> idempotent no-op, snapshot returned;
        * SUCCEEDED / FAILED -> InvalidStateTransitionError.
        """
        with self._lock:
            record = self._by_id.get(task_id)
            if record is None:
                raise TaskNotFoundError(f"task {task_id!r} does not exist")

            record.cancel_requested = True
            record.cancel_event.set()

            if record.status == TaskStatus.PENDING:
                if not self.transition(
                    task_id,
                    TaskStatus.CANCELLED,
                    lambda r: setattr(r, "error", _cancelled_error()),
                ):
                    raise InvalidStateTransitionError(
                        record.status.value, TaskStatus.CANCELLED.value
                    )
            elif record.status in (
                TaskStatus.SUCCEEDED,
                TaskStatus.FAILED,
            ):
                raise InvalidStateTransitionError(
                    record.status.value, TaskStatus.CANCELLED.value
                )
            # RUNNING: signal only; CANCELLED is reported as-is.
            return self._by_id[task_id].snapshot()


def _cancelled_error():
    from app.models import FailureCategory, TaskErrorInfo

    return TaskErrorInfo(
        category=FailureCategory.CANCELLED,
        message="task was cancelled before it started running",
    )
