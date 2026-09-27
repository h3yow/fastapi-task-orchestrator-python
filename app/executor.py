"""Bounded worker-pool executor.

Concurrency is limited to ``max_workers`` daemon threads; the store holds a
bounded waiting queue, so overload is rejected at submit time (503
``queue_full``) instead of accumulating unbounded work.

Completion vs. cancellation is decided by a single CAS-like state transition
inside the store lock: exactly one side wins and only the winner's outcome is
ever recorded, so a task ends in exactly one terminal state.
"""

from __future__ import annotations

import logging
import threading

from app.models import FailureCategory, TaskErrorInfo, TaskStatus
from app.runner import RunOutcome, run_task
from app.store import TaskRecord, TaskStore

logger = logging.getLogger("app.executor")


class TaskExecutor:
    def __init__(self, store: TaskStore, max_workers: int = 2) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be >= 1")
        self.store = store
        self.max_workers = max_workers
        self._stop = threading.Event()
        self._condition = threading.Condition()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        if self._threads:
            return
        self._stop.clear()
        for i in range(self.max_workers):
            thread = threading.Thread(
                target=self._worker_loop,
                name=f"task-worker-{i + 1}",
                daemon=True,
            )
            thread.start()
            self._threads.append(thread)
        logger.info("executor started with %d workers", self.max_workers)

    def shutdown(self, timeout: float | None = 5.0) -> None:
        self._stop.set()
        with self._condition:
            self._condition.notify_all()
        for thread in self._threads:
            thread.join(timeout=timeout)
        self._threads.clear()
        logger.info("executor stopped")

    def notify(self) -> None:
        """Wake a sleeping worker after a new task has been enqueued."""
        with self._condition:
            self._condition.notify()

    def _worker_loop(self) -> None:
        name = threading.current_thread().name
        while not self._stop.is_set():
            record = self.store.dequeue()
            if record is None:
                with self._condition:
                    if self._stop.is_set():
                        break
                    # Re-check after taking the lock; submit() enqueues before
                    # notify(), so a queued task is always observed here and no
                    # wakeup can be lost.
                    if self.store.stats()["queue_depth"] == 0:
                        self._condition.wait(timeout=0.05)
                continue
            logger.info("%s picked task id=%s token=%s", name, record.id,
                        record.client_token)
            try:
                self._run_one(record)
            except Exception:
                logger.exception("worker crashed while running task %s",
                                 record.id)

    def _run_one(self, snapshot: TaskRecord) -> None:
        outcome = run_task(snapshot.spec, snapshot.cancel_event)
        self._commit(snapshot.id, outcome)

    def _commit(self, task_id: str, outcome: RunOutcome) -> None:
        """Apply exactly one terminal transition, deciding the completion vs.
        cancellation race atomically inside the store lock.

        Deterministic rule:
          * if the runner observed cancellation (CANCELLED outcome), the task
            ends CANCELLED and produces no result;
          * a SUCCEEDED outcome is committed only when cancellation has not been
            requested yet; if it has, cancellation wins and the result is
            discarded (a cancel that lands after the success commit loses, via
            the terminal-state rule in request_cancel);
          * a FAILED outcome is still recorded even if cancel raced, because
            the error itself is the informative outcome.
        """
        cancel_won = {"value": False}

        def apply(record: TaskRecord) -> None:
            if outcome.status == TaskStatus.SUCCEEDED:
                record.result = outcome.result
                record.error = None
            else:
                record.result = None
                record.error = outcome.error

        def apply_cancelled(record: TaskRecord) -> None:
            record.result = None
            record.error = TaskErrorInfo(
                category=FailureCategory.CANCELLED,
                message="cancellation won the race with task completion",
            )

        if outcome.status == TaskStatus.SUCCEEDED:
            won = self.store.transition(
                task_id,
                TaskStatus.SUCCEEDED,
                apply,
                only_if=lambda r: not r.cancel_requested,
            )
            if not won:
                cancel_won["value"] = True
                won = self.store.transition(
                    task_id, TaskStatus.CANCELLED, apply_cancelled
                )
        else:
            # CANCELLED observed by the runner, or a task failure: both are
            # committed directly; RUNNING -> CANCELLED is a legal edge.
            won = self.store.transition(task_id, outcome.status, apply)
            if not won:
                # Only possible if cancel raced and already closed the state;
                # align with that decision instead of dropping the task.
                cancel_won["value"] = True
                won = self.store.transition(
                    task_id, TaskStatus.CANCELLED, apply_cancelled
                )

        final = self.store.get(task_id)
        if won and not cancel_won["value"]:
            logger.info(
                "task id=%s token=%s -> %s (winner=completion)",
                task_id, final.client_token, final.status.value,
            )
        else:
            logger.info(
                "task id=%s token=%s runner=%s final=%s "
                "(winner=cancel, runner outcome discarded)",
                task_id, final.client_token, outcome.status.value,
                final.status.value,
            )
