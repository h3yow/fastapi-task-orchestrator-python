"""Application service: orchestrates idempotent submit / cancel / query."""

from __future__ import annotations

import logging

from app.executor import TaskExecutor
from app.models import SubmitTaskRequest, SubmitTaskResponse, TaskSpec
from app.store import TaskRecord, TaskStore, new_record

logger = logging.getLogger("app.service")


class TaskService:
    def __init__(
        self,
        max_workers: int = 2,
        max_queue_size: int = 100,
    ) -> None:
        self.max_workers = max_workers
        self.max_queue_size = max_queue_size
        self.store = TaskStore(
            max_queue_size=max_queue_size, max_workers=max_workers
        )
        self.executor = TaskExecutor(self.store, max_workers=max_workers)

    def start(self) -> None:
        self.executor.start()

    def shutdown(self) -> None:
        self.executor.shutdown()

    def submit(self, request: SubmitTaskRequest) -> SubmitTaskResponse:
        """Idempotent submit.

        Same token + same payload -> the original task is returned
        (``duplicate=True``), it is never re-enqueued or executed twice.
        Same token + different payload -> IdempotencyConflictError.
        """
        spec = TaskSpec(
            payload=request.payload,
            timeout_seconds=request.timeout_seconds,
        )
        record = new_record(request.client_token, spec)
        stored, created = self.store.submit(record, spec)
        if created:
            self.executor.notify()
            logger.info(
                "submit accepted token=%s task_id=%s status=%s",
                request.client_token, stored.id, stored.status.value,
            )
        else:
            logger.info(
                "submit deduped token=%s task_id=%s status=%s",
                request.client_token, stored.id, stored.status.value,
            )
        return SubmitTaskResponse(
            task_id=stored.id, status=stored.status, duplicate=not created
        )

    def cancel(self, task_id: str) -> TaskRecord:
        record = self.store.request_cancel(task_id)
        logger.info(
            "cancel requested task_id=%s token=%s status=%s",
            task_id, record.client_token, record.status.value,
        )
        return record

    def get(self, task_id: str) -> TaskRecord:
        return self.store.get(task_id)

    def stats(self) -> dict[str, int]:
        return self.store.stats()
