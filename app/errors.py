"""Error taxonomy for the task orchestrator.

Each error has a stable ``code`` that is exposed to API clients so that
different failure causes (idempotency conflict, illegal state transition,
queue saturation, unknown task ...) remain distinguishable.
"""

from __future__ import annotations


class TaskOrchestratorError(Exception):
    """Base class for all expected orchestrator errors."""

    code: str = "internal_error"
    http_status: int = 500

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or self.code)
        self.message = message or self.code


class TaskNotFoundError(TaskOrchestratorError):
    code = "task_not_found"
    http_status = 404


class IdempotencyConflictError(TaskOrchestratorError):
    """Same client token reused with a *different* request payload."""

    code = "idempotency_conflict"
    http_status = 409

    def __init__(self, token: str) -> None:
        super().__init__(
            f"client token {token!r} was already used with a different payload"
        )
        self.token = token


class InvalidStateTransitionError(TaskOrchestratorError):
    """A task was asked to leave its state through an illegal transition."""

    code = "invalid_state_transition"
    http_status = 409

    def __init__(self, from_state: str, to_state: str) -> None:
        super().__init__(f"illegal transition: {from_state} -> {to_state}")
        self.from_state = from_state
        self.to_state = to_state


class QueueFullError(TaskOrchestratorError):
    """In-flight capacity (running workers + waiting queue) is saturated."""

    code = "queue_full"
    http_status = 503

    def __init__(self, max_inflight: int) -> None:
        super().__init__(
            f"in-flight capacity saturated (running + waiting = "
            f"{max_inflight}); retry later"
        )
        self.max_inflight = max_inflight
