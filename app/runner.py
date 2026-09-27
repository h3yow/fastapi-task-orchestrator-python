"""Task execution logic (pure side-effect function run inside a worker).

The runner is cooperative: it periodically checks ``cancel_event`` so an
in-flight task responds to cancellation promptly and never produces a result
after being cancelled. Every failure is mapped to a ``FailureCategory``.

Demo payload contract (``TaskSpec.payload``):
  * ``sleep``   : float seconds to wait (default 0); interruptible;
  * ``raise``   : when true, raise ``RuntimeError(message)`` after sleeping;
  * ``message`` : message used by the raised error (default text provided).
Anything else is echoed back in the successful result.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any

from app.models import FailureCategory, TaskErrorInfo, TaskSpec, TaskStatus

_TICK_SECONDS = 0.02


@dataclass
class RunOutcome:
    """Terminal outcome produced by a single task execution."""

    status: TaskStatus  # SUCCEEDED, FAILED or CANCELLED
    result: Any | None = None
    error: TaskErrorInfo | None = None


def run_task(spec: TaskSpec, cancel_event: threading.Event) -> RunOutcome:
    payload = spec.payload
    sleep_seconds = float(payload.get("sleep", 0) or 0)
    should_raise = bool(payload.get("raise", False))
    message = str(payload.get("message", "task failed on purpose"))

    deadline: float | None = None
    if spec.timeout_seconds is not None:
        deadline = time.monotonic() + float(spec.timeout_seconds)

    slept = 0.0
    while slept < sleep_seconds:
        if cancel_event.is_set():
            return _cancelled("cancellation requested while task was waiting")
        remaining = sleep_seconds - slept
        wait = min(_TICK_SECONDS, remaining)
        if deadline is not None:
            budget = deadline - time.monotonic()
            if budget <= 0:
                return _timeout(spec.timeout_seconds)
            wait = min(wait, budget)
        if cancel_event.wait(wait):
            return _cancelled("cancellation requested while task was waiting")
        slept += wait

    if deadline is not None and time.monotonic() >= deadline:
        return _timeout(spec.timeout_seconds)
    if cancel_event.is_set():
        return _cancelled("cancellation requested before task completed")

    try:
        if should_raise:
            raise RuntimeError(message)
    except Exception as exc:  # task-body failure, not an orchestrator failure
        return RunOutcome(
            status=TaskStatus.FAILED,
            error=TaskErrorInfo(
                category=FailureCategory.EXECUTION_ERROR,
                message=str(exc),
                exception_type=type(exc).__name__,
            ),
        )

    return RunOutcome(
        status=TaskStatus.SUCCEEDED,
        result={"payload": payload, "slept_seconds": round(slept, 6)},
    )


def _cancelled(message: str) -> RunOutcome:
    return RunOutcome(
        status=TaskStatus.CANCELLED,
        error=TaskErrorInfo(
            category=FailureCategory.CANCELLED, message=message
        ),
    )


def _timeout(timeout_seconds: float | None) -> RunOutcome:
    return RunOutcome(
        status=TaskStatus.FAILED,
        error=TaskErrorInfo(
            category=FailureCategory.TASK_TIMEOUT,
            message=f"task exceeded its timeout of {timeout_seconds}s",
            exception_type="TimeoutError",
        ),
    )
