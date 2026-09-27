"""State machine transition table and failure-code taxonomy tests."""

from __future__ import annotations

import logging

import pytest

from app.errors import (
    IdempotencyConflictError,
    InvalidStateTransitionError,
    QueueFullError,
    TaskNotFoundError,
)
from app.models import ALLOWED_TRANSITIONS, TaskSpec, TaskStatus
from app.store import TaskStore, new_record

logger = logging.getLogger("test.state_machine")


def _log_verdict(token, task_id, status, reason):
    logger.info("token=%s task_id=%s status=%s verdict=%s",
                token, task_id, status, reason)


def test_transition_table_exactly_matches_documented_edges():
    assert ALLOWED_TRANSITIONS == {
        TaskStatus.PENDING: {TaskStatus.RUNNING, TaskStatus.CANCELLED},
        TaskStatus.RUNNING: {
            TaskStatus.SUCCEEDED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        },
        TaskStatus.SUCCEEDED: set(),
        TaskStatus.FAILED: set(),
        TaskStatus.CANCELLED: set(),
    }
    # terminal states never leave
    for terminal in (TaskStatus.SUCCEEDED, TaskStatus.FAILED,
                     TaskStatus.CANCELLED):
        assert ALLOWED_TRANSITIONS[terminal] == frozenset()
        assert terminal.is_terminal
    logger.info("verdict=PASS reason=transition table is closed for terminal "
                "states")


def test_illegal_edges_are_rejected_and_reported():
    store = TaskStore(max_queue_size=10)
    rec, _ = store.submit(new_record("tok-edge", TaskSpec()), TaskSpec())

    # PENDING cannot jump straight to SUCCEEDED / FAILED
    for target in (TaskStatus.SUCCEEDED, TaskStatus.FAILED):
        assert store.transition(rec.id, target) is False
        _log_verdict("tok-edge", rec.id, store.get(rec.id).status.value,
                     f"illegal edge pending->{target.value} rejected")

    # PENDING -> RUNNING -> SUCCEEDED
    assert store.transition(rec.id, TaskStatus.RUNNING)
    assert store.transition(rec.id, TaskStatus.SUCCEEDED)
    finished = store.get(rec.id)
    assert finished.status == TaskStatus.SUCCEEDED
    assert finished.finished_at is not None
    # SUCCEEDED cannot be cancelled
    with pytest.raises(InvalidStateTransitionError) as exc:
        store.request_cancel(rec.id)
    _log_verdict("tok-edge", rec.id, "succeeded",
                 f"cancel rejected code={exc.value.code}")
    assert exc.value.code == "invalid_state_transition"


def test_error_codes_are_distinguishable():
    # 1 worker + queue 1 => 2 in-flight slots; fill both, third is rejected.
    store = TaskStore(max_queue_size=1, max_workers=1)
    store.submit(new_record("tok-w", TaskSpec()), TaskSpec())
    spec = TaskSpec()
    store.submit(new_record("tok-x", spec), spec)
    with pytest.raises(QueueFullError) as e1:
        store.submit(new_record("tok-y", TaskSpec()), TaskSpec())
    with pytest.raises(IdempotencyConflictError) as e2:
        store.submit(new_record("tok-x", TaskSpec(payload={"z": 1})),
                     TaskSpec(payload={"z": 1}))
    with pytest.raises(TaskNotFoundError) as e3:
        store.get("missing-id")
    codes = {e1.value.code, e2.value.code, e3.value.code}
    logger.info("verdict=PASS codes=%s reason=each overload/conflict/missing "
                "case has a distinct code", sorted(codes))
    assert codes == {"queue_full", "idempotency_conflict", "task_not_found"}
