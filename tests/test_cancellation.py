"""Cancellation & failure semantics, including the run/cancel race:

* queued cancel  -> CANCELLED, never runs, never yields a result;
* running cancel -> cooperative interrupt, no result after cancellation;
* near-simultaneous complete-vs-cancel -> exactly one terminal state;
* repeated cancels are idempotent; cancelling a finished task is rejected;
* execution errors and timeouts are surfaced with distinct categories.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from app.errors import InvalidStateTransitionError
from app.models import FailureCategory, SubmitTaskRequest

logger = logging.getLogger("test.cancellation")


def _wait_terminal(svc, task_id, timeout=10.0):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = svc.get(task_id)
        if last.status.is_terminal:
            return last
        time.sleep(0.005)
    raise AssertionError(f"task {task_id} never terminated: {last}")


def test_cancel_queued_task_never_runs(service_factory):
    svc = service_factory(max_workers=1, max_queue_size=10)
    # Occupy the single worker.
    blocker = svc.submit(SubmitTaskRequest(
        client_token="blk", payload={"sleep": 0.5}))
    time.sleep(0.05)
    queued = svc.submit(SubmitTaskRequest(
        client_token="waiter", payload={"raise": True, "message": "must not run"}))
    assert svc.store.queue_position(queued.task_id) == 1

    rec = svc.cancel(queued.task_id)
    logger.info("token=waiter task_id=%s status=%s cancel_requested=%s "
                "verdict=cancelled while queued",
                queued.task_id, rec.status.value, rec.cancel_requested)
    assert rec.status.value == "cancelled"
    assert rec.result is None
    assert rec.error.category == FailureCategory.CANCELLED

    # Wait for the worker to drain; the cancelled queued task must not run.
    _wait_terminal(svc, blocker.task_id)
    time.sleep(0.15)
    final = svc.get(queued.task_id)
    logger.info("token=waiter task_id=%s status=%s result=%s "
                "verdict=%s", final.id, final.status.value, final.result,
                "never executed" if final.status.value == "cancelled"
                and final.result is None else "EXECUTED AFTER CANCEL")
    assert final.status.value == "cancelled"
    assert final.result is None
    stats = svc.stats()
    assert stats["failed"] == 0
    assert stats["succeeded"] == 1


def test_cancel_running_task_interrupts_and_has_no_result(service_factory):
    svc = service_factory(max_workers=1, max_queue_size=10)
    resp = svc.submit(SubmitTaskRequest(
        client_token="long-run", payload={"sleep": 5}))
    time.sleep(0.1)  # let it start
    assert svc.get(resp.task_id).status.value == "running"

    rec = svc.cancel(resp.task_id)
    assert rec.cancel_requested is True
    final = _wait_terminal(svc, resp.task_id)
    logger.info("token=long-run task_id=%s status=%s result=%s error=%s "
                "verdict=%s", final.id, final.status.value, final.result,
                final.error.category.value if final.error else None,
                "interrupted cleanly" if final.status.value == "cancelled"
                else "CANCEL IGNORED")
    assert final.status.value == "cancelled"
    assert final.result is None
    assert final.error.category == FailureCategory.CANCELLED


def test_complete_and_cancel_race_yields_exactly_one_outcome(service_factory):
    """Fire completion and cancellation at nearly the same instant many times.
    Every task must end in exactly one terminal state, and CANCELLED tasks must
    never carry a result (no contradiction)."""
    svc = service_factory(max_workers=4, max_queue_size=200)
    rounds = 60
    outcomes: dict[str, int] = {}
    lock = threading.Lock()

    def round_once(i):
        token = f"race-{i:03d}"
        # Sleep varies so sometimes completion wins, sometimes cancel wins.
        resp = svc.submit(SubmitTaskRequest(
            client_token=token, payload={"sleep": 0.02 + (i % 5) * 0.008}))
        # Cancel at a random-ish point around the completion time.
        time.sleep(0.015 + (i % 7) * 0.004)
        reject_reason = None
        try:
            svc.cancel(resp.task_id)
        except InvalidStateTransitionError as exc:
            # Task already terminal before cancel landed -> completion wins;
            # this is an expected, clearly-distinguished race outcome.
            reject_reason = exc.code
        final = _wait_terminal(svc, resp.task_id)
        with lock:
            outcomes[final.status.value] = outcomes.get(final.status.value, 0) + 1
        logger.info("token=%s task_id=%s status=%s result_present=%s "
                    "cancel_rejected=%s verdict=single terminal outcome",
                    token, final.id, final.status.value,
                    final.result is not None, reject_reason)
        if reject_reason is not None:
            assert reject_reason == "invalid_state_transition"
        # Core invariant: a cancelled task never exposes a produced result.
        if final.status.value == "cancelled":
            assert final.result is None
        # Only one terminal timestamp, status stable under repeated reads.
        reread = svc.get(final.id)
        assert reread.status == final.status
        assert reread.finished_at == final.finished_at

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(round_once, range(rounds)))

    logger.info("race_outcomes=%s total=%d verdict=exactly one winner each",
                outcomes, rounds)
    assert sum(outcomes.values()) == rounds
    assert set(outcomes) <= {"cancelled", "succeeded", "failed"}
    # At least a few of each side should win across the jitter; not a hard
    # requirement for correctness, so only log it.
    assert outcomes.get("cancelled", 0) >= 1


def test_cancel_is_idempotent_and_cancelling_finished_is_rejected(
    service_factory,
):
    svc = service_factory(max_workers=2, max_queue_size=10)
    resp = svc.submit(SubmitTaskRequest(
        client_token="cancel-idem", payload={"sleep": 0.05}))
    final = _wait_terminal(svc, resp.task_id)
    assert final.status.value == "succeeded"

    from app.errors import InvalidStateTransitionError

    try:
        svc.cancel(resp.task_id)
        raise AssertionError("cancel of succeeded task must be rejected")
    except InvalidStateTransitionError as exc:
        logger.info("token=cancel-idem task_id=%s status=succeeded "
                    "verdict=cancel rejected code=%s", resp.task_id, exc.code)
        assert exc.code == "invalid_state_transition"

    # Cancelling an already cancelled task is a no-op, not an error.
    running = svc.submit(SubmitTaskRequest(
        client_token="cancel-twice", payload={"sleep": 3}))
    time.sleep(0.1)
    first = svc.cancel(running.task_id)
    second = svc.cancel(running.task_id)
    final = _wait_terminal(svc, running.task_id)
    logger.info("token=cancel-twice task_id=%s first=%s second=%s final=%s "
                "verdict=idempotent cancel", running.task_id,
                first.status.value, second.status.value, final.status.value)
    assert final.status.value == "cancelled"
    assert second.cancel_requested is True


def test_failure_categories_are_distinguishable(service_factory):
    svc = service_factory(max_workers=2, max_queue_size=10)

    err = svc.submit(SubmitTaskRequest(
        client_token="fail-err", payload={"raise": True, "message": "kaboom"}))
    timeout = svc.submit(SubmitTaskRequest(
        client_token="fail-timeout",
        payload={"sleep": 1.0}, timeout_seconds=0.05))

    err_final = _wait_terminal(svc, err.task_id)
    to_final = _wait_terminal(svc, timeout.task_id)

    logger.info("token=fail-err task_id=%s status=%s category=%s type=%s "
                "message=%s verdict=execution error classified",
                err_final.id, err_final.status.value,
                err_final.error.category.value,
                err_final.error.exception_type, err_final.error.message)
    assert err_final.status.value == "failed"
    assert err_final.error.category == FailureCategory.EXECUTION_ERROR
    assert err_final.error.exception_type == "RuntimeError"
    assert "kaboom" in err_final.error.message

    logger.info("token=fail-timeout task_id=%s status=%s category=%s "
                "verdict=timeout classified", to_final.id,
                to_final.status.value,
                to_final.error.category.value)
    assert to_final.status.value == "failed"
    assert to_final.error.category == FailureCategory.TASK_TIMEOUT

    # A cancelled failure uses its own category and must not look like an error.
    canceled = svc.submit(SubmitTaskRequest(
        client_token="fail-cancel", payload={"sleep": 3}))
    time.sleep(0.1)
    svc.cancel(canceled.task_id)
    c_final = _wait_terminal(svc, canceled.task_id)
    assert c_final.status.value == "cancelled"
    assert c_final.error.category == FailureCategory.CANCELLED
    logger.info("token=fail-cancel task_id=%s status=%s category=%s "
                "verdict=cancel category distinct", c_final.id,
                c_final.status.value, c_final.error.category.value)
