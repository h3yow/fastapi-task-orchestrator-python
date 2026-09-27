"""Idempotency: duplicate submissions collapse to one task and never run twice;
the same token with a different payload is rejected with a distinct code.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from app.errors import IdempotencyConflictError
from app.models import SubmitTaskRequest

logger = logging.getLogger("test.idempotency")


def _wait_terminal(svc, task_id, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rec = svc.get(task_id)
        if rec.status.is_terminal:
            return rec
        time.sleep(0.01)
    raise AssertionError(f"task {task_id} did not finish")


def test_duplicate_submissions_run_exactly_once(service_factory):
    """Concurrent identical submissions: one task, one execution, one result."""
    svc = service_factory(max_workers=4, max_queue_size=100)

    token = "dup-token-1"
    request = SubmitTaskRequest(client_token=token, payload={"sleep": 0.15})
    barrier = threading.Barrier(20)

    def submit():
        barrier.wait()
        return svc.submit(request.model_copy(deep=True))

    with ThreadPoolExecutor(max_workers=20) as pool:
        responses = list(pool.map(lambda _: submit(), range(20)))

    task_ids = {r.task_id for r in responses}
    created = [r for r in responses if not r.duplicate]
    logger.info(
        "token=%s distinct_task_ids=%d created=%d duplicates=%d verdict=%s",
        token, len(task_ids), len(created), len(responses) - len(created),
        "single task" if len(task_ids) == 1 else "DUPLICATE TASK!",
    )
    assert len(task_ids) == 1
    assert len(created) == 1

    final = _wait_terminal(svc, created[0].task_id)
    logger.info("token=%s task_id=%s status=%s result=%s verdict=executed once",
                token, final.id, final.status.value, final.result)
    assert final.status.value == "succeeded"
    assert svc.store.find_by_token(token).id == final.id


def test_same_token_different_payload_is_rejected_with_distinct_code(
    service_factory,
):
    svc = service_factory()
    token = "dup-token-conflict"
    first = svc.submit(SubmitTaskRequest(
        client_token=token, payload={"op": "A"}))
    try:
        svc.submit(SubmitTaskRequest(
            client_token=token, payload={"op": "B"}))
        raise AssertionError("conflict was not rejected")
    except IdempotencyConflictError as exc:
        logger.info(
            "token=%s task_id=%s status=%s verdict=rejected code=%s "
            "reason=same token different payload",
            token, first.task_id, first.status.value, exc.code,
        )
        assert exc.code == "idempotency_conflict"
        assert exc.http_status == 409

    # Original task remains executable and unaffected by the rejected replay.
    final = _wait_terminal(svc, first.task_id)
    assert final.status.value == "succeeded"
    assert final.spec.payload == {"op": "A"}
    logger.info("token=%s task_id=%s status=%s verdict=original unaffected",
                token, final.id, final.status.value)


def test_replay_after_completion_returns_same_task_not_rerun(service_factory):
    svc = service_factory(max_workers=2, max_queue_size=100)
    token = "dup-token-after-done"
    req = SubmitTaskRequest(client_token=token, payload={"sleep": 0.05})
    first = svc.submit(req)
    final = _wait_terminal(svc, first.task_id)
    finished_at = final.finished_at

    replay = svc.submit(req.model_copy(deep=True))
    logger.info(
        "token=%s task_id=%s replay_id=%s duplicate=%s verdict=%s",
        token, first.task_id, replay.task_id, replay.duplicate,
        "same task returned, no rerun" if replay.task_id == first.task_id
        else "RERUN DETECTED",
    )
    assert replay.duplicate is True
    assert replay.task_id == first.task_id
    assert replay.status.value == "succeeded"
    assert svc.get(first.task_id).finished_at == finished_at


def test_concurrent_mixed_replays_never_overwrite_original(service_factory):
    """Original task established first; then it is hammered concurrently with
    both identical replays (idempotent) and conflicting payloads (rejected)."""
    svc = service_factory(max_workers=4, max_queue_size=100)
    token = "dup-token-race-conflict"
    original = svc.submit(SubmitTaskRequest(
        client_token=token, payload={"op": "ORIG", "sleep": 0.2}))

    barrier = threading.Barrier(20)
    counts = {"duplicate": 0, "conflict": 0}
    lock = threading.Lock()

    def submit(i):
        barrier.wait()
        payload = (
            {"op": "ORIG", "sleep": 0.2}
            if i % 2 == 0
            else {"op": f"OTHER-{i}"}
        )
        try:
            resp = svc.submit(SubmitTaskRequest(
                client_token=token, payload=payload))
            with lock:
                counts["duplicate"] += int(resp.duplicate)
        except IdempotencyConflictError:
            with lock:
                counts["conflict"] += 1

    with ThreadPoolExecutor(max_workers=20) as pool:
        list(pool.map(submit, range(20)))

    stored = svc.store.find_by_token(token)
    logger.info(
        "token=%s task_id=%s duplicates=%d conflicts=%d final_payload=%s "
        "verdict=%s",
        token, stored.id, counts["duplicate"], counts["conflict"],
        stored.spec.payload,
        "original preserved" if stored.spec.payload["op"] == "ORIG"
        else "ORIGINAL OVERWRITTEN",
    )
    assert counts["duplicate"] == 10
    assert counts["conflict"] == 10
    assert stored.id == original.task_id
    assert stored.spec.payload == {"op": "ORIG", "sleep": 0.2}
    final = _wait_terminal(svc, original.task_id)
    assert final.status.value == "succeeded"
