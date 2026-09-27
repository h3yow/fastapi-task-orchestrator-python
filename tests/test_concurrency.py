"""Bounded concurrency & queuing: no loss, no duplicate execution, predictable
rejection when saturated, and a hard cap on simultaneously running tasks.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from app.errors import QueueFullError
from app.models import SubmitTaskRequest

logger = logging.getLogger("test.concurrency")

MAX_WORKERS = 2
MAX_QUEUE = 5


def _wait_stats(svc, predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate(svc.stats()):
            return svc.stats()
        time.sleep(0.01)
    raise AssertionError(f"stats never satisfied: {svc.stats()}")


def test_no_task_lost_and_never_duplicated_under_burst(service_factory):
    svc = service_factory(max_workers=MAX_WORKERS, max_queue_size=100)
    n = 60
    barrier = threading.Barrier(n)
    responses: list = []
    responses_lock = threading.Lock()

    def submit(i):
        req = SubmitTaskRequest(
            client_token=f"burst-{i:03d}", payload={"sleep": 0.02})
        barrier.wait()
        resp = svc.submit(req)
        with responses_lock:
            responses.append(resp)

    with ThreadPoolExecutor(max_workers=n) as pool:
        list(pool.map(submit, range(n)))

    ids = [r.task_id for r in responses]
    logger.info("submitted=%d unique_task_ids=%d verdict=%s",
                n, len(set(ids)),
                "no duplicates" if len(set(ids)) == n else "DUPLICATE IDS")
    assert len(responses) == n
    assert len(set(ids)) == n
    assert all(not r.duplicate for r in responses)

    stats = _wait_stats(svc, lambda s: s["succeeded"] == n)
    logger.info("final_stats=%s verdict=nothing lost, each ran once", stats)
    assert stats["succeeded"] == n
    assert stats["failed"] == 0
    assert stats["cancelled"] == 0
    # Every stored task carries exactly one result.
    results = [svc.get(tid).result for tid in ids]
    assert all(r is not None for r in results)


def test_queue_overflow_is_rejected_not_accumulated(service_factory):
    """workers(2) running + queue(5) full -> the next submit gets 503
    queue_full; nothing is silently dropped or executed twice."""
    svc = service_factory(max_workers=MAX_WORKERS, max_queue_size=MAX_QUEUE)
    capacity = MAX_WORKERS + MAX_QUEUE  # in-flight + waiting

    accepted, rejected = [], []
    # Long tasks so the first submissions occupy workers and the rest queue.
    for i in range(capacity + 5):
        try:
            resp = svc.submit(SubmitTaskRequest(
                client_token=f"cap-{i:02d}", payload={"sleep": 0.6}))
            accepted.append(resp)
        except QueueFullError as exc:
            rejected.append((i, exc.code))

    logger.info("accepted=%d rejected=%d rejected_codes=%s verdict=%s",
                len(accepted), len(rejected), sorted({c for _, c in rejected}),
                "bounded" if len(accepted) == capacity else "UNBOUNDED")
    assert len(accepted) == capacity
    assert len(rejected) == 5
    assert all(code == "queue_full" for _, code in rejected)

    stats = svc.stats()
    assert stats["running"] + stats["pending"] == capacity
    logger.info("inflight_stats=%s verdict=no silent accumulation", stats)

    # Cancelling one queued task frees exactly one slot.
    queued_id = accepted[capacity - 1].task_id  # last accepted => queued
    svc.cancel(queued_id)
    time.sleep(0.05)
    resp = svc.submit(SubmitTaskRequest(
        client_token="cap-freed", payload={"sleep": 0.1}))
    logger.info("token=cap-freed task_id=%s status=%s "
                "verdict=freed slot reusable", resp.task_id, resp.status.value)
    assert resp.duplicate is False


def test_running_parallelism_is_hard_capped(service_factory):
    """Observed concurrency must never exceed the worker count."""
    svc = service_factory(max_workers=MAX_WORKERS, max_queue_size=50)
    running_now = 0
    peak = 0
    lock = threading.Lock()
    original_run = None

    import app.executor as executor_module

    def instrumented_run(spec, cancel_event):
        nonlocal running_now, peak
        with lock:
            running_now += 1
            peak = max(peak, running_now)
        try:
            return original_run(spec, cancel_event)
        finally:
            with lock:
                running_now -= 1

    original_run = executor_module.run_task
    executor_module.run_task = instrumented_run
    try:
        for i in range(20):
            svc.submit(SubmitTaskRequest(
                client_token=f"par-{i:02d}", payload={"sleep": 0.05}))
        _wait_stats(svc, lambda s: s["succeeded"] == 20, timeout=15)
    finally:
        executor_module.run_task = original_run

    logger.info("peak_concurrent=%d limit=%d verdict=%s", peak, MAX_WORKERS,
                "within cap" if peak <= MAX_WORKERS else "CAP EXCEEDED")
    assert peak == MAX_WORKERS


def test_queue_position_reports_pending_order(service_factory):
    svc = service_factory(max_workers=1, max_queue_size=10)
    svc.submit(SubmitTaskRequest(client_token="q-0", payload={"sleep": 0.4}))
    time.sleep(0.05)  # let the worker pick q-0
    queued = []
    for i in range(1, 4):
        queued.append(svc.submit(SubmitTaskRequest(
            client_token=f"q-{i}", payload={"sleep": 0.05})).task_id)

    positions = [svc.store.queue_position(tid) for tid in queued]
    logger.info("queue_positions=%s verdict=FIFO ordering visible", positions)
    assert positions == [1, 2, 3]
    _wait_stats(svc, lambda s: s["succeeded"] == 4, timeout=10)
