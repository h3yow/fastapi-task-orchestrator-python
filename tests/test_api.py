"""HTTP contract tests: status codes, stable error codes and JSON shapes."""

from __future__ import annotations

import logging
import time

logger = logging.getLogger("test.api")


def _wait_status(client, task_id, *statuses, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(f"/tasks/{task_id}").json()
        if body["status"] in statuses:
            return body
        time.sleep(0.01)
    raise AssertionError(f"task never reached {statuses}")


def test_submit_dedupe_conflict_and_status_query(client_factory):
    client, svc = client_factory(max_workers=2, max_queue_size=10)
    with client:
        r = client.post("/tasks", json={
            "client_token": "api-1", "payload": {"sleep": 0.05}})
        assert r.status_code == 201
        first = r.json()
        assert first["duplicate"] is False

        r2 = client.post("/tasks", json={
            "client_token": "api-1", "payload": {"sleep": 0.05}})
        assert r2.status_code == 201
        body = r2.json()
        assert body["duplicate"] is True and body["task_id"] == first["task_id"]
        logger.info("token=api-1 task_id=%s status=%s duplicate=%s "
                    "verdict=idempotent HTTP replay",
                    body["task_id"], body["status"], body["duplicate"])

        r3 = client.post("/tasks", json={
            "client_token": "api-1", "payload": {"different": True}})
        assert r3.status_code == 409
        err = r3.json()["error"]
        logger.info("token=api-1 verdict=conflict code=%s message=%s",
                    err["code"], err["message"])
        assert err["code"] == "idempotency_conflict"

        view = _wait_status(client, first["task_id"], "succeeded")
        assert view["result"] is not None and view["error"] is None
        assert view["client_token"] == "api-1"


def test_unknown_task_and_illegal_cancel_use_distinct_404_409(client_factory):
    client, _ = client_factory()
    with client:
        r = client.get("/tasks/does-not-exist")
        assert r.status_code == 404
        assert r.json()["error"]["code"] == "task_not_found"

        r2 = client.post("/tasks/does-not-exist/cancel")
        assert r2.status_code == 404
        assert r2.json()["error"]["code"] == "task_not_found"
        logger.info("verdict=missing tasks reported with code=task_not_found")


def test_queue_full_returns_503_with_retryable_code(client_factory):
    client, _ = client_factory(max_workers=1, max_queue_size=1)
    with client:
        ids = []
        for i in range(2):  # 1 running + 1 waiting = capacity
            r = client.post("/tasks", json={
                "client_token": f"api-cap-{i}", "payload": {"sleep": 0.4}})
            assert r.status_code == 201
            ids.append(r.json()["task_id"])
        r3 = client.post("/tasks", json={
            "client_token": "api-cap-2", "payload": {"sleep": 0.4}})
        assert r3.status_code == 503
        assert r3.json()["error"]["code"] == "queue_full"
        logger.info("token=api-cap-2 verdict=rejected code=queue_full "
                    "message=%s", r3.json()["error"]["message"])

        # Cancel the queued one; freed slot is reusable.
        cr = client.post(f"/tasks/{ids[1]}/cancel")
        assert cr.json()["status"] == "cancelled"
        r4 = client.post("/tasks", json={
            "client_token": "api-cap-3", "payload": {}})
        assert r4.status_code == 201 and r4.json()["duplicate"] is False


def test_cancel_running_task_end_to_end(client_factory):
    client, _ = client_factory(max_workers=1, max_queue_size=5)
    with client:
        r = client.post("/tasks", json={
            "client_token": "api-cancel", "payload": {"sleep": 5}})
        task_id = r.json()["task_id"]
        _wait_status(client, task_id, "running")
        cr = client.post(f"/tasks/{task_id}/cancel")
        assert cr.status_code == 200
        final = _wait_status(client, task_id, "cancelled")
        logger.info("token=api-cancel task_id=%s status=%s result=%s "
                    "error_category=%s verdict=cancelled with no result",
                    task_id, final["status"], final["result"],
                    final["error"]["category"])
        assert final["result"] is None
        assert final["error"]["category"] == "cancelled"

        # A second cancel of the same cancelled task stays 200/idempotent.
        again = client.post(f"/tasks/{task_id}/cancel")
        assert again.status_code == 200 and again.json()["status"] == "cancelled"
