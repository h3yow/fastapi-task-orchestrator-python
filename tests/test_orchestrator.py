"""任务编排器行为测试：重复提交、并发提交、取消竞态、失败分类。

日志约定：每个用例打印令牌、任务状态与判定依据（basis），
便于从测试日志直接解释判定结果。
"""
from __future__ import annotations

import json
import logging
import threading
import time

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.errors import InvalidStateError, QueueFullError, TokenConflictError
from app.executor import Executor
from app.models import IllegalTransitionError, Task, TaskStatus
from app.service import TaskCancelled, TaskExecutionError, TaskService
from app.store import TaskStore

logger = logging.getLogger("test.orchestrator")


def make_service(workers: int = 2, queue_size: int = 8, work_fn=None) -> TaskService:
    return TaskService(TaskStore(), Executor(max_workers=workers, max_queue=queue_size), work_fn or (lambda t: "ok"))


def wait_status(svc: TaskService, task_id: str, timeout: float = 3.0) -> Task:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        task = svc.get(task_id)
        if task.status in (TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELLED):
            return task
        time.sleep(0.005)
    raise AssertionError(f"task {task_id} not terminal within {timeout}s")


def verdict(token: str, task: Task, basis: str) -> None:
    logger.info(
        "VERDICT token=%s task=%s status=%s error_kind=%s basis=%s",
        token, task.id, task.status.value, task.error_kind, basis,
    )


class CountingWork:
    """统计执行次数的任务体，支持协作式取消。"""

    def __init__(self, duration: float = 0.05) -> None:
        self.duration = duration
        self.calls: list[str] = []
        self._lock = threading.Lock()

    def __call__(self, task: Task) -> str:
        with self._lock:
            self.calls.append(task.token)
        deadline = time.monotonic() + self.duration
        while time.monotonic() < deadline:
            if task.cancel_event.is_set():
                raise TaskCancelled()
            time.sleep(0.002)
        return f"ok:{task.token}"

    def count(self, token: str) -> int:
        with self._lock:
            return self.calls.count(token)


# ---------- 幂等与重复提交 ----------

def test_duplicate_submission_same_token_executes_once():
    work = CountingWork()
    svc = make_service(work_fn=work)
    t1, c1 = svc.submit("dup", json.dumps({}))
    t2, c2 = svc.submit("dup", json.dumps({}))
    task = wait_status(svc, t1.id)
    verdict("dup", task, "同令牌同内容第二次提交应幂等返回且只执行一次")
    assert c1 and not c2
    assert t1.id == t2.id
    assert work.count("dup") == 1


def test_same_token_different_payload_rejected_with_distinct_reason():
    svc = make_service()
    svc.submit("tok", "payload-A")
    with pytest.raises(TokenConflictError) as exc_info:
        svc.submit("tok", "payload-B")
    task = svc.get(svc.submit("tok", "payload-A")[0].id)
    verdict("tok", task, "同令牌不同内容必须拒绝，code=TOKEN_CONFLICT 可区分于其他失败")
    assert exc_info.value.code == "TOKEN_CONFLICT"
    assert exc_info.value.code != QueueFullError.code


def test_concurrent_same_token_single_task_single_execution():
    work = CountingWork()
    svc = make_service(workers=4, work_fn=work)
    created_flags: list[bool] = []
    ids: list[str] = []
    lock = threading.Lock()
    barrier = threading.Barrier(16)

    def submit():
        barrier.wait()
        task, created = svc.submit("race", json.dumps({"n": 1}))
        with lock:
            created_flags.append(created)
            ids.append(task.id)

    threads = [threading.Thread(target=submit) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    task = wait_status(svc, ids[0])
    verdict("race", task, "16 线程并发同令牌提交：仅 1 个创建、1 个任务 ID、执行 1 次")
    assert sum(created_flags) == 1
    assert len(set(ids)) == 1
    assert work.count("race") == 1


def test_concurrent_distinct_tokens_no_loss_no_duplication():
    work = CountingWork(duration=0.01)
    n = 30
    svc = make_service(workers=4, queue_size=64, work_fn=work)

    def submit(i: int):
        svc.submit(f"tok-{i}", json.dumps({"i": i}))

    threads = [threading.Thread(target=submit, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for i in range(n):
        task, _ = svc.submit(f"tok-{i}", json.dumps({"i": i}))
        wait_status(svc, task.id)
        verdict(f"tok-{i}", task, "并发不同令牌：每个令牌恰好执行一次，不丢不重")
        assert work.count(f"tok-{i}") == 1
        assert task.status == TaskStatus.SUCCEEDED


# ---------- 取消 ----------

def test_cancel_running_task_produces_no_result():
    work = CountingWork(duration=5.0)
    svc = make_service(work_fn=work)
    task, _ = svc.submit("cancel-run", json.dumps({}))
    while svc.get(task.id).status != TaskStatus.RUNNING:
        time.sleep(0.005)
    cancelled = svc.cancel(task.id)
    task = wait_status(svc, task.id)
    verdict("cancel-run", task, "运行中取消：终态 CANCELLED 且不得产出 result")
    assert cancelled.status == TaskStatus.CANCELLED
    assert task.status == TaskStatus.CANCELLED
    assert task.result is None


def test_cancel_queued_task_never_executes():
    started = threading.Event()
    release = threading.Event()

    def slow(task: Task) -> str:
        started.set()
        release.wait(3)
        return "ok"

    svc = TaskService(TaskStore(), Executor(max_workers=1, max_queue=4), slow)
    svc.submit("blocker", "{}")
    assert started.wait(2)
    queued, _ = svc.submit("cancel-queued", "{}")
    svc.cancel(queued.id)
    release.set()
    task = svc.get(queued.id)
    verdict("cancel-queued", task, "排队中取消：工作线程取出后跳过，不得执行")
    assert task.status == TaskStatus.CANCELLED
    assert task.result is None


def test_cancel_terminal_task_rejected():
    svc = make_service()
    task, _ = svc.submit("done", "{}")
    wait_status(svc, task.id)
    with pytest.raises(InvalidStateError) as exc_info:
        svc.cancel(task.id)
    verdict("done", svc.get(task.id), "终态任务取消必须拒绝，code=INVALID_STATE")
    assert exc_info.value.code == "INVALID_STATE"


def test_cancel_vs_complete_race_single_deterministic_outcome():
    for i in range(50):
        duration = 0.002

        def work(task: Task, d=duration) -> str:
            time.sleep(d)
            return f"ok:{task.token}"

        svc = make_service(workers=1, work_fn=work)
        token = f"race-{i}"
        task, _ = svc.submit(token, "{}")
        canceller = threading.Thread(target=lambda: _try_cancel(svc, task.id))
        canceller.start()
        final = wait_status(svc, task.id)
        canceller.join()
        final = svc.get(task.id)
        verdict(token, final, "取消与完成竞态：终态唯一且自洽（CANCELLED 必无 result）")
        assert final.status in (TaskStatus.SUCCEEDED, TaskStatus.CANCELLED)
        if final.status == TaskStatus.CANCELLED:
            assert final.result is None
        else:
            assert final.result == f"ok:{token}"
        # 再取消一次：要么 INVALID_STATE，要么状态不变
        try:
            svc.cancel(task.id)
        except InvalidStateError:
            pass
        assert svc.get(task.id).status == final.status


def _try_cancel(svc: TaskService, task_id: str) -> None:
    try:
        svc.cancel(task_id)
    except InvalidStateError:
        pass


# ---------- 排队与失败分类 ----------

def test_queue_full_rejection_recorded_with_distinct_reason():
    release = threading.Event()

    def slow(task: Task) -> str:
        release.wait(3)
        return "ok"

    svc = TaskService(TaskStore(), Executor(max_workers=1, max_queue=1), slow)
    running, _ = svc.submit("running", "{}")
    while svc.get(running.id).status != TaskStatus.RUNNING:
        time.sleep(0.005)
    svc.submit("queued", "{}")
    with pytest.raises(QueueFullError) as exc_info:
        svc.submit("overflow", "{}")
    assert exc_info.value.code == "QUEUE_FULL"
    task, created = svc.submit("overflow", "{}")
    verdict("overflow", task, "队列满拒绝：任务记录 FAILED/QUEUE_FULL，原因可查询可区分")
    assert not created
    assert task.status == TaskStatus.FAILED
    assert task.error_kind == "QUEUE_FULL"
    release.set()


def test_failure_reasons_are_classified():
    from app.service import default_work

    svc = make_service(work_fn=default_work)
    bad_payload, _ = svc.submit("bad-json", "{not-json")
    biz_fail, _ = svc.submit("biz-fail", json.dumps({"fail": True}))
    t1 = wait_status(svc, bad_payload.id)
    t2 = wait_status(svc, biz_fail.id)
    verdict("bad-json", t1, "非法 JSON 负载 => FAILED/PAYLOAD_ERROR")
    verdict("biz-fail", t2, "业务主动失败 => FAILED/TASK_ERROR，与 PAYLOAD_ERROR 可区分")
    assert (t1.status, t1.error_kind) == (TaskStatus.FAILED, "PAYLOAD_ERROR")
    assert (t2.status, t2.error_kind) == (TaskStatus.FAILED, "TASK_ERROR")
    assert t1.error_kind != t2.error_kind


def test_illegal_transition_rejected():
    task = Task(token="t", payload="p")
    task.transition(TaskStatus.RUNNING)
    task.transition(TaskStatus.SUCCEEDED)
    with pytest.raises(IllegalTransitionError):
        task.transition(TaskStatus.RUNNING)
    with pytest.raises(IllegalTransitionError):
        Task(token="t2", payload="p").transition(TaskStatus.SUCCEEDED)
    logger.info("VERDICT basis=终态不可再流转、PENDING 不可直接 SUCCEEDED，均被拒绝")


# ---------- API 层 ----------

@pytest.fixture()
def client():
    svc = make_service(work_fn=CountingWork(duration=5.0))
    return TestClient(create_app(svc))


def test_api_submit_dedup_and_conflict(client):
    r1 = client.post("/tasks", json={"token": "api-tok", "payload": "{}"})
    assert r1.status_code == 201
    r2 = client.post("/tasks", json={"token": "api-tok", "payload": "{}"})
    assert r2.status_code == 200 and r2.json()["deduplicated"] is True
    assert r2.json()["id"] == r1.json()["id"]
    r3 = client.post("/tasks", json={"token": "api-tok", "payload": "other"})
    assert r3.status_code == 409
    assert r3.json()["error"]["code"] == "TOKEN_CONFLICT"
    logger.info("VERDICT token=api-tok basis=API 层幂等 200/201 与冲突 409 码可区分")


def test_api_get_and_cancel(client):
    r = client.post("/tasks", json={"token": "api-cancel", "payload": '{"duration_ms": 5000}'})
    tid = r.json()["id"]
    rc = client.post(f"/tasks/{tid}/cancel")
    assert rc.status_code == 200
    assert rc.json()["status"] in ("CANCELLED", "PENDING", "RUNNING")
    rg = client.get(f"/tasks/{tid}")
    assert rg.json()["status"] == "CANCELLED"
    rc2 = client.post(f"/tasks/{tid}/cancel")
    assert rc2.status_code == 409
    assert rc2.json()["error"]["code"] == "INVALID_STATE"
    assert client.get("/tasks/missing").status_code == 404
    logger.info("VERDICT token=api-cancel basis=取消后查询为 CANCELLED，重复取消 409")
