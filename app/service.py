"""编排服务：组合存储与执行器，实现提交幂等、排队、取消与失败分类。

取消与完成竞态的确定性保证：
- 所有状态流转都经 TaskStore.transition 的同一把锁；
- 运行中任务的终态（SUCCEEDED/FAILED/CANCELLED）只由工作线程写入，
  cancel() 对运行中任务也走同一把锁竞争 CANCELLED 流转，
  谁先拿到锁谁决定终态，因此只会产生一个确定结果。
"""
from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable

from app.errors import InvalidStateError, QueueFullError
from app.executor import Executor
from app.models import TERMINAL_STATES, IllegalTransitionError, Task, TaskStatus
from app.store import TaskStore

logger = logging.getLogger("orchestrator")


class TaskCancelled(Exception):
    """任务体协作式取消信号。"""


class TaskExecutionError(Exception):
    """任务执行失败，kind 用于对外分类失败原因。"""

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


def default_work(task: Task) -> str:
    """默认可执行任务体：payload 为 JSON，支持 duration_ms / fail 便于本地验证。"""
    try:
        spec = json.loads(task.payload) if task.payload else {}
    except json.JSONDecodeError as exc:
        raise TaskExecutionError("PAYLOAD_ERROR", f"invalid json payload: {exc}") from exc
    if not isinstance(spec, dict):
        raise TaskExecutionError("PAYLOAD_ERROR", "payload must be a JSON object")

    duration_ms = float(spec.get("duration_ms", 0))
    deadline = time.monotonic() + duration_ms / 1000.0
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        if task.cancel_event.is_set():
            raise TaskCancelled()
        time.sleep(min(0.005, remaining))

    if spec.get("fail"):
        raise TaskExecutionError("TASK_ERROR", "payload requested failure")
    return f"done:{task.token}"


class TaskService:
    def __init__(
        self,
        store: TaskStore,
        executor: Executor,
        work_fn: Callable[[Task], str] = default_work,
    ) -> None:
        self._store = store
        self._executor = executor
        self._work_fn = work_fn

    def submit(self, token: str, payload: str) -> tuple[Task, bool]:
        """提交任务。同令牌同内容幂等返回；同令牌不同内容抛 TokenConflictError。

        队列满时任务被记录为 FAILED/QUEUE_FULL（原因可查询、可区分），
        同时向调用方抛 QueueFullError。
        """
        task, created = self._store.get_or_create(token, payload)
        if not created:
            logger.info("submit dedup token=%s task=%s status=%s",
                        token, task.id, task.status.value)
            return task, False
        try:
            self._executor.submit(task, self._run)
        except QueueFullError:
            self._store.transition(
                task.id, TaskStatus.FAILED,
                error="queue full, task rejected",
                error_kind=QueueFullError.code,
            )
            raise
        logger.info("submit accepted token=%s task=%s", token, task.id)
        return task, True

    def get(self, task_id: str) -> Task:
        return self._store.get(task_id)

    def cancel(self, task_id: str) -> Task:
        """取消任务。

        - 排队中（PENDING）：直接流转 CANCELLED，工作线程取出时会跳过；
        - 运行中（RUNNING）：先置位取消事件（协作式中断任务体），
          再经锁竞争 CANCELLED 流转，与正常完成只会有一个获胜；
        - 已终态：抛 InvalidStateError。
        """
        task = self._store.get(task_id)
        if task.status in TERMINAL_STATES:
            raise InvalidStateError(
                f"task {task_id} already terminal: {task.status.value}"
            )
        task.cancel_event.set()
        try:
            return self._store.transition(task_id, TaskStatus.CANCELLED)
        except IllegalTransitionError:
            # 与工作线程的终态流转竞态失败：任务已到达终态，返回确定结果
            return self._store.get(task_id)

    # ---- 工作线程侧 ----

    def _run(self, task: Task) -> None:
        try:
            self._store.transition(task.id, TaskStatus.RUNNING)
        except IllegalTransitionError:
            # 排队期间被取消，不再执行
            logger.info("skip cancelled task=%s token=%s", task.id, task.token)
            return
        try:
            result = self._work_fn(task)
        except TaskCancelled:
            self._finish_cancelled(task)
        except TaskExecutionError as exc:
            self._finish(task, TaskStatus.FAILED, error=str(exc), error_kind=exc.kind)
        except Exception as exc:  # 未预期异常统一归类
            self._finish(task, TaskStatus.FAILED, error=str(exc), error_kind="INTERNAL")
        else:
            self._finish(task, TaskStatus.SUCCEEDED, result=result)

    def _finish_cancelled(self, task: Task) -> None:
        try:
            self._store.transition(task.id, TaskStatus.CANCELLED)
        except IllegalTransitionError:
            pass  # cancel() 已完成流转，终态唯一

    def _finish(self, task: Task, target: TaskStatus, **fields: object) -> None:
        try:
            self._store.transition(task.id, target, **fields)
        except IllegalTransitionError:
            # 取消竞态失败：丢弃结果，不得再产出
            logger.info("finish lost race task=%s token=%s target=%s",
                        task.id, task.token, target.value)
