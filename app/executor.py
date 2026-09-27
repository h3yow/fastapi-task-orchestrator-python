"""受限并发执行器：固定工作线程 + 有界队列。

执行器只做通用调度，不感知任务状态机；任务体由服务层注入，
排队容量有界，满时非阻塞拒绝，保证不无限堆积。
"""
from __future__ import annotations

import queue
import threading
from collections.abc import Callable
from dataclasses import dataclass

from app.errors import QueueFullError
from app.models import Task


@dataclass
class _WorkItem:
    task: Task
    fn: Callable[[Task], None]


class Executor:
    def __init__(self, max_workers: int = 2, max_queue: int = 8) -> None:
        self._max_workers = max_workers
        self._queue: queue.Queue[_WorkItem | None] = queue.Queue(maxsize=max_queue)
        self._threads: list[threading.Thread] = []
        self._started = False
        self._lock = threading.Lock()

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
            for i in range(self._max_workers):
                t = threading.Thread(
                    target=self._worker, name=f"executor-{i}", daemon=True
                )
                t.start()
                self._threads.append(t)

    def submit(self, task: Task, fn: Callable[[Task], None]) -> None:
        """非阻塞提交；队列满时抛 QueueFullError，调用方负责回滚任务状态。"""
        self.start()
        try:
            self._queue.put_nowait(_WorkItem(task=task, fn=fn))
        except queue.Full:
            raise QueueFullError(
                f"queue full (capacity={self._queue.maxsize}), task rejected"
            ) from None

    def _worker(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:  # 关闭信号
                return
            try:
                item.fn(item.task)
            except Exception:  # 任务体内部应自行捕获，这里兜底防线程退出
                pass

    def shutdown(self) -> None:
        for _ in self._threads:
            self._queue.put(None)
        for t in self._threads:
            t.join(timeout=5)
