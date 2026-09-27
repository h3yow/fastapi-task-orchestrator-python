"""Shared pytest fixtures: small, deterministic service instances."""

from __future__ import annotations

import logging
import os
import sys

import pytest

# Make every test run print token/status/verdict logs to stdout; ``pytest -s``
# shows them in real time, otherwise they appear after the run.
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(name)s [%(threadName)s] %(message)s",
    force=True,
)

from app.api import create_app
from app.service import TaskService
from fastapi.testclient import TestClient


@pytest.fixture
def service_factory():
    started: list[TaskService] = []

    def _make(max_workers: int = 2, max_queue_size: int = 100) -> TaskService:
        svc = TaskService(max_workers=max_workers, max_queue_size=max_queue_size)
        svc.start()
        started.append(svc)
        return svc

    yield _make

    for svc in started:
        svc.shutdown()


@pytest.fixture
def client_factory(service_factory):
    def _make(max_workers: int = 2, max_queue_size: int = 100):
        svc = service_factory(max_workers=max_workers,
                              max_queue_size=max_queue_size)
        return TestClient(create_app(svc)), svc

    return _make
