import uvicorn
from fastapi import FastAPI

from app.api import create_app
from app.executor import Executor
from app.service import TaskService
from app.store import TaskStore


def build_app() -> FastAPI:
    store = TaskStore()
    executor = Executor(max_workers=2, max_queue=8)
    service = TaskService(store, executor)
    return create_app(service)


app = build_app()


@app.get("/")
def read_root():
    return {"Hello": "World"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
