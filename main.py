"""Local entrypoint: ``python main.py`` or ``uv run uvicorn main:app``."""

from __future__ import annotations

import os

import uvicorn

from app.api import create_app

app = create_app()


if __name__ == "__main__":
    uvicorn.run(
        app,
        host=os.environ.get("HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", "8000")),
    )
