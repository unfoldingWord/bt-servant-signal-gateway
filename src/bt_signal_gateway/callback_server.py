"""HTTP server the worker calls back into.

For now this only exposes ``GET /health``. The ``POST /progress-callback``
endpoint (worker -> gateway reply delivery) lands in a later issue.
"""

from __future__ import annotations

from fastapi import FastAPI

SERVICE_NAME = "bt-servant-signal-gateway"


def create_app() -> FastAPI:
    """Build the FastAPI application."""
    app = FastAPI(title=SERVICE_NAME)

    @app.get("/health")
    async def health() -> dict[str, object]:
        return {"ok": True, "service": SERVICE_NAME}

    return app


app = create_app()
