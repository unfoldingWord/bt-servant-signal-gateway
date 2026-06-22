"""HTTP server the worker calls back into.

Exposes ``GET /health`` and ``POST /progress-callback`` — the worker -> gateway
reply path. The callback handler authenticates the worker (``X-Engine-Token``
against ``ENGINE_API_KEY``), parses the payload, dedups ``complete`` events on
``message_key``, then schedules delivery to Signal as a background task so the
worker's ack returns immediately (the worker's webhook post is non-blocking and
short-timeout; we must not hold it open while sending to signal-cli).

Dependencies (``signal_client``, ``settings``, ``dedup``) are injected into
:func:`create_app` and stashed on ``app.state``. The module-level ``app`` keeps
the no-arg form working for the ``/health`` smoke test; the entrypoint
(``app.py``) constructs the app with real dependencies.
"""

from __future__ import annotations

import logging

from fastapi import BackgroundTasks, FastAPI, Request, Response

from bt_signal_gateway.config import Settings, get_settings
from bt_signal_gateway.dedup import CompletedKeys
from bt_signal_gateway.dispatch import dispatch_callback, parse_callback_payload
from bt_signal_gateway.signal_client import SignalClient

logger = logging.getLogger(__name__)

SERVICE_NAME = "bt-servant-signal-gateway"

#: TTL for the ``complete`` dedup cache. Generous relative to a request's
#: lifetime; only needs to outlive any worker re-delivery window.
_DEDUP_TTL_MS = 10 * 60 * 1000.0


def create_app(
    *,
    signal_client: SignalClient | None = None,
    settings: Settings | None = None,
    dedup: CompletedKeys | None = None,
) -> FastAPI:
    """Build the FastAPI application, optionally with injected dependencies.

    When omitted, ``dedup`` defaults to a fresh cache and ``settings`` is
    resolved lazily per request via :func:`get_settings`. ``signal_client`` is
    required to actually deliver replies; if it is absent, ``/progress-callback``
    authenticates and acks but cannot send (logged + reported as unavailable).
    """
    app = FastAPI(title=SERVICE_NAME)
    app.state.signal_client = signal_client
    app.state.settings = settings
    app.state.dedup = dedup or CompletedKeys(ttl_ms=_DEDUP_TTL_MS)

    @app.get("/health")
    async def health() -> dict[str, object]:
        return {"ok": True, "service": SERVICE_NAME}

    @app.post("/progress-callback")
    async def progress_callback(request: Request, background: BackgroundTasks) -> Response:
        settings = app.state.settings or get_settings()

        token = request.headers.get("X-Engine-Token")
        if token != settings.engine_api_key:
            logger.warning("callback: rejected (bad X-Engine-Token)")
            return Response(status_code=401)

        try:
            body = await request.json()
        except ValueError:
            logger.warning("callback: rejected (malformed JSON body)")
            return Response(status_code=400)

        payload = parse_callback_payload(body)
        if payload is None:
            keys = list(body.keys()) if isinstance(body, dict) else None
            logger.warning("callback: rejected (unrecognized payload)", extra={"body_keys": keys})
            return Response(status_code=400)

        # status/progress carry no terminal reply for Signal — ack and drop.
        if payload.type not in ("complete", "error"):
            return Response(status_code=200)

        if payload.type == "complete":
            dedup: CompletedKeys = app.state.dedup
            if dedup.is_completed(payload.message_key):
                logger.info(
                    "callback: duplicate complete ignored",
                    extra={"message_key": payload.message_key, "user_id": payload.user_id},
                )
                return Response(status_code=200)
            dedup.mark_completed(payload.message_key)

        signal_client: SignalClient | None = app.state.signal_client
        if signal_client is None:
            logger.error(
                "callback: no signal client configured; cannot deliver",
                extra={"message_key": payload.message_key, "type": payload.type},
            )
            return Response(status_code=503)

        # Deliver off the ack path so the worker's webhook post returns fast.
        background.add_task(dispatch_callback, payload, signal_client, settings)
        return Response(status_code=200)

    return app


app = create_app()
