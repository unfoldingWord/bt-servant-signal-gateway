"""HTTP server the worker calls back into.

Exposes ``GET /health`` and ``POST /progress-callback`` — the worker -> gateway
reply path. The callback handler authenticates the worker (``X-Engine-Token``
against ``ENGINE_API_KEY``), parses the payload, then schedules delivery to
Signal as a background task so the worker's ack returns immediately (the
worker's webhook post is non-blocking and short-timeout; we must not hold it
open while sending to signal-cli).

``complete`` callbacks are deduped on ``message_key`` with a deliver-then-mark
discipline: a key is marked *completed* only after delivery fully succeeds, and
an in-flight set blocks concurrent duplicates while the first attempt is still
running. A failed/partial send leaves the key neither completed nor in-flight,
so a repeated callback is free to re-deliver the reply rather than being
silently dropped.

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
from bt_signal_gateway.dispatch import CallbackPayload, dispatch_callback, parse_callback_payload
from bt_signal_gateway.signal_client import SignalClient

logger = logging.getLogger(__name__)

SERVICE_NAME = "bt-servant-signal-gateway"

#: TTL for the ``complete`` dedup cache. Generous relative to a request's
#: lifetime; only needs to outlive any worker re-delivery window.
_DEDUP_TTL_MS = 10 * 60 * 1000.0


async def _deliver_and_mark(
    payload: CallbackPayload,
    signal_client: SignalClient,
    settings: Settings,
    dedup: CompletedKeys,
    in_flight: set[str],
) -> None:
    """Deliver a ``complete`` callback, marking its key done only on success.

    Marks ``message_key`` completed only when :func:`dispatch_callback` confirms
    full delivery; a failed/partial send (or a raised error) leaves the key
    unmarked so a repeated callback can retry. The in-flight reservation is
    always released so a later attempt isn't permanently blocked.
    """
    key = payload.message_key
    try:
        delivered = await dispatch_callback(payload, signal_client, settings)
    except Exception:
        logger.exception("callback: delivery raised; key left re-deliverable", extra={"key": key})
        delivered = False

    if delivered:
        dedup.mark_completed(key)
    else:
        logger.warning("callback: delivery incomplete; key left re-deliverable", extra={"key": key})
    in_flight.discard(key)


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
    # message_keys whose delivery is currently running (blocks concurrent dupes).
    app.state.in_flight = set()

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

        # status carries no text — ack and drop. (progress carries intermediate
        # text we relay as a new message; complete/error are terminal.)
        if payload.type == "status":
            return Response(status_code=200)

        signal_client: SignalClient | None = app.state.signal_client
        if signal_client is None:
            logger.error(
                "callback: no signal client configured; cannot deliver",
                extra={"message_key": payload.message_key, "type": payload.type},
            )
            return Response(status_code=503)

        # progress/error: fire-and-forget, not deduped. progress streams an
        # intermediate update as a new message; error sends the fallback. Only
        # the terminal `complete` is deduped (below).
        if payload.type in ("progress", "error"):
            background.add_task(dispatch_callback, payload, signal_client, settings)
            return Response(status_code=200)

        # complete: dedup on message_key, marking done only after delivery
        # succeeds (in _deliver_and_mark). is_completed covers finished replies;
        # in_flight covers one that's still being delivered.
        dedup: CompletedKeys = app.state.dedup
        in_flight: set[str] = app.state.in_flight
        key = payload.message_key
        if dedup.is_completed(key) or key in in_flight:
            logger.info(
                "callback: duplicate complete ignored",
                extra={"message_key": key, "user_id": payload.user_id},
            )
            return Response(status_code=200)

        # Reserve before scheduling so a concurrent duplicate can't slip past.
        in_flight.add(key)
        background.add_task(_deliver_and_mark, payload, signal_client, settings, dedup, in_flight)
        return Response(status_code=200)

    return app


app = create_app()
