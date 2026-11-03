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
from bt_signal_gateway.dispatch import (
    CallbackPayload,
    chat_id_for,
    dispatch_callback,
    parse_callback_payload,
)
from bt_signal_gateway.signal_client import SignalClient
from bt_signal_gateway.typing_indicator import (
    TypingIndicators,
    send_typing_start,
    send_typing_stop,
)

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


async def _dispatch_then_stop_typing(
    payload: CallbackPayload,
    signal_client: SignalClient,
    settings: Settings,
    chat_id: str,
    *,
    stop: bool,
) -> None:
    """Deliver an ``error`` callback, then clear the typing indicator.

    The stop runs after the fallback message so a slow typing RPC cannot delay
    the reply. It runs even when delivery raises, and its own failure is logged
    inside :func:`send_typing_stop` rather than failing the callback.
    """
    try:
        await dispatch_callback(payload, signal_client, settings)
    finally:
        if stop:
            await send_typing_stop(signal_client, chat_id)


async def _deliver_and_mark_then_stop_typing(
    payload: CallbackPayload,
    signal_client: SignalClient,
    settings: Settings,
    dedup: CompletedKeys,
    in_flight: set[str],
    chat_id: str,
    *,
    stop: bool,
) -> None:
    """Deliver a ``complete`` callback, then clear the typing indicator."""
    try:
        await _deliver_and_mark(payload, signal_client, settings, dedup, in_flight)
    finally:
        if stop:
            await send_typing_stop(signal_client, chat_id)


def create_app(
    *,
    signal_client: SignalClient | None = None,
    settings: Settings | None = None,
    dedup: CompletedKeys | None = None,
    typing: TypingIndicators | None = None,
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
    app.state.typing = typing or TypingIndicators()

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

        # status carries no chat text. It refreshes Signal's native typing
        # indicator for the chat (group ``chat_id``, otherwise the DM
        # ``user_id``) and never becomes a message. progress still relays
        # intermediate text. complete/error are terminal, but only once we
        # know a client can deliver: marking the turn finished before that
        # check would answer 503 and still swallow a later status.
        typing: TypingIndicators = app.state.typing
        chat_id = chat_id_for(user_id=payload.user_id, chat_id=payload.chat_id)

        if payload.type == "status":
            signal_client: SignalClient | None = app.state.signal_client
            if signal_client is not None and typing.try_acquire(chat_id, payload.message_key):
                ticket = typing.current_ticket(chat_id)
                if ticket is not None:
                    background.add_task(send_typing_start, signal_client, typing, chat_id, ticket)
            return Response(status_code=200)

        signal_client = app.state.signal_client
        if signal_client is None:
            logger.error(
                "callback: no signal client configured; cannot deliver",
                extra={"message_key": payload.message_key, "type": payload.type},
            )
            return Response(status_code=503)

        # Mark the turn finished before the response returns, so a late status
        # cannot restart the indicator while delivery is still queued. This is
        # the first accepted complete/error, not a successful Signal send: a
        # retry may redeliver the text, but it must not bring typing back.
        stop_typing = payload.type in ("complete", "error") and typing.finish(
            chat_id, payload.message_key
        )

        # progress/error: fire-and-forget, not deduped. progress streams an
        # intermediate update as a new message; error sends the fallback. Only
        # the terminal `complete` is deduped (below). Typing stops after the
        # error fallback, not before, so the stop RPC cannot delay the message.
        if payload.type == "error":
            background.add_task(
                _dispatch_then_stop_typing,
                payload,
                signal_client,
                settings,
                chat_id,
                stop=stop_typing,
            )
            return Response(status_code=200)
        if payload.type == "progress":
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
        background.add_task(
            _deliver_and_mark_then_stop_typing,
            payload,
            signal_client,
            settings,
            dedup,
            in_flight,
            chat_id,
            stop=stop_typing,
        )
        return Response(status_code=200)

    return app


app = create_app()
