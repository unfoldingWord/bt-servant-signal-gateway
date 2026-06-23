"""Inbound SSE listener against the signal-cli daemon.

Holds a long-lived connection to ``GET {SIGNAL_HTTP_URL}/api/v1/events`` (the
signal-cli Server-Sent-Events stream), parses each ``data:`` line into a
normalized :class:`~bt_signal_gateway.envelope.InboundMessage`, and hands accepted
messages to an async *handler*. The handler that relays to the worker lives in
``engine_client`` (a later issue); this module only owns the transport.

Resilience, ported and trimmed from ``../hermes-agent/gateway/platforms/signal.py``
(``_sse_listener`` / ``_health_monitor``):

- auto-reconnect with exponential backoff (2s→5s) + jitter, reset on each
  successful connect; the backoff sleep is interruptible so the health monitor
  can cut it short the moment the daemon is reachable again;
- a health monitor that, between connection attempts, probes ``/api/v1/check`` and
  wakes the listener as soon as the daemon is back (no waiting out the backoff), and
  while a stream is live forces a reconnect if the daemon goes quiet and unreachable.

``run_listener`` preserves a clean :class:`asyncio.CancelledError` contract so the
app entrypoint can cancel it as part of an orderly shutdown.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections.abc import Awaitable, Callable
from contextlib import suppress
from urllib.parse import quote

import httpx

from bt_signal_gateway.config import Settings
from bt_signal_gateway.envelope import InboundMessage, parse_envelope
from bt_signal_gateway.signal_client import SignalClient

logger = logging.getLogger(__name__)

#: Coroutine called once per accepted inbound message.
InboundHandler = Callable[[InboundMessage], Awaitable[None]]

# signal-cli runs on loopback in the same container, so a long backoff cap only
# delays recovery after a daemon bounce — keep it small.
SSE_RETRY_DELAY_INITIAL = 2.0
SSE_RETRY_DELAY_MAX = 5.0
SSE_RETRY_JITTER = 0.2
HEALTH_CHECK_INTERVAL = 5.0
HEALTH_CHECK_STALE_THRESHOLD = 30.0


class _Listener:
    """Owns the SSE connection lifecycle and its health monitor."""

    def __init__(
        self,
        settings: Settings,
        *,
        handler: InboundHandler,
        signal_client: SignalClient | None,
        client: httpx.AsyncClient | None,
    ) -> None:
        self._settings = settings
        self._handler = handler
        self._signal_client = signal_client
        # When a client is injected (tests / shared client) we don't own its
        # lifecycle and must not close it.
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=None)

        http_url = settings.signal_http_url.rstrip("/")
        account = quote(settings.signal_account, safe="")
        self._events_url = f"{http_url}/api/v1/events?account={account}"
        self._check_url = f"{http_url}/api/v1/check"

        self._last_activity = time.monotonic()
        self._response: httpx.Response | None = None
        self._backoff = SSE_RETRY_DELAY_INITIAL
        # Set by the health monitor to cut a backoff sleep short once the daemon
        # is reachable again, so we don't wait out the full delay after a restart.
        self._wakeup = asyncio.Event()

    async def run(self) -> None:
        """Stream forever, with a sidecar health monitor, until cancelled."""
        monitor = asyncio.create_task(self._health_monitor(), name="signal-health-monitor")
        try:
            await self._consume_forever()
        finally:
            monitor.cancel()
            with suppress(asyncio.CancelledError):
                await monitor
            if self._owns_client:
                await self._client.aclose()

    async def _consume_forever(self) -> None:
        while True:
            try:
                await self._stream_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # any stream error → reconnect
                logger.warning("signal sse: stream error", extra={"error": str(exc)})
            delay = self._backoff + self._backoff * SSE_RETRY_JITTER * random.random()
            logger.debug("signal sse: reconnecting", extra={"delay_s": round(delay, 1)})
            # Interruptible sleep: the health monitor sets _wakeup once the daemon
            # is reachable, so a daemon bounce doesn't cost the full backoff delay.
            self._wakeup.clear()
            with suppress(TimeoutError):
                await asyncio.wait_for(self._wakeup.wait(), timeout=delay)
            self._backoff = min(self._backoff * 2, SSE_RETRY_DELAY_MAX)

    async def _stream_once(self) -> None:
        logger.debug("signal sse: connecting")
        async with self._client.stream(
            "GET",
            self._events_url,
            headers={"Accept": "text/event-stream"},
            timeout=None,
        ) as response:
            response.raise_for_status()
            self._response = response
            self._last_activity = time.monotonic()
            self._backoff = SSE_RETRY_DELAY_INITIAL  # healthy connect → reset
            logger.info("signal sse: connected")
            try:
                async for line in response.aiter_lines():
                    self._last_activity = time.monotonic()
                    await self._handle_line(line)
            finally:
                self._response = None

    async def _handle_line(self, line: str) -> None:
        line = line.strip()
        # Blank lines and ``:`` keepalive comments only refresh liveness.
        if not line or line.startswith(":"):
            return
        if not line.startswith("data:"):
            return
        data_str = line[len("data:") :].strip()
        if not data_str:
            return
        try:
            raw = json.loads(data_str)
        except json.JSONDecodeError:
            logger.debug("signal sse: invalid json", extra={"snippet": data_str[:100]})
            return
        try:
            await self._dispatch(raw)
        except Exception:  # one bad envelope must not kill the loop
            logger.exception("signal sse: error handling envelope")

    async def _dispatch(self, raw: dict) -> None:
        message = parse_envelope(raw, self._settings)
        if message is None:
            return
        if self._signal_client is not None:
            # Feed the outbound client's number<->uuid cache from live traffic.
            self._signal_client.remember_identifiers(message.source_number, message.source_uuid)
        await self._handler(message)

    async def _health_monitor(self) -> None:
        """Drive reconnection: wake a backoff once the daemon is back; reconnect a
        stalled live stream if the daemon goes quiet and unreachable."""
        while True:
            await asyncio.sleep(HEALTH_CHECK_INTERVAL)
            if self._response is None:
                # Between attempts (in backoff): if the daemon answers, retry now
                # instead of waiting out the remaining backoff.
                if await self._daemon_reachable():
                    self._wakeup.set()
                continue
            idle = time.monotonic() - self._last_activity
            if idle < HEALTH_CHECK_STALE_THRESHOLD:
                continue
            logger.warning("signal sse: idle, pinging daemon", extra={"idle_s": round(idle)})
            if await self._daemon_reachable():
                # Daemon alive but quiet — reset so we don't ping in a tight loop.
                self._last_activity = time.monotonic()
            else:
                await self._force_reconnect()

    async def _daemon_reachable(self) -> bool:
        """Whether the daemon's HTTP server answers at all.

        Any completed response — even a non-2xx — means the server is up enough to
        attempt the event stream; only a transport/connection error counts as down.
        """
        try:
            await self._client.get(self._check_url, timeout=5.0)
        except Exception as exc:
            logger.warning("signal: health check error", extra={"error": str(exc)})
            return False
        return True

    async def _force_reconnect(self) -> None:
        """Break the active stream so :meth:`_consume_forever` reconnects."""
        response = self._response
        if response is not None:
            with suppress(Exception):
                await response.aclose()


async def run_listener(
    settings: Settings,
    *,
    handler: InboundHandler,
    signal_client: SignalClient | None = None,
    client: httpx.AsyncClient | None = None,
) -> None:
    """Run the inbound SSE listener until cancelled.

    ``handler`` is awaited once per accepted :class:`InboundMessage`. ``signal_client``
    (optional) has its identifier cache fed from inbound traffic. ``client`` (optional)
    injects an :class:`httpx.AsyncClient` for tests; when omitted the listener owns one.
    """
    logger.info("signal listener started", extra={"account": settings.signal_account})
    listener = _Listener(settings, handler=handler, signal_client=signal_client, client=client)
    try:
        await listener.run()
    except asyncio.CancelledError:
        logger.info("signal listener stopped")
        raise
