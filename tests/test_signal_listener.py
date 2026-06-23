"""Unit tests for the inbound SSE listener.

Drives :func:`run_listener` against an ``httpx.MockTransport`` standing in for the
signal-cli daemon's event stream — no live daemon or network. Backoff constants are
shrunk so the reconnect path runs fast.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from bt_signal_gateway import signal_listener
from bt_signal_gateway.config import Settings
from bt_signal_gateway.envelope import InboundMessage

ACCOUNT = "+15551234567"
PEER = "+15559998888"
PEER_UUID = "11111111-2222-3333-4444-555555555555"


def _settings() -> Settings:
    return Settings(
        _env_file=None,  # type: ignore  # pydantic-settings runtime kwarg
        signal_account=ACCOUNT,
        engine_base_url="https://api.btservant.ai",
        engine_api_key="secret",
        gateway_public_url="https://gw.fly.dev",
    )


def _dm_envelope(message: str = "hello") -> dict[str, Any]:
    return {
        "envelope": {
            "sourceNumber": PEER,
            "sourceUuid": PEER_UUID,
            "sourceName": "Peer",
            "timestamp": 0,  # 0 disables the age-cutoff check
            "dataMessage": {"message": message},
        }
    }


def _sse_body(*envelopes: dict[str, Any]) -> bytes:
    """Encode envelopes as an SSE ``data:`` stream (with a keepalive comment)."""
    lines = [": keepalive"]
    lines += [f"data: {json.dumps(env)}" for env in envelopes]
    return ("\n".join(lines) + "\n").encode()


@pytest.fixture(autouse=True)
def _fast_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shrink reconnect delays so the reconnect test doesn't actually wait."""
    monkeypatch.setattr(signal_listener, "SSE_RETRY_DELAY_INITIAL", 0.01)
    monkeypatch.setattr(signal_listener, "SSE_RETRY_DELAY_MAX", 0.01)
    monkeypatch.setattr(signal_listener, "SSE_RETRY_JITTER", 0.0)


async def _run_until(coro_fn: Any, *, received: asyncio.Event, client: httpx.AsyncClient) -> None:
    """Start the listener, wait for the handler signal, then cancel cleanly."""
    task = asyncio.create_task(coro_fn())
    try:
        await asyncio.wait_for(received.wait(), timeout=2.0)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_accepted_message_reaches_handler() -> None:
    received: list[InboundMessage] = []
    done = asyncio.Event()

    async def handler(msg: InboundMessage) -> None:
        received.append(msg)
        done.set()

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/events"
        return httpx.Response(200, content=_sse_body(_dm_envelope("hi from peer")))

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))

    await _run_until(
        lambda: signal_listener.run_listener(_settings(), handler=handler, client=client),
        received=done,
        client=client,
    )
    await client.aclose()

    assert len(received) == 1
    assert received[0].text == "hi from peer"
    assert received[0].user_id == PEER_UUID


@pytest.mark.asyncio
async def test_filtered_envelope_is_not_dispatched() -> None:
    """A receipt envelope must not reach the handler, but the stream stays alive."""
    handled: list[InboundMessage] = []
    second_seen = asyncio.Event()

    async def handler(msg: InboundMessage) -> None:
        handled.append(msg)
        second_seen.set()

    receipt = {"envelope": {"sourceUuid": PEER_UUID, "timestamp": 0, "receiptMessage": {}}}

    def respond(request: httpx.Request) -> httpx.Response:
        # Receipt first (dropped), then a real message — both in one stream.
        return httpx.Response(200, content=_sse_body(receipt, _dm_envelope("real")))

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))

    await _run_until(
        lambda: signal_listener.run_listener(_settings(), handler=handler, client=client),
        received=second_seen,
        client=client,
    )
    await client.aclose()

    assert [m.text for m in handled] == ["real"]


@pytest.mark.asyncio
async def test_reconnects_after_stream_drop() -> None:
    """First connect ends immediately; the listener must reconnect and deliver."""
    connects = 0
    done = asyncio.Event()
    received: list[InboundMessage] = []

    async def handler(msg: InboundMessage) -> None:
        received.append(msg)
        done.set()

    async def _empty() -> AsyncIterator[bytes]:
        # A stream that yields nothing and ends → triggers a reconnect.
        return
        yield b""  # pragma: no cover — makes this an async generator

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal connects
        connects += 1
        if connects == 1:
            return httpx.Response(200, content=_empty())
        return httpx.Response(200, content=_sse_body(_dm_envelope("after reconnect")))

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))

    await _run_until(
        lambda: signal_listener.run_listener(_settings(), handler=handler, client=client),
        received=done,
        client=client,
    )
    await client.aclose()

    assert connects >= 2
    assert [m.text for m in received] == ["after reconnect"]


@pytest.mark.asyncio
async def test_health_monitor_wakes_backoff_after_daemon_refuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mimics a deploy: the daemon refuses connections, so the listener enters a *long*
    backoff. The only thing that can rescue it quickly is the health monitor probing
    ``/api/v1/check`` and waking the backoff — so this deterministically covers both
    ``_daemon_reachable`` (tolerating a non-2xx ``/check``) and the ``_wakeup`` path.

    Backoff is forced long and the monitor interval short: if ``_wakeup.set()`` or
    ``_daemon_reachable()`` were broken, the listener would sit in the 30s sleep and
    ``_run_until``'s 2s timeout would fail the test."""
    # Override the autouse _fast_backoff fixture: make the backoff far longer than the
    # monitor interval, so a prompt reconnect can only come from the wakeup.
    monkeypatch.setattr(signal_listener, "SSE_RETRY_DELAY_INITIAL", 30.0)
    monkeypatch.setattr(signal_listener, "SSE_RETRY_DELAY_MAX", 30.0)
    monkeypatch.setattr(signal_listener, "HEALTH_CHECK_INTERVAL", 0.02)

    events_attempts = 0
    check_hits = 0
    done = asyncio.Event()
    received: list[InboundMessage] = []

    async def handler(msg: InboundMessage) -> None:
        received.append(msg)
        done.set()

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal events_attempts, check_hits
        if request.url.path == "/api/v1/check":
            # Server up but account not yet connected → non-2xx, still "reachable".
            check_hits += 1
            return httpx.Response(404)
        events_attempts += 1
        if events_attempts == 1:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(200, content=_sse_body(_dm_envelope("back online")))

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))

    await _run_until(
        lambda: signal_listener.run_listener(_settings(), handler=handler, client=client),
        received=done,
        client=client,
    )
    await client.aclose()

    assert events_attempts >= 2  # 1 refused + the wakeup-driven success
    assert check_hits > 0  # the health monitor actually probed /api/v1/check
    assert [m.text for m in received] == ["back online"]


@pytest.mark.asyncio
async def test_cancellation_is_clean() -> None:
    """Cancelling the listener task propagates CancelledError, not a swallow."""

    async def handler(msg: InboundMessage) -> None:  # pragma: no cover — never called
        pass

    async def _hang() -> AsyncIterator[bytes]:
        await asyncio.Event().wait()  # never completes
        yield b""  # pragma: no cover

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=_hang())

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    task = asyncio.create_task(
        signal_listener.run_listener(_settings(), handler=handler, client=client)
    )
    await asyncio.sleep(0.05)  # let it connect and start streaming
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await client.aclose()
