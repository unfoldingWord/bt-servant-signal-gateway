"""Unit tests for the engine client (worker ``/api/v1/chat/callback`` relay).

Drives :class:`EngineClient` against an ``httpx.MockTransport`` standing in for
bt-servant-worker — no live worker or network required.
"""

from __future__ import annotations

import json

import httpx
import pytest

from bt_signal_gateway import engine_client as engine_module
from bt_signal_gateway.config import Settings
from bt_signal_gateway.engine_client import EngineClient, build_chat_request
from bt_signal_gateway.envelope import InboundMessage
from bt_signal_gateway.media import InboundAudio

ACCOUNT = "+15551234567"
API_KEY = "secret-token"
ENGINE_BASE_URL = "https://api.btservant.ai"


def _settings() -> Settings:
    return Settings(
        _env_file=None,  # type: ignore  # pydantic-settings runtime kwarg, not a model field
        signal_account=ACCOUNT,
        engine_base_url=ENGINE_BASE_URL,
        engine_api_key=API_KEY,
        gateway_public_url="https://gw.fly.dev",
    )


def _dm_message() -> InboundMessage:
    return InboundMessage(
        user_id="11111111-2222-3333-4444-555555555555",
        chat_id="11111111-2222-3333-4444-555555555555",
        text="hello there",
        timestamp_ms=1700000000000,
        source_number="+15559998888",
        source_uuid="11111111-2222-3333-4444-555555555555",
        sender_name="Alice",
    )


def _group_message() -> InboundMessage:
    return InboundMessage(
        user_id="11111111-2222-3333-4444-555555555555",
        chat_id="group:dGVzdGdyb3Vw",
        text="hello group",
        timestamp_ms=1700000000001,
        is_group=True,
        source_number="+15559998888",
        source_uuid="11111111-2222-3333-4444-555555555555",
        sender_name="Alice",
        group_id="dGVzdGdyb3Vw",
        group_name="Test Group",
    )


class FakeWorker:
    """Records callback requests and replies from a queued response script."""

    def __init__(self, *responses: httpx.Response) -> None:
        self.requests: list[httpx.Request] = []
        self.bodies: list[dict] = []
        # Responses popped in order; the last one repeats once exhausted.
        self._responses = list(responses) or [httpx.Response(202, json={"status": "queued"})]

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        self.bodies.append(json.loads(request.content))
        if len(self._responses) > 1:
            return self._responses.pop(0)
        return self._responses[0]


def _client(worker: FakeWorker) -> EngineClient:
    transport = httpx.MockTransport(worker.handler)
    return EngineClient(_settings(), client=httpx.AsyncClient(transport=transport))


# ---------------------------------------------------------------------------
# Request shape
# ---------------------------------------------------------------------------


def test_build_chat_request_dm_shape() -> None:
    body = build_chat_request(_dm_message(), _settings())
    assert body == {
        "client_id": "signal-gateway",
        "user_id": "11111111-2222-3333-4444-555555555555",
        "message_type": "text",
        "message": "hello there",
        "message_key": "1700000000000",
        "progress_callback_url": "https://gw.fly.dev/progress-callback",
        "progress_mode": "complete",
        "org": "unfoldingWord",
    }
    # DMs carry no group-context fields.
    assert "chat_type" not in body
    assert "chat_id" not in body
    assert "speaker" not in body


def test_build_chat_request_group_shape() -> None:
    body = build_chat_request(_group_message(), _settings())
    assert body["chat_type"] == "group"
    assert body["chat_id"] == "group:dGVzdGdyb3Vw"
    assert body["speaker"] == "Alice"
    assert body["message_key"] == "1700000000001"


def test_build_chat_request_audio_shape() -> None:
    body = build_chat_request(
        _dm_message(),
        _settings(),
        audio=InboundAudio(audio_base64="QUJD", audio_format="aac"),
    )
    assert body["message_type"] == "audio"
    assert body["audio_base64"] == "QUJD"
    assert body["audio_format"] == "aac"
    # The text caption still rides along as the message body.
    assert body["message"] == "hello there"


def test_build_chat_request_text_omits_audio_fields() -> None:
    body = build_chat_request(_dm_message(), _settings())
    assert body["message_type"] == "text"
    assert "audio_base64" not in body
    assert "audio_format" not in body


async def test_submit_with_audio_sends_audio_request() -> None:
    worker = FakeWorker(httpx.Response(202, json={"status": "queued"}))
    client = _client(worker)
    try:
        ok = await client.submit(
            _dm_message(), audio=InboundAudio(audio_base64="QUJD", audio_format="ogg")
        )
        assert ok is True
    finally:
        await client.aclose()

    assert worker.bodies[0]["message_type"] == "audio"
    assert worker.bodies[0]["audio_base64"] == "QUJD"
    assert worker.bodies[0]["audio_format"] == "ogg"


def test_build_chat_request_group_omits_empty_speaker() -> None:
    message = InboundMessage(
        user_id="u",
        chat_id="group:g",
        text="hi",
        timestamp_ms=1,
        is_group=True,
        group_id="g",
        sender_name="",
    )
    body = build_chat_request(message, _settings())
    assert "speaker" not in body


# ---------------------------------------------------------------------------
# submit()
# ---------------------------------------------------------------------------


async def test_submit_sends_auth_header_and_acks() -> None:
    worker = FakeWorker(httpx.Response(202, json={"status": "queued"}))
    client = _client(worker)
    try:
        assert await client.submit(_dm_message()) is True
    finally:
        await client.aclose()

    assert len(worker.requests) == 1
    assert worker.requests[0].headers["Authorization"] == f"Bearer {API_KEY}"
    assert worker.requests[0].url.path == "/api/v1/chat/callback"
    assert worker.bodies[0]["client_id"] == "signal-gateway"


async def test_submit_retries_on_429_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    slept: list[float] = []

    async def _fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(engine_module.asyncio, "sleep", _fake_sleep)

    worker = FakeWorker(
        httpx.Response(
            429,
            json={"code": "CONCURRENT_REQUEST_REJECTED", "retry_after_ms": 2000},
            headers={"Retry-After": "5"},
        ),
        httpx.Response(202, json={"status": "queued"}),
    )
    client = _client(worker)
    try:
        assert await client.submit(_dm_message()) is True
    finally:
        await client.aclose()

    assert len(worker.requests) == 2
    # Body retry_after_ms (2000ms) wins over the Retry-After header (5s).
    assert slept == [2.0]


async def test_submit_honors_retry_after_header_when_no_body_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    slept: list[float] = []

    async def _fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(engine_module.asyncio, "sleep", _fake_sleep)

    worker = FakeWorker(
        httpx.Response(
            429, json={"code": "CONCURRENT_REQUEST_REJECTED"}, headers={"Retry-After": "3"}
        ),
        httpx.Response(202, json={"status": "queued"}),
    )
    client = _client(worker)
    try:
        assert await client.submit(_dm_message()) is True
    finally:
        await client.aclose()

    assert slept == [3.0]


async def test_submit_returns_false_when_429_retries_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(engine_module.asyncio, "sleep", _fake_sleep)

    rejected = httpx.Response(429, json={"retry_after_ms": 1000})
    worker = FakeWorker(rejected, rejected, rejected, rejected)
    client = _client(worker)
    try:
        assert await client.submit(_dm_message()) is False
    finally:
        await client.aclose()

    # _MAX_ATTEMPTS total POSTs, no more.
    assert len(worker.requests) == engine_module._MAX_ATTEMPTS


async def test_submit_returns_false_on_non_2xx_without_retry() -> None:
    worker = FakeWorker(httpx.Response(400, json={"error": "user_id is required"}))
    client = _client(worker)
    try:
        assert await client.submit(_dm_message()) is False
    finally:
        await client.aclose()

    assert len(worker.requests) == 1


async def test_submit_returns_false_on_transport_error() -> None:
    def _boom(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("worker unreachable")

    transport = httpx.MockTransport(_boom)
    client = EngineClient(_settings(), client=httpx.AsyncClient(transport=transport))
    try:
        assert await client.submit(_dm_message()) is False
    finally:
        await client.aclose()
