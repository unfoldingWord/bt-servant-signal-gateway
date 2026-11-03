"""Tests for the async entrypoint's shutdown / failure handling."""

from __future__ import annotations

import asyncio
from typing import cast

import pytest
from fastapi.testclient import TestClient

from bt_signal_gateway import app as app_module
from bt_signal_gateway.callback_server import create_app
from bt_signal_gateway.config import Settings, get_settings
from bt_signal_gateway.engine_client import EngineClient
from bt_signal_gateway.envelope import AttachmentRef, InboundMessage
from bt_signal_gateway.media import InboundAudio
from bt_signal_gateway.signal_client import SignalClient
from bt_signal_gateway.typing_indicator import TypingIndicators

REQUIRED_ENV = {
    "SIGNAL_ACCOUNT": "+15551234567",
    "ENGINE_BASE_URL": "https://api.btservant.ai",
    "ENGINE_API_KEY": "secret-token",
    "GATEWAY_PUBLIC_URL": "https://gw.fly.dev",
}


@pytest.fixture
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    # Keep the test from clobbering pytest's logging config.
    monkeypatch.setattr(app_module, "configure_logging", lambda *a, **k: None)


def _settings() -> Settings:
    return Settings(
        _env_file=None,  # type: ignore  # pydantic-settings runtime kwarg, not a model field
        signal_account="+15551234567",
        engine_base_url="https://api.btservant.ai",
        engine_api_key="secret-token",
        gateway_public_url="https://gw.fly.dev",
    )


class _FakeEngine:
    def __init__(self, order: list[str] | None = None) -> None:
        self.calls: list[InboundAudio | None] = []
        self._order = order

    async def submit(self, _message: InboundMessage, *, audio: InboundAudio | None = None) -> bool:
        if self._order is not None:
            self._order.append("submit")
        self.calls.append(audio)
        return True


class _FakeSignal:
    def __init__(
        self,
        data: bytes | None = b"voice",
        *,
        reaction_raises: bool = False,
        typing_raises: bool = False,
        order: list[str] | None = None,
    ) -> None:
        self._data = data
        self._reaction_raises = reaction_raises
        self._typing_raises = typing_raises
        self._order = order
        self.reactions: list[tuple[str, str, str, int]] = []
        self.typing: list[tuple[str, bool]] = []

    async def get_attachment(self, _attachment_id: str) -> bytes | None:
        return self._data

    async def send_reaction(
        self, chat_id: str, emoji: str, target_author: str, target_timestamp: int
    ) -> bool:
        if self._order is not None:
            self._order.append("reaction")
        if self._reaction_raises:
            raise RuntimeError("reaction rpc boom")
        self.reactions.append((chat_id, emoji, target_author, target_timestamp))
        return True

    async def send_typing(self, chat_id: str, *, stop: bool = False) -> bool:
        if self._order is not None:
            self._order.append("typing")
        self.typing.append((chat_id, stop))
        if self._typing_raises:
            raise RuntimeError("typing rpc down")
        return True


def _message(attachments: list[AttachmentRef]) -> InboundMessage:
    return InboundMessage(
        user_id="u",
        chat_id="u",
        text="caption",
        timestamp_ms=1700000000000,
        attachments=attachments,
    )


async def test_inbound_handler_fetches_audio_attachment() -> None:
    engine = _FakeEngine()
    handler = app_module._make_inbound_handler(
        cast(EngineClient, engine), cast(SignalClient, _FakeSignal()), _settings()
    )
    await handler(_message([AttachmentRef(id="a", content_type="audio/aac")]))

    assert len(engine.calls) == 1
    audio = engine.calls[0]
    assert audio is not None and audio.audio_format == "aac"


async def test_inbound_handler_ignores_non_audio_attachment() -> None:
    engine = _FakeEngine()
    handler = app_module._make_inbound_handler(
        cast(EngineClient, engine), cast(SignalClient, _FakeSignal()), _settings()
    )
    await handler(_message([AttachmentRef(id="img", content_type="image/png")]))

    # Submitted as a plain text message — no audio fetched.
    assert engine.calls == [None]


async def test_inbound_handler_reacts_eyes_then_relays() -> None:
    # issue #28: a 👀 reaction acks receipt before relaying to the worker.
    engine = _FakeEngine()
    signal = _FakeSignal()
    handler = app_module._make_inbound_handler(
        cast(EngineClient, engine), cast(SignalClient, signal), _settings()
    )
    msg = _message([])
    await handler(msg)

    assert signal.reactions == [(msg.chat_id, "👀", msg.user_id, msg.timestamp_ms)]
    assert engine.calls == [None]  # still relayed


async def test_inbound_handler_shows_typing_before_relaying() -> None:
    order: list[str] = []
    engine = _FakeEngine(order)
    signal = _FakeSignal(order=order)
    handler = app_module._make_inbound_handler(
        cast(EngineClient, engine), cast(SignalClient, signal), _settings()
    )
    msg = _message([])
    await handler(msg)

    assert signal.typing == [(msg.chat_id, False)]
    assert engine.calls == [None]
    assert order.index("typing") < order.index("submit")


async def test_inbound_handler_shows_typing_in_a_group() -> None:
    engine = _FakeEngine()
    signal = _FakeSignal()
    handler = app_module._make_inbound_handler(
        cast(EngineClient, engine), cast(SignalClient, signal), _settings()
    )
    msg = InboundMessage(
        user_id="u",
        chat_id="group:abc",
        text="hello",
        timestamp_ms=1700000000000,
        is_group=True,
        group_id="abc",
    )
    await handler(msg)

    assert signal.typing == [("group:abc", False)]
    assert engine.calls == [None]


def test_inbound_and_status_share_the_typing_throttle() -> None:
    clock = {"now": 50.0}
    typing = TypingIndicators(now=lambda: clock["now"])
    engine = _FakeEngine()
    signal = _FakeSignal()
    handler = app_module._make_inbound_handler(
        cast(EngineClient, engine),
        cast(SignalClient, signal),
        _settings(),
        typing,
    )
    msg = _message([])

    async def _show_typing() -> None:
        await handler(msg)

    asyncio.run(_show_typing())

    app = create_app(signal_client=cast(SignalClient, signal), settings=_settings(), typing=typing)
    client = TestClient(app)
    body = {
        "type": "status",
        "user_id": msg.user_id,
        "message_key": str(msg.timestamp_ms),
        "text": "working",
    }
    headers = {"X-Engine-Token": "secret-token"}
    response = client.post("/progress-callback", json=body, headers=headers)
    assert response.status_code == 200
    assert signal.typing == [(msg.chat_id, False)]

    clock["now"] = 60.0
    response = client.post("/progress-callback", json=body, headers=headers)
    assert response.status_code == 200
    assert signal.typing == [(msg.chat_id, False), (msg.chat_id, False)]


async def test_inbound_handler_relays_even_if_typing_fails() -> None:
    engine = _FakeEngine()
    signal = _FakeSignal(typing_raises=True)
    handler = app_module._make_inbound_handler(
        cast(EngineClient, engine), cast(SignalClient, signal), _settings()
    )
    await handler(_message([]))

    assert engine.calls == [None]


async def test_inbound_handler_relays_even_if_reaction_fails() -> None:
    engine = _FakeEngine()
    signal = _FakeSignal(reaction_raises=True)
    handler = app_module._make_inbound_handler(
        cast(EngineClient, engine), cast(SignalClient, signal), _settings()
    )
    await handler(_message([]))

    # Reaction RPC raised, but the message is still relayed to the worker.
    assert engine.calls == [None]


async def test_run_propagates_core_task_failure(
    monkeypatch: pytest.MonkeyPatch, _env: None
) -> None:
    """A crashing core task must make run() raise, not exit cleanly."""

    async def boom(_settings: object, **_kwargs: object) -> None:
        raise RuntimeError("listener crashed")

    async def fake_serve(self: object) -> None:
        # Mimic uvicorn's own serve loop: return once should_exit is set
        # (graceful stop). noqa: emulating uvicorn's poll, not production code.
        while not getattr(self, "should_exit", False):  # noqa: ASYNC110
            await asyncio.sleep(0.01)

    monkeypatch.setattr(app_module, "run_listener", boom)
    monkeypatch.setattr(app_module._Server, "serve", fake_serve)

    # wait_for guards against a regression that would otherwise hang the suite.
    with pytest.raises(RuntimeError, match="listener crashed"):
        await asyncio.wait_for(app_module.run(), timeout=5)
