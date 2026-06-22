"""Unit tests for callback parsing and reply dispatch to Signal."""

from __future__ import annotations

from typing import cast

import pytest

from bt_signal_gateway.config import Settings
from bt_signal_gateway.dispatch import (
    DEFAULT_FALLBACK_MESSAGE,
    CallbackPayload,
    dispatch_callback,
    parse_callback_payload,
)
from bt_signal_gateway.signal_client import SignalClient

ACCOUNT = "+15551234567"
USER_ID = "11111111-2222-3333-4444-555555555555"
GROUP_CHAT_ID = "group:dGVzdGdyb3Vw"


def _settings(chunk_size: int = 1500) -> Settings:
    return Settings(
        _env_file=None,  # type: ignore  # pydantic-settings runtime kwarg, not a model field
        signal_account=ACCOUNT,
        engine_base_url="https://api.btservant.ai",
        engine_api_key="secret-token",
        gateway_public_url="https://gw.fly.dev",
        chunk_size=chunk_size,
    )


class _FakeSignalClient:
    """Records ``send`` calls; returns ``True`` (or a queued failure)."""

    def __init__(self, results: list[bool] | None = None) -> None:
        self.sends: list[tuple[str, str]] = []
        self._results = list(results) if results is not None else None

    async def send(
        self,
        chat_id: str,
        message: str,
        attachments: list[str] | None = None,
        text_styles: list[str] | None = None,
    ) -> bool:
        self.sends.append((chat_id, message))
        if self._results is None:
            return True
        return self._results.pop(0) if self._results else True


def _client(results: list[bool] | None = None) -> tuple[_FakeSignalClient, SignalClient]:
    fake = _FakeSignalClient(results)
    return fake, cast(SignalClient, fake)


# --- parse_callback_payload ---


def test_parse_accepts_complete() -> None:
    payload = parse_callback_payload(
        {"type": "complete", "user_id": USER_ID, "message_key": "k1", "text": "hi"}
    )
    assert payload == CallbackPayload(type="complete", user_id=USER_ID, message_key="k1", text="hi")


def test_parse_captures_group_chat_id() -> None:
    payload = parse_callback_payload(
        {"type": "complete", "user_id": USER_ID, "message_key": "k1", "chat_id": GROUP_CHAT_ID}
    )
    assert payload is not None
    assert payload.chat_id == GROUP_CHAT_ID


def test_parse_tolerates_media_fields() -> None:
    payload = parse_callback_payload(
        {
            "type": "complete",
            "user_id": USER_ID,
            "message_key": "k1",
            "text": "hi",
            "voice_audio_url": "https://x/a.ogg",
            "attachments": [{"type": "audio"}],
        }
    )
    assert payload is not None
    assert payload.text == "hi"


@pytest.mark.parametrize(
    "body",
    [
        None,
        "not a dict",
        {"user_id": USER_ID, "message_key": "k1"},  # missing type
        {"type": "bogus", "user_id": USER_ID, "message_key": "k1"},  # unknown type
        {"type": "complete", "message_key": "k1"},  # missing user_id
        {"type": "complete", "user_id": USER_ID},  # missing message_key
        {"type": "complete", "user_id": "", "message_key": "k1"},  # empty user_id
    ],
)
def test_parse_rejects_bad_bodies(body: object) -> None:
    assert parse_callback_payload(body) is None


# --- dispatch_callback ---


async def test_complete_dm_sends_chunks_to_user_id() -> None:
    fake, client = _client()
    payload = CallbackPayload(
        type="complete", user_id=USER_ID, message_key="k1", text="alpha beta gamma delta"
    )
    await dispatch_callback(payload, client, _settings(chunk_size=10))

    assert len(fake.sends) > 1  # text exceeds the 10-char limit
    assert all(recipient == USER_ID for recipient, _ in fake.sends)
    assert all(len(message) <= 10 for _, message in fake.sends)


async def test_complete_group_sends_to_chat_id() -> None:
    fake, client = _client()
    payload = CallbackPayload(
        type="complete",
        user_id=USER_ID,
        message_key="k1",
        text="hello group",
        chat_id=GROUP_CHAT_ID,
    )
    await dispatch_callback(payload, client, _settings())

    assert fake.sends == [(GROUP_CHAT_ID, "hello group")]


async def test_complete_with_blank_text_sends_nothing() -> None:
    fake, client = _client()
    payload = CallbackPayload(type="complete", user_id=USER_ID, message_key="k1", text="   ")
    await dispatch_callback(payload, client, _settings())
    assert fake.sends == []


async def test_error_sends_fallback_message() -> None:
    fake, client = _client()
    payload = CallbackPayload(type="error", user_id=USER_ID, message_key="k1", error="boom")
    await dispatch_callback(payload, client, _settings())
    assert fake.sends == [(USER_ID, DEFAULT_FALLBACK_MESSAGE)]


async def test_chunk_send_failure_does_not_abort_remaining_chunks() -> None:
    fake, client = _client(results=[False, True])
    payload = CallbackPayload(
        type="complete", user_id=USER_ID, message_key="k1", text="alpha beta gamma"
    )
    await dispatch_callback(payload, client, _settings(chunk_size=8))
    # First chunk "fails" but the dispatcher still attempts the rest.
    assert len(fake.sends) >= 2
