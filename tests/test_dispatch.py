"""Unit tests for callback parsing and reply dispatch to Signal."""

from __future__ import annotations

import base64
from typing import cast

import httpx
import pytest

from bt_signal_gateway.config import Settings
from bt_signal_gateway.dispatch import (
    DEFAULT_FALLBACK_MESSAGE,
    CallbackPayload,
    dispatch_callback,
    parse_callback_payload,
)
from bt_signal_gateway.media import OutboundAttachment
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
    """Records ``send`` / media calls; returns ``True`` (or a queued failure)."""

    def __init__(
        self,
        results: list[bool] | None = None,
        *,
        voice_ok: bool = True,
        attachments_ok: bool = True,
        reaction_raises: bool = False,
    ) -> None:
        self.sends: list[tuple[str, str]] = []
        self.voice_notes: list[tuple[str, str]] = []
        self.attachment_sends: list[tuple[str, list[str]]] = []
        self.reactions: list[tuple[str, str, str, int]] = []
        self._results = list(results) if results is not None else None
        self._voice_ok = voice_ok
        self._attachments_ok = attachments_ok
        self._reaction_raises = reaction_raises

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

    async def send_voice_note(self, chat_id: str, file_path: str) -> bool:
        self.voice_notes.append((chat_id, file_path))
        return self._voice_ok

    async def send_attachments(
        self, chat_id: str, file_paths: list[str], message: str = ""
    ) -> bool:
        self.attachment_sends.append((chat_id, list(file_paths)))
        return self._attachments_ok

    async def send_reaction(
        self, chat_id: str, emoji: str, target_author: str, target_timestamp: int
    ) -> bool:
        if self._reaction_raises:
            raise RuntimeError("reaction rpc boom")
        self.reactions.append((chat_id, emoji, target_author, target_timestamp))
        return True


def _client(
    results: list[bool] | None = None,
    *,
    voice_ok: bool = True,
    attachments_ok: bool = True,
    reaction_raises: bool = False,
) -> tuple[_FakeSignalClient, SignalClient]:
    fake = _FakeSignalClient(
        results,
        voice_ok=voice_ok,
        attachments_ok=attachments_ok,
        reaction_raises=reaction_raises,
    )
    return fake, cast(SignalClient, fake)


def _http_client(routes: dict[str, httpx.Response]) -> httpx.AsyncClient:
    """A mock httpx client mapping ``url -> response`` for media downloads."""

    def handler(request: httpx.Request) -> httpx.Response:
        resp = routes.get(str(request.url))
        if resp is None:
            return httpx.Response(404)
        return httpx.Response(resp.status_code, content=resp.content)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


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


# --- dispatch_callback: progress streaming (issue #28) ---


async def test_progress_streams_text_as_new_message() -> None:
    fake, client = _client()
    payload = CallbackPayload(
        type="progress", user_id=USER_ID, message_key="1700000000000", text="working on it…"
    )
    await dispatch_callback(payload, client, _settings())
    assert fake.sends == [(USER_ID, "working on it…")]
    # progress is intermediate: no media, no terminal reaction.
    assert fake.voice_notes == []
    assert fake.reactions == []


async def test_progress_to_group_routes_to_chat_id() -> None:
    fake, client = _client()
    payload = CallbackPayload(
        type="progress",
        user_id=USER_ID,
        message_key="1700000000000",
        text="thinking",
        chat_id=GROUP_CHAT_ID,
    )
    await dispatch_callback(payload, client, _settings())
    assert fake.sends == [(GROUP_CHAT_ID, "thinking")]


async def test_progress_with_blank_text_sends_nothing() -> None:
    fake, client = _client()
    payload = CallbackPayload(
        type="progress", user_id=USER_ID, message_key="1700000000000", text="   "
    )
    await dispatch_callback(payload, client, _settings())
    assert fake.sends == []
    assert fake.reactions == []


# --- dispatch_callback: terminal reactions (issue #28) ---


async def test_complete_reacts_with_check() -> None:
    fake, client = _client()
    payload = CallbackPayload(
        type="complete", user_id=USER_ID, message_key="1700000000000", text="done"
    )
    await dispatch_callback(payload, client, _settings())
    assert fake.reactions == [(USER_ID, "✅", USER_ID, 1700000000000)]


async def test_complete_with_blank_text_still_reacts() -> None:
    fake, client = _client()
    payload = CallbackPayload(
        type="complete", user_id=USER_ID, message_key="1700000000000", text="   "
    )
    await dispatch_callback(payload, client, _settings())
    assert fake.sends == []
    assert fake.reactions == [(USER_ID, "✅", USER_ID, 1700000000000)]


async def test_error_reacts_with_cross_and_sends_fallback() -> None:
    fake, client = _client()
    payload = CallbackPayload(
        type="error", user_id=USER_ID, message_key="1700000000000", error="boom"
    )
    await dispatch_callback(payload, client, _settings())
    assert fake.sends == [(USER_ID, DEFAULT_FALLBACK_MESSAGE)]
    assert fake.reactions == [(USER_ID, "❌", USER_ID, 1700000000000)]


async def test_group_complete_reaction_targets_author_not_group() -> None:
    fake, client = _client()
    payload = CallbackPayload(
        type="complete",
        user_id=USER_ID,
        message_key="1700000000000",
        text="hi",
        chat_id=GROUP_CHAT_ID,
    )
    await dispatch_callback(payload, client, _settings())
    # Reaction routes to the group but targets the original author + timestamp.
    assert fake.reactions == [(GROUP_CHAT_ID, "✅", USER_ID, 1700000000000)]


async def test_non_numeric_message_key_skips_reaction() -> None:
    fake, client = _client()
    payload = CallbackPayload(type="complete", user_id=USER_ID, message_key="k1", text="hi")
    await dispatch_callback(payload, client, _settings())
    assert fake.sends == [(USER_ID, "hi")]
    assert fake.reactions == []  # int("k1") fails -> reaction skipped, reply unaffected


async def test_reaction_failure_does_not_change_delivery_result() -> None:
    _fake, client = _client(reaction_raises=True)
    payload = CallbackPayload(
        type="complete", user_id=USER_ID, message_key="1700000000000", text="hi"
    )
    # send_reaction raises, but the reply still counts as delivered.
    ok = await dispatch_callback(payload, client, _settings())
    assert ok is True


async def test_chunk_send_failure_does_not_abort_remaining_chunks() -> None:
    fake, client = _client(results=[False, True])
    payload = CallbackPayload(
        type="complete", user_id=USER_ID, message_key="k1", text="alpha beta gamma"
    )
    await dispatch_callback(payload, client, _settings(chunk_size=8))
    # First chunk "fails" but the dispatcher still attempts the rest.
    assert len(fake.sends) >= 2


# --- parse_callback_payload: media fields ---


def test_parse_populates_media_fields() -> None:
    payload = parse_callback_payload(
        {
            "type": "complete",
            "user_id": USER_ID,
            "message_key": "k1",
            "voice_audio_url": "https://w/reply.m4a",
            "voice_audio_base64": "QUJD",
            "attachments": [{"type": "pdf", "url": "https://w/a.pdf", "filename": "a.pdf"}],
        }
    )
    assert payload is not None
    assert payload.voice_audio_url == "https://w/reply.m4a"
    assert payload.voice_audio_base64 == "QUJD"
    assert payload.attachments == [OutboundAttachment(url="https://w/a.pdf", filename="a.pdf")]


# --- dispatch_callback: outbound media ---


async def test_complete_with_voice_url_sends_voice_note() -> None:
    fake, client = _client()
    url = "https://w/reply.m4a"
    payload = CallbackPayload(
        type="complete", user_id=USER_ID, message_key="k1", voice_audio_url=url
    )
    http = _http_client({url: httpx.Response(200, content=b"voice-bytes")})
    try:
        ok = await dispatch_callback(payload, client, _settings(), http_client=http)
    finally:
        await http.aclose()

    assert ok is True
    assert len(fake.voice_notes) == 1
    recipient, path = fake.voice_notes[0]
    assert recipient == USER_ID
    assert path.endswith("reply.m4a")
    assert fake.sends == []  # no text in this reply


async def test_voice_url_failure_falls_back_to_base64() -> None:
    fake, client = _client()
    url = "https://w/reply.m4a"
    payload = CallbackPayload(
        type="complete",
        user_id=USER_ID,
        message_key="k1",
        voice_audio_url=url,
        voice_audio_base64=base64.b64encode(b"fallback-bytes").decode(),
    )
    # URL 404s -> base64 path is used.
    http = _http_client({url: httpx.Response(404)})
    try:
        ok = await dispatch_callback(payload, client, _settings(), http_client=http)
    finally:
        await http.aclose()

    assert ok is True
    assert len(fake.voice_notes) == 1
    assert fake.voice_notes[0][1].endswith("voice.m4a")


async def test_complete_with_attachments_sends_them() -> None:
    fake, client = _client()
    payload = CallbackPayload(
        type="complete",
        user_id=USER_ID,
        message_key="k1",
        text="here are your files",
        attachments=[
            OutboundAttachment(url="https://w/a.pdf", filename="a.pdf"),
            OutboundAttachment(url="https://w/b.pdf", filename="b.pdf"),
        ],
    )
    http = _http_client(
        {
            "https://w/a.pdf": httpx.Response(200, content=b"pdf-a"),
            "https://w/b.pdf": httpx.Response(200, content=b"pdf-b"),
        }
    )
    try:
        ok = await dispatch_callback(payload, client, _settings(), http_client=http)
    finally:
        await http.aclose()

    assert ok is True
    assert fake.sends == [(USER_ID, "here are your files")]
    assert len(fake.attachment_sends) == 1
    recipient, paths = fake.attachment_sends[0]
    assert recipient == USER_ID
    assert [p.split("/")[-1] for p in paths] == ["a.pdf", "b.pdf"]


async def test_voice_send_failure_marks_incomplete() -> None:
    _fake, client = _client(voice_ok=False)
    url = "https://w/reply.m4a"
    payload = CallbackPayload(
        type="complete", user_id=USER_ID, message_key="k1", voice_audio_url=url
    )
    http = _http_client({url: httpx.Response(200, content=b"voice-bytes")})
    try:
        ok = await dispatch_callback(payload, client, _settings(), http_client=http)
    finally:
        await http.aclose()

    # send_voice_note returned False -> key stays re-deliverable.
    assert ok is False


async def test_attachment_download_failure_marks_incomplete() -> None:
    fake, client = _client()
    payload = CallbackPayload(
        type="complete",
        user_id=USER_ID,
        message_key="k1",
        attachments=[OutboundAttachment(url="https://w/missing.pdf", filename="m.pdf")],
    )
    http = _http_client({})  # every URL 404s
    try:
        ok = await dispatch_callback(payload, client, _settings(), http_client=http)
    finally:
        await http.aclose()

    assert ok is False
    assert fake.attachment_sends == []  # nothing downloaded -> nothing sent
