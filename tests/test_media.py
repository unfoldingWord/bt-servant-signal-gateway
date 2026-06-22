"""Unit tests for media handling (inbound audio + outbound media download).

Inbound audio is driven against a fake signal client; outbound downloads run
against an ``httpx.MockTransport`` — no live daemon, worker, or network.
"""

from __future__ import annotations

import base64
from collections.abc import Callable
from pathlib import Path
from typing import cast

import httpx

from bt_signal_gateway.config import Settings
from bt_signal_gateway.envelope import AttachmentRef
from bt_signal_gateway.media import (
    InboundAudio,
    OutboundAttachment,
    decode_base64_to_temp,
    download_to_temp,
    fetch_inbound_audio,
    is_audio,
    parse_outbound_attachments,
    select_inbound_audio,
    temp_workspace,
)
from bt_signal_gateway.signal_client import SignalClient

API_KEY = "secret-token"


def _settings() -> Settings:
    return Settings(
        _env_file=None,  # type: ignore  # pydantic-settings runtime kwarg, not a model field
        signal_account="+15551234567",
        engine_base_url="https://api.btservant.ai",
        engine_api_key=API_KEY,
        gateway_public_url="https://gw.fly.dev",
    )


class _FakeSignalClient:
    """Stands in for :class:`SignalClient.get_attachment`."""

    def __init__(self, data: bytes | None) -> None:
        self._data = data
        self.requested: list[str] = []

    async def get_attachment(self, attachment_id: str) -> bytes | None:
        self.requested.append(attachment_id)
        return self._data


def _signal(data: bytes | None) -> SignalClient:
    return cast(SignalClient, _FakeSignalClient(data))


def _mock_client(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# ---------------------------------------------------------------------------
# Inbound audio selection + encoding
# ---------------------------------------------------------------------------


def test_is_audio_by_content_type() -> None:
    assert is_audio(AttachmentRef(id="1", content_type="audio/aac")) is True
    assert is_audio(AttachmentRef(id="2", content_type="image/png")) is False


def test_is_audio_by_extension_when_no_content_type() -> None:
    assert is_audio(AttachmentRef(id="1", filename="note.m4a")) is True
    assert is_audio(AttachmentRef(id="2", filename="doc.pdf")) is False


def test_select_inbound_audio_picks_first_audio() -> None:
    refs = [
        AttachmentRef(id="img", content_type="image/png"),
        AttachmentRef(id="aud", content_type="audio/ogg"),
    ]
    chosen = select_inbound_audio(refs)
    assert chosen is not None and chosen.id == "aud"


def test_select_inbound_audio_none_when_no_audio() -> None:
    assert select_inbound_audio([AttachmentRef(id="x", content_type="image/png")]) is None


async def test_fetch_inbound_audio_encodes_and_maps_format() -> None:
    raw = b"\x00\x01voice"
    audio = await fetch_inbound_audio(AttachmentRef(id="a", content_type="audio/mp4"), _signal(raw))
    assert audio == InboundAudio(audio_base64=base64.b64encode(raw).decode(), audio_format="m4a")


async def test_fetch_inbound_audio_format_defaults_to_aac() -> None:
    audio = await fetch_inbound_audio(
        AttachmentRef(id="a", content_type="audio/unknown-codec"), _signal(b"x")
    )
    assert audio is not None and audio.audio_format == "aac"


async def test_fetch_inbound_audio_rejects_declared_oversize() -> None:
    big = AttachmentRef(id="a", content_type="audio/aac", size=99)
    assert await fetch_inbound_audio(big, _signal(b"x"), max_bytes=10) is None


async def test_fetch_inbound_audio_rejects_actual_oversize() -> None:
    ref = AttachmentRef(id="a", content_type="audio/aac")
    assert await fetch_inbound_audio(ref, _signal(b"0123456789AB"), max_bytes=5) is None


async def test_fetch_inbound_audio_none_when_no_bytes() -> None:
    ref = AttachmentRef(id="a", content_type="audio/aac")
    assert await fetch_inbound_audio(ref, _signal(None)) is None


# ---------------------------------------------------------------------------
# Outbound attachment parsing
# ---------------------------------------------------------------------------


def test_parse_outbound_attachments_filters_missing_url() -> None:
    raw = [
        {
            "type": "pdf",
            "url": "https://x/a.pdf",
            "filename": "a.pdf",
            "mime_type": "application/pdf",
            "size_bytes": 12,
        },
        {"type": "pdf", "filename": "no-url.pdf"},  # no url -> dropped
        "garbage",
    ]
    assert parse_outbound_attachments(raw) == [
        OutboundAttachment(
            url="https://x/a.pdf",
            filename="a.pdf",
            mime_type="application/pdf",
            size_bytes=12,
        )
    ]


def test_parse_outbound_attachments_non_list() -> None:
    assert parse_outbound_attachments(None) == []
    assert parse_outbound_attachments({"url": "x"}) == []


# ---------------------------------------------------------------------------
# download_to_temp
# ---------------------------------------------------------------------------


async def test_download_to_temp_writes_file_with_bearer(tmp_path: Path) -> None:
    seen: dict[str, str | None] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("Authorization")
        return httpx.Response(200, content=b"audio-bytes")

    client = _mock_client(handler)
    try:
        path = await download_to_temp(client, "https://w/v.m4a", tmp_path, _settings())
    finally:
        await client.aclose()

    assert path is not None
    assert path.read_bytes() == b"audio-bytes"
    assert path.name == "v.m4a"
    assert seen["auth"] == f"Bearer {API_KEY}"


async def test_download_to_temp_rejects_non_https(tmp_path: Path) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("must not hit the network for a non-HTTPS URL")

    client = _mock_client(handler)
    try:
        assert (
            await download_to_temp(client, "http://insecure/v.m4a", tmp_path, _settings()) is None
        )
    finally:
        await client.aclose()


async def test_download_to_temp_uses_explicit_filename(tmp_path: Path) -> None:
    client = _mock_client(lambda _r: httpx.Response(200, content=b"x"))
    try:
        path = await download_to_temp(
            client, "https://w/opaque-id", tmp_path, _settings(), filename="report.pdf"
        )
    finally:
        await client.aclose()
    assert path is not None and path.name == "report.pdf"


async def test_download_to_temp_falls_back_to_suffix_name(tmp_path: Path) -> None:
    client = _mock_client(lambda _r: httpx.Response(200, content=b"x"))
    try:
        path = await download_to_temp(
            client, "https://w/", tmp_path, _settings(), fallback_suffix=".m4a"
        )
    finally:
        await client.aclose()
    assert path is not None and path.name == "media.m4a"


async def test_download_to_temp_enforces_size_cap(tmp_path: Path) -> None:
    client = _mock_client(lambda _r: httpx.Response(200, content=b"0123456789"))
    try:
        path = await download_to_temp(client, "https://w/v.m4a", tmp_path, _settings(), max_bytes=4)
    finally:
        await client.aclose()
    assert path is None  # oversize -> nothing written


async def test_download_to_temp_returns_none_on_http_error(tmp_path: Path) -> None:
    client = _mock_client(lambda _r: httpx.Response(404))
    try:
        assert await download_to_temp(client, "https://w/v.m4a", tmp_path, _settings()) is None
    finally:
        await client.aclose()


# ---------------------------------------------------------------------------
# decode_base64_to_temp + temp_workspace
# ---------------------------------------------------------------------------


def test_decode_base64_to_temp_writes_bytes(tmp_path: Path) -> None:
    raw = b"hello-voice"
    path = decode_base64_to_temp(base64.b64encode(raw).decode(), tmp_path, "voice.m4a")
    assert path is not None
    assert path.read_bytes() == raw
    assert path.name == "voice.m4a"


def test_decode_base64_to_temp_rejects_invalid(tmp_path: Path) -> None:
    assert decode_base64_to_temp("not valid base64 !!!", tmp_path, "v.m4a") is None


def test_decode_base64_to_temp_rejects_oversize(tmp_path: Path) -> None:
    raw = b"0123456789"
    encoded = base64.b64encode(raw).decode()
    assert decode_base64_to_temp(encoded, tmp_path, "v.m4a", max_bytes=4) is None


def test_temp_workspace_cleans_up() -> None:
    captured: Path | None = None
    with temp_workspace() as ws:
        captured = ws
        assert ws.is_dir()
        (ws / "f.txt").write_text("x")
    assert captured is not None and not captured.exists()
