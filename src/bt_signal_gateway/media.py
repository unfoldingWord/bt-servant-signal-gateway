"""Media handling: inbound audio encoding + outbound media download.

Inbound: pick an audio attachment off an :class:`~bt_signal_gateway.envelope.InboundMessage`,
fetch its bytes via the signal-cli ``getAttachment`` RPC, and base64-encode them
into the worker's ``message_type="audio"`` shape (with an ``audio_format`` hint).

Outbound: download worker-hosted media (voice replies + file attachments) over
HTTPS with the engine bearer token, to a temp file signal-cli can read off the
shared volume. The per-delivery temp workspace is removed wholesale after the
send.

Mirrors the size-guard / HTTPS-only / bearer-auth posture of
``../bt-servant-whatsapp-gateway`` and the file-path attachment transport of
``../hermes-agent``.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import logging
import os
import re
import shutil
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

from bt_signal_gateway.config import Settings
from bt_signal_gateway.envelope import AttachmentRef
from bt_signal_gateway.signal_client import SignalClient
from bt_signal_gateway.signal_rate_limit import SIGNAL_MAX_ATTACHMENT_SIZE

logger = logging.getLogger(__name__)

#: Inbound audio cap (mirrors the WhatsApp gateway's 25 MB voice-message guard).
MAX_INBOUND_AUDIO_BYTES = 25 * 1024 * 1024

#: contentType -> worker ``audio_format`` hint.
_AUDIO_MIME_TO_FORMAT = {
    "audio/aac": "aac",
    "audio/mp4": "m4a",
    "audio/x-m4a": "m4a",
    "audio/mpeg": "mp3",
    "audio/mp3": "mp3",
    "audio/ogg": "ogg",
    "audio/opus": "ogg",
    "audio/wav": "wav",
    "audio/x-wav": "wav",
    "audio/webm": "webm",
    "audio/3gpp": "3gp",
}
#: filename extension -> ``audio_format`` hint (fallback when contentType absent).
_AUDIO_EXT_TO_FORMAT = {
    ".aac": "aac",
    ".m4a": "m4a",
    ".mp4": "m4a",
    ".mp3": "mp3",
    ".ogg": "ogg",
    ".oga": "ogg",
    ".opus": "ogg",
    ".wav": "wav",
    ".webm": "webm",
    ".3gp": "3gp",
}
#: Default when nothing identifies the codec (Signal voice notes are AAC).
_DEFAULT_AUDIO_FORMAT = "aac"

_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]")


@dataclass(frozen=True, slots=True)
class InboundAudio:
    """A fetched inbound audio attachment, ready for the worker ChatRequest."""

    audio_base64: str
    audio_format: str


@dataclass(frozen=True, slots=True)
class OutboundAttachment:
    """A worker-hosted file attachment to deliver to Signal."""

    url: str
    filename: str | None = None
    mime_type: str | None = None
    size_bytes: int | None = None


# ---------------------------------------------------------------------------
# Inbound audio
# ---------------------------------------------------------------------------


def is_audio(ref: AttachmentRef) -> bool:
    """True if *ref* looks like audio (by contentType, else filename extension)."""
    if ref.content_type and ref.content_type.lower().startswith("audio/"):
        return True
    if ref.filename:
        return Path(ref.filename).suffix.lower() in _AUDIO_EXT_TO_FORMAT
    return False


def select_inbound_audio(refs: list[AttachmentRef]) -> AttachmentRef | None:
    """Return the first audio attachment in *refs*, or ``None``."""
    for ref in refs:
        if is_audio(ref):
            return ref
    return None


def _audio_format(ref: AttachmentRef) -> str:
    """Best-effort worker ``audio_format`` for *ref* (contentType, then ext)."""
    if ref.content_type:
        fmt = _AUDIO_MIME_TO_FORMAT.get(ref.content_type.lower().split(";")[0].strip())
        if fmt:
            return fmt
    if ref.filename:
        fmt = _AUDIO_EXT_TO_FORMAT.get(Path(ref.filename).suffix.lower())
        if fmt:
            return fmt
    return _DEFAULT_AUDIO_FORMAT


async def fetch_inbound_audio(
    ref: AttachmentRef,
    signal_client: SignalClient,
    *,
    max_bytes: int = MAX_INBOUND_AUDIO_BYTES,
) -> InboundAudio | None:
    """Fetch and base64-encode an inbound audio attachment.

    Enforces *max_bytes* both from the declared size (pre-fetch, cheap) and the
    actual byte length (post-fetch). Returns ``None`` when the attachment is
    missing or too large.
    """
    if ref.size is not None and ref.size > max_bytes:
        logger.warning(
            "media: inbound audio too large (declared)",
            extra={"size": ref.size, "max": max_bytes, "id": ref.id},
        )
        return None

    data = await signal_client.get_attachment(ref.id)
    if not data:
        logger.warning("media: inbound audio fetch returned no bytes", extra={"id": ref.id})
        return None
    if len(data) > max_bytes:
        logger.warning(
            "media: inbound audio too large",
            extra={"size": len(data), "max": max_bytes, "id": ref.id},
        )
        return None

    return InboundAudio(
        audio_base64=base64.b64encode(data).decode("ascii"),
        audio_format=_audio_format(ref),
    )


# ---------------------------------------------------------------------------
# Outbound media
# ---------------------------------------------------------------------------


def parse_outbound_attachments(raw: Any) -> list[OutboundAttachment]:
    """Parse a callback ``attachments`` array; skip entries without a URL."""
    if not isinstance(raw, list):
        return []
    out: list[OutboundAttachment] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        url = item.get("url")
        if not isinstance(url, str) or not url:
            continue
        filename = item.get("filename")
        mime_type = item.get("mime_type")
        size_bytes = item.get("size_bytes")
        out.append(
            OutboundAttachment(
                url=url,
                filename=filename if isinstance(filename, str) and filename else None,
                mime_type=mime_type if isinstance(mime_type, str) and mime_type else None,
                size_bytes=size_bytes if isinstance(size_bytes, int) else None,
            )
        )
    return out


@contextmanager
def temp_workspace() -> Iterator[Path]:
    """Yield a fresh temp directory, removed wholesale on exit."""
    path = Path(tempfile.mkdtemp(prefix="bt-signal-media-"))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def _safe_filename(name: str) -> str:
    """Sanitize *name* to a basename safe to write into the temp workspace."""
    cleaned = _SAFE_NAME_RE.sub("_", os.path.basename(name)).strip("._")
    return cleaned or "attachment"


def _target_name(url: str, filename: str | None, fallback_suffix: str) -> str:
    """Pick the on-disk name: explicit filename, then a URL basename with an
    extension, then ``media`` + *fallback_suffix*."""
    if filename:
        return _safe_filename(filename)
    url_name = _safe_filename(os.path.basename(urlsplit(url).path))
    if url_name and "." in url_name:
        return url_name
    return f"media{fallback_suffix}"


async def download_to_temp(
    client: httpx.AsyncClient,
    url: str,
    dest_dir: Path,
    settings: Settings,
    *,
    filename: str | None = None,
    fallback_suffix: str = "",
    max_bytes: int = SIGNAL_MAX_ATTACHMENT_SIZE,
) -> Path | None:
    """Download *url* (HTTPS-only, engine bearer auth) to a file in *dest_dir*.

    Streams with a hard *max_bytes* cap (the declared ``Content-Length`` when
    present, then enforced byte-by-byte). Returns the written path, or ``None``
    on any rejection/failure. signal-cli reads the file off the shared volume.
    """
    scheme = urlsplit(url).scheme.lower()
    if scheme != "https":
        logger.error("media: refusing non-HTTPS download", extra={"scheme": scheme})
        return None

    dest = dest_dir / _target_name(url, filename, fallback_suffix)
    headers = {"Authorization": f"Bearer {settings.engine_api_key}"}

    try:
        async with client.stream("GET", url, headers=headers) as resp:
            if resp.status_code != 200:
                logger.error("media: download failed", extra={"status": resp.status_code})
                return None

            declared = resp.headers.get("Content-Length")
            if declared is not None:
                try:
                    if int(declared) > max_bytes:
                        logger.error(
                            "media: download exceeds size cap (declared)",
                            extra={"size": declared, "max": max_bytes},
                        )
                        return None
                except ValueError:
                    pass

            buf = bytearray()
            oversize = False
            async for chunk in resp.aiter_bytes():
                buf += chunk
                if len(buf) > max_bytes:
                    oversize = True
                    break
    except httpx.HTTPError as exc:
        logger.error(
            "media: download error",
            extra={"host": urlsplit(url).netloc, "error": str(exc)},
        )
        return None

    if oversize:
        logger.error("media: download exceeds size cap", extra={"max": max_bytes})
        return None

    # Write off the event loop — signal-cli reads the file off the shared volume.
    await asyncio.to_thread(dest.write_bytes, bytes(buf))
    return dest


def decode_base64_to_temp(
    b64: str,
    dest_dir: Path,
    name: str,
    *,
    max_bytes: int = SIGNAL_MAX_ATTACHMENT_SIZE,
) -> Path | None:
    """Decode a base64 payload to a file in *dest_dir*; ``None`` on bad/oversize."""
    try:
        data = base64.b64decode(b64, validate=True)
    except (binascii.Error, ValueError):
        logger.error("media: invalid base64 payload")
        return None
    if not data:
        logger.error("media: empty base64 payload")
        return None
    if len(data) > max_bytes:
        logger.error("media: base64 payload exceeds size cap", extra={"size": len(data)})
        return None
    dest = dest_dir / _safe_filename(name)
    dest.write_bytes(data)
    return dest
