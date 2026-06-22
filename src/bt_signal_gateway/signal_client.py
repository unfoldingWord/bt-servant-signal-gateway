"""signal-cli JSON-RPC client (outbound actions).

A standalone async client for the local ``signal-cli`` daemon's HTTP mode,
speaking JSON-RPC 2.0 at ``{SIGNAL_HTTP_URL}/api/v1/rpc``. Ported and trimmed
from ``../hermes-agent/gateway/platforms/signal.py`` — only the outbound RPC
surface (send, reactions, contacts, attachments) is kept; the inbound SSE
listener, on-disk caching, and typing indicators live elsewhere.

Recipient convention (shared with the inbound envelope layer): a direct chat is
a raw phone number or Signal service ID; a group is the string
``"group:<groupId>"``.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import re
import time
import uuid
from typing import Any

import httpx

from bt_signal_gateway.config import Settings
from bt_signal_gateway.signal_rate_limit import (
    SIGNAL_BATCH_PACING_NOTICE_THRESHOLD,
    SIGNAL_MAX_ATTACHMENTS_PER_MSG,
    SIGNAL_RATE_LIMIT_MAX_ATTEMPTS,
    SignalRateLimitError,
    _extract_retry_after_seconds,
    _format_wait,
    _is_signal_rate_limit_error,
    _signal_send_timeout,
    get_scheduler,
)

logger = logging.getLogger(__name__)

_GROUP_PREFIX = "group:"


# ---------------------------------------------------------------------------
# Identifier helpers
# ---------------------------------------------------------------------------


def _is_signal_service_id(value: str) -> bool:
    """Return True if *value* already looks like a Signal service identifier."""
    if not value:
        return False
    if value.startswith("PNI:") or value.startswith("u:"):
        return True
    try:
        uuid.UUID(value)
        return True
    except (ValueError, AttributeError, TypeError):
        return False


def _looks_like_e164_number(value: str) -> bool:
    """Return True for a plausible E.164 phone number."""
    if not value or not value.startswith("+"):
        return False
    digits = value[1:]
    return digits.isdigit() and 7 <= len(digits) <= 15


# ---------------------------------------------------------------------------
# Markdown -> Signal body ranges
# ---------------------------------------------------------------------------


def _utf16_len(s: str) -> int:
    """Length of *s* in UTF-16 code units."""
    return len(s.encode("utf-16-le")) // 2


def markdown_to_signal(text: str) -> tuple[str, list[str]]:
    """Convert markdown to plain text + Signal textStyles list.

    Signal doesn't render markdown. Instead it uses ``bodyRanges`` (exposed by
    signal-cli as ``textStyle`` / ``textStyles`` params) with the format
    ``start:length:STYLE``.

    Positions are measured in **UTF-16 code units** (not Python code points)
    because that's what the Signal protocol uses.

    Supported styles: BOLD, ITALIC, STRIKETHROUGH, MONOSPACE. (Signal's SPOILER
    style is not currently mapped — no standard markdown syntax for it.)

    Returns ``(plain_text, styles_list)`` where *styles_list* may be empty if
    there's nothing to format.
    """
    # Pre-process: normalize whitespace before any position tracking so later
    # operations don't invalidate recorded offsets.
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()

    styles: list[tuple[int, int, str]] = []

    # --- Phase 1: fenced code blocks  ```...``` -> MONOSPACE ---
    # The optional language tag is only consumed when followed by a newline, so
    # a single-line fence like ``` ```abc``` ``` keeps "abc" as code content
    # instead of mistaking it for a language-only block and dropping it.
    _cb = re.compile(r"```(?:[a-zA-Z0-9_+-]*\n)?(.*?)```", re.DOTALL)
    while m := _cb.search(text):
        inner = m.group(1).rstrip("\n")
        start = m.start()
        text = text[: m.start()] + inner + text[m.end() :]
        styles.append((start, len(inner), "MONOSPACE"))

    # --- Phase 2: heading markers  # Foo -> Foo (BOLD) ---
    _heading = re.compile(r"^#{1,6}\s+", re.MULTILINE)
    new_text = ""
    last_end = 0
    for m in _heading.finditer(text):
        new_text += text[last_end : m.start()]
        last_end = m.end()
        eol = text.find("\n", m.end())
        if eol == -1:
            eol = len(text)
        heading_text = text[m.end() : eol]
        start = len(new_text)
        new_text += heading_text
        styles.append((start, len(heading_text), "BOLD"))
        last_end = eol
    new_text += text[last_end:]
    text = new_text

    # --- Phase 3: inline patterns (single-pass to avoid offset drift) ---
    # Collect ALL non-overlapping matches first, then strip every marker in one
    # pass so positions are computed against the final text.
    #
    # Inline code (`` `...` ``) is listed first so it claims its span before the
    # bold/italic/strike patterns run — that keeps literal markdown markers
    # *inside* code (e.g. ``**x**``) from being stripped as formatting.
    _patterns = [
        (re.compile(r"`(.+?)`"), "MONOSPACE"),
        (re.compile(r"\*\*(.+?)\*\*", re.DOTALL), "BOLD"),
        (re.compile(r"__(.+?)__", re.DOTALL), "BOLD"),
        (re.compile(r"~~(.+?)~~", re.DOTALL), "STRIKETHROUGH"),
        (re.compile(r"(?<!\*)\*(?!\*| )(.+?)(?<!\*)\*(?!\*)"), "ITALIC"),
        (re.compile(r"(?<!\w)_(?!_)(.+?)(?<!_)_(?!\w)"), "ITALIC"),
    ]

    # Seed the occupied set with the fenced-code-block ranges recorded in
    # Phase 1 so the inline patterns below don't reach into already-extracted
    # code content and strip markers from it (``` ```**x**``` ``` must stay
    # literal ``**x**``, MONOSPACE, not become BOLD).
    all_matches: list[tuple[int, int, int, int, str]] = []
    occupied: list[tuple[int, int]] = [
        (s, s + length) for s, length, st in styles if st == "MONOSPACE"
    ]
    for pat, style in _patterns:
        for m in pat.finditer(text):
            ms, me = m.start(), m.end()
            if not any(ms < oe and me > os for os, oe in occupied):
                all_matches.append((ms, me, m.start(1), m.end(1), style))
                occupied.append((ms, me))
    all_matches.sort()

    # Build removal list so we can adjust Phase 1/2 styles. Each match removes
    # its prefix markers (start..g1_start) and suffix markers (g1_end..end).
    removals: list[tuple[int, int]] = []
    for ms, me, g1s, g1e, _ in all_matches:
        if g1s > ms:
            removals.append((ms, g1s - ms))
        if me > g1e:
            removals.append((g1e, me - g1e))
    removals.sort()

    def _adj(pos: int) -> int:
        shift = 0
        for rp, rl in removals:
            if rp < pos:
                shift += min(rl, pos - rp)
            else:
                break
        return pos - shift

    adjusted_prior: list[tuple[int, int, str]] = []
    for s, length, st in styles:
        ns = _adj(s)
        ne = _adj(s + length)
        if ne > ns:
            adjusted_prior.append((ns, ne - ns, st))

    # Strip all inline markers in one pass -> positions are correct.
    result = ""
    last_end = 0
    inline_styles: list[tuple[int, int, str]] = []
    for ms, me, g1s, g1e, sty in all_matches:
        result += text[last_end:ms]
        pos = len(result)
        inner = text[g1s:g1e]
        result += inner
        inline_styles.append((pos, len(inner), sty))
        last_end = me
    result += text[last_end:]
    text = result

    styles = adjusted_prior + inline_styles

    # Convert code-point offsets -> UTF-16 code-unit offsets.
    style_strings: list[str] = []
    for cp_start, cp_len, stype in sorted(styles):
        # Safety: skip any out-of-bounds styles.
        if cp_start < 0 or cp_start + cp_len > len(text):
            continue
        u16_start = _utf16_len(text[:cp_start])
        u16_len = _utf16_len(text[cp_start : cp_start + cp_len])
        style_strings.append(f"{u16_start}:{u16_len}:{stype}")

    return text, style_strings


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class SignalClient:
    """Async JSON-RPC 2.0 client for the signal-cli daemon (outbound actions)."""

    def __init__(self, settings: Settings, *, client: httpx.AsyncClient | None = None) -> None:
        self._account = settings.signal_account
        self._http_url = settings.signal_http_url.rstrip("/")
        # When a client is injected (tests / shared client) we don't own its
        # lifecycle and must not close it in aclose().
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=30.0)

        # Best-effort number<->service-id mapping so outbound sends can upgrade
        # an E.164 number to the UUID signal-cli prefers. Seeded from inbound
        # envelopes (by the listener) and from listContacts.
        self._uuid_by_number: dict[str, str] = {}
        self._number_by_uuid: dict[str, str] = {}
        self._cache_lock = asyncio.Lock()

    async def aclose(self) -> None:
        """Close the underlying HTTP client if this instance owns it."""
        if self._owns_client:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # JSON-RPC core
    # ------------------------------------------------------------------

    async def _rpc(
        self,
        method: str,
        params: dict[str, Any],
        *,
        log_failures: bool = True,
        raise_on_rate_limit: bool = False,
        rpc_timeout: float = 30.0,
    ) -> Any:
        """Send a JSON-RPC 2.0 request to the signal-cli daemon.

        Returns the ``result`` payload, or ``None`` on any error. When
        ``raise_on_rate_limit=True``, a Signal ``[429]`` / ``RateLimitException``
        response raises :class:`SignalRateLimitError` instead of returning
        ``None`` — lets the attachment path opt into backoff-retry.
        """
        rpc_id = f"{method}_{int(time.time() * 1000)}"
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": rpc_id,
        }

        try:
            resp = await self._client.post(
                f"{self._http_url}/api/v1/rpc",
                json=payload,
                timeout=rpc_timeout,
            )
            resp.raise_for_status()
            data = resp.json()

            if "error" in data:
                err = data["error"]
                if raise_on_rate_limit and _is_signal_rate_limit_error(err):
                    err_msg = str(err.get("message", "")) if isinstance(err, dict) else str(err)
                    retry_after = _extract_retry_after_seconds(err)
                    raise SignalRateLimitError(err_msg, retry_after=retry_after)
                if log_failures:
                    logger.warning("signal rpc error", extra={"method": method, "error": err})
                else:
                    logger.debug("signal rpc error", extra={"method": method, "error": err})
                return None

            return data.get("result")

        except SignalRateLimitError:
            raise
        except Exception as exc:
            if log_failures:
                logger.warning("signal rpc failed", extra={"method": method, "error": str(exc)})
            else:
                logger.debug("signal rpc failed", extra={"method": method, "error": str(exc)})
            return None

    # ------------------------------------------------------------------
    # Recipient / UUID resolution
    # ------------------------------------------------------------------

    def remember_identifiers(self, number: str | None, service_id: str | None) -> None:
        """Cache any number<->UUID mapping observed (e.g. from inbound envelopes)."""
        if not number or not service_id or not _is_signal_service_id(service_id):
            return
        self._uuid_by_number[number] = service_id
        self._number_by_uuid[service_id] = number

    def _extract_contact_uuid(self, contact: Any, phone_number: str) -> str | None:
        """Best-effort extraction of a Signal service ID from listContacts output."""
        if not isinstance(contact, dict):
            return None

        number = contact.get("number")
        recipient = contact.get("recipient")
        service_id = contact.get("uuid") or contact.get("serviceId")
        if not service_id:
            profile = contact.get("profile")
            if isinstance(profile, dict):
                service_id = profile.get("serviceId") or profile.get("uuid")

        if service_id and _is_signal_service_id(service_id):
            if number == phone_number or recipient == phone_number:
                return service_id
        return None

    async def _resolve_recipient(self, chat_id: str) -> str:
        """Return the preferred Signal recipient identifier for a direct chat.

        Upgrades an E.164 number to its service ID when one is known (cached or
        discoverable via listContacts); otherwise returns *chat_id* unchanged.
        """
        if (
            not chat_id
            or chat_id.startswith(_GROUP_PREFIX)
            or _is_signal_service_id(chat_id)
            or not _looks_like_e164_number(chat_id)
        ):
            return chat_id

        cached = self._uuid_by_number.get(chat_id)
        if cached:
            return cached

        async with self._cache_lock:
            cached = self._uuid_by_number.get(chat_id)
            if cached:
                return cached

            contacts = await self.list_contacts()
            if isinstance(contacts, list):
                for contact in contacts:
                    number = contact.get("number") if isinstance(contact, dict) else None
                    service_id = self._extract_contact_uuid(contact, chat_id)
                    if number and service_id:
                        self.remember_identifiers(number, service_id)

            return self._uuid_by_number.get(chat_id, chat_id)

    async def list_contacts(self) -> Any:
        """Return signal-cli's contact list (``listContacts`` RPC result)."""
        return await self._rpc(
            "listContacts",
            {"account": self._account, "allRecipients": True},
        )

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    async def _apply_destination(self, params: dict[str, Any], chat_id: str) -> None:
        """Set ``groupId`` or ``recipient`` on *params* based on *chat_id*."""
        if chat_id.startswith(_GROUP_PREFIX):
            params["groupId"] = chat_id[len(_GROUP_PREFIX) :]
        else:
            params["recipient"] = [await self._resolve_recipient(chat_id)]

    # ------------------------------------------------------------------
    # Sending
    # ------------------------------------------------------------------

    async def send(
        self,
        chat_id: str,
        message: str,
        attachments: list[str] | None = None,
        text_styles: list[str] | None = None,
    ) -> bool:
        """Send a text (and/or attachment) message with native Signal formatting.

        When *text_styles* is None the message is run through
        :func:`markdown_to_signal`; pass an explicit list to override. Returns
        True on a successful RPC.
        """
        if text_styles is None:
            message, text_styles = markdown_to_signal(message)

        params: dict[str, Any] = {"account": self._account, "message": message}

        if text_styles:
            if len(text_styles) == 1:
                params["textStyle"] = text_styles[0]
            else:
                params["textStyles"] = text_styles

        await self._apply_destination(params, chat_id)

        if attachments:
            return await self._send_with_attachments(params, attachments)

        result = await self._rpc("send", params)
        return result is not None

    async def _send_with_attachments(self, params: dict[str, Any], attachments: list[str]) -> bool:
        """Send a single message carrying attachments (one RPC).

        Used by :meth:`send` for the text-with-attachments case; the multi-batch
        outbound media senders below build on the same paced primitive.
        """
        params = dict(params, attachments=attachments)
        return await self._send_attachment_batch(params, len(attachments))

    async def _send_attachment_batch(self, params: dict[str, Any], n: int) -> bool:
        """Send one already-built attachment-carrying ``send`` RPC, paced by the
        rate-limit scheduler with 429 backoff-retry.

        ``params`` must already include ``attachments`` and a resolved
        destination; ``n`` is the attachment count (drives the scheduler reserve
        and the upload-scaled timeout). Returns True on a successful send.
        """
        scheduler = get_scheduler()
        send_timeout = _signal_send_timeout(n)

        for attempt in range(1, SIGNAL_RATE_LIMIT_MAX_ATTEMPTS + 1):
            await scheduler.acquire(n)
            try:
                t0 = time.monotonic()
                result = await self._rpc(
                    "send", params, raise_on_rate_limit=True, rpc_timeout=send_timeout
                )
                if result is not None:
                    await scheduler.report_rpc_duration(time.monotonic() - t0, n)
                    return True
                # Non-rate-limit transient failure: retry with simple backoff.
                if attempt < SIGNAL_RATE_LIMIT_MAX_ATTEMPTS:
                    await asyncio.sleep(2.0**attempt)
                    continue
                return False
            except SignalRateLimitError as exc:
                scheduler.feedback(exc.retry_after, n)
                if attempt >= SIGNAL_RATE_LIMIT_MAX_ATTEMPTS:
                    logger.error(
                        "signal: rate-limit retries exhausted",
                        extra={"attachments": n, "retry_after": exc.retry_after},
                    )
                    return False
                logger.warning(
                    "signal: rate-limited, scheduler will pace the retry",
                    extra={"attempt": attempt, "retry_after": exc.retry_after},
                )
        return False

    # ------------------------------------------------------------------
    # Outbound media (voice notes + file attachments)
    # ------------------------------------------------------------------

    async def send_voice_note(self, chat_id: str, file_path: str) -> bool:
        """Send a single audio file as a playable Signal voice note.

        signal-cli's ``voiceNote`` flag marks the lone attachment as an inline
        playable voice message rather than a generic file. Paced like any other
        attachment send.
        """
        params: dict[str, Any] = {
            "account": self._account,
            "message": "",
            "attachments": [file_path],
            "voiceNote": True,
        }
        await self._apply_destination(params, chat_id)
        return await self._send_attachment_batch(params, 1)

    async def send_attachments(
        self, chat_id: str, file_paths: list[str], message: str = ""
    ) -> bool:
        """Send file attachments, split into ``SIGNAL_MAX_ATTACHMENTS_PER_MSG``
        (32) per RPC and paced by the rate-limit scheduler.

        The optional caption *message* rides only on the first batch. Returns
        True only if every batch is delivered; an empty list is a no-op success.
        """
        if not file_paths:
            return True

        base_params: dict[str, Any] = {"account": self._account}
        await self._apply_destination(base_params, chat_id)

        scheduler = get_scheduler()
        batches = [
            file_paths[i : i + SIGNAL_MAX_ATTACHMENTS_PER_MSG]
            for i in range(0, len(file_paths), SIGNAL_MAX_ATTACHMENTS_PER_MSG)
        ]
        all_ok = True
        for idx, batch in enumerate(batches):
            n = len(batch)
            wait = scheduler.estimate_wait(n)
            if wait >= SIGNAL_BATCH_PACING_NOTICE_THRESHOLD:
                await self._notify_batch_pacing(chat_id, idx + 1, len(batches), wait)
            params = dict(base_params, message=message if idx == 0 else "", attachments=batch)
            if not await self._send_attachment_batch(params, n):
                all_ok = False
        return all_ok

    async def _notify_batch_pacing(
        self, chat_id: str, next_batch_idx: int, total_batches: int, wait_s: float
    ) -> None:
        """Tell the user when an inter-batch pacing wait crosses the notice
        threshold. Best-effort; logs and continues on failure."""
        try:
            await self.send(
                chat_id,
                f"(More files coming — pausing ~{_format_wait(wait_s)} for Signal "
                f"rate limit, batch {next_batch_idx}/{total_batches}.)",
            )
        except Exception as exc:  # informational only — never fail the send
            logger.warning("signal: failed to send pacing notice", extra={"error": str(exc)})

    # ------------------------------------------------------------------
    # Reactions (progress indicators)
    # ------------------------------------------------------------------

    async def send_reaction(
        self,
        chat_id: str,
        emoji: str,
        target_author: str,
        target_timestamp: int,
    ) -> bool:
        """React to a specific message (e.g. 👀 on start, ✅/❌ on completion)."""
        params: dict[str, Any] = {
            "account": self._account,
            "emoji": emoji,
            "targetAuthor": target_author,
            "targetTimestamp": target_timestamp,
        }
        await self._apply_destination(params, chat_id)
        result = await self._rpc("sendReaction", params)
        return result is not None

    async def remove_reaction(
        self,
        chat_id: str,
        target_author: str,
        target_timestamp: int,
    ) -> bool:
        """Remove a previously sent reaction."""
        params: dict[str, Any] = {
            "account": self._account,
            "emoji": "",
            "targetAuthor": target_author,
            "targetTimestamp": target_timestamp,
            "remove": True,
        }
        await self._apply_destination(params, chat_id)
        result = await self._rpc("sendReaction", params)
        return result is not None

    # ------------------------------------------------------------------
    # Attachments (inbound)
    # ------------------------------------------------------------------

    async def get_attachment(self, attachment_id: str) -> bytes | None:
        """Fetch an inbound attachment's raw bytes via JSON-RPC.

        Returns the decoded bytes, or ``None`` if the attachment is missing.
        On-disk caching and media-type handling are the caller's concern
        (issue #7).
        """
        result = await self._rpc(
            "getAttachment",
            {"account": self._account, "id": attachment_id},
        )
        if not result:
            return None

        # signal-cli returns {"data": "base64..."}; older shapes return the
        # base64 string directly.
        if isinstance(result, dict):
            result = result.get("data")
            if not result:
                logger.warning("signal: attachment response missing 'data'")
                return None

        return base64.b64decode(result)
