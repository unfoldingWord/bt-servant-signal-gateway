"""Per-chat gate for Signal typing indicators.

signal-cli's ``sendTyping`` shows "typing…" for about 15 seconds unless a stop
is sent. Signal's own clients refresh a still-active indicator every 10
seconds, so a start is allowed at most once per chat in that window. A turn
marked finished (``complete`` / ``error``) is never started again, which keeps
a late ``status`` from bringing the indicator back.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

from bt_signal_gateway.signal_client import SignalClient

logger = logging.getLogger(__name__)

#: Minimum gap between typing-start RPCs for one chat. Matches the cadence
#: Signal uses to keep its own 15-second indicator visible.
TYPING_REFRESH_SECONDS = 10.0


class TypingIndicators:
    """In-memory throttle and per-turn stop for typing indicators.

    ``now`` is injectable so tests can advance time without sleeping. One
    process owns the Signal session, so the gate does not need to be shared
    across instances.
    """

    def __init__(
        self,
        *,
        interval_s: float = TYPING_REFRESH_SECONDS,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self._interval_s = interval_s
        self._now = now
        self._last_sent: dict[str, float] = {}
        self._finished: set[str] = set()

    def try_acquire(self, chat_id: str, message_key: str) -> bool:
        """Reserve a typing-start for *chat_id*, or return False when skipped.

        False when *message_key* already finished, or when this chat was
        started less than ``interval_s`` ago. A successful reserve records the
        time immediately so a second callback cannot slip through before the
        RPC returns.
        """
        if message_key in self._finished:
            return False
        now = self._now()
        last = self._last_sent.get(chat_id)
        if last is not None and now - last < self._interval_s:
            return False
        self._last_sent[chat_id] = now
        return True

    def release(self, chat_id: str) -> None:
        """Drop a reserve so a failed start can be retried on the next status."""
        self._last_sent.pop(chat_id, None)

    def finish(self, chat_id: str, message_key: str) -> bool:
        """Mark *message_key* finished. True only the first time.

        Clears the chat throttle so the next turn can show typing immediately.
        The caller sends the stop RPC only when this returns True.
        """
        if message_key in self._finished:
            return False
        self._finished.add(message_key)
        self._last_sent.pop(chat_id, None)
        return True


def chat_id_for(*, user_id: str, chat_id: str | None) -> str:
    """Chat a callback addresses: the group id, or the DM sender."""
    return chat_id or user_id


async def send_typing_start(
    signal_client: SignalClient,
    typing: TypingIndicators,
    chat_id: str,
) -> None:
    """Send a start indicator. A failure is logged and the reserve released."""
    try:
        ok = await signal_client.send_typing(chat_id, stop=False)
    except Exception as exc:
        typing.release(chat_id)
        logger.warning(
            "typing indicator failed",
            extra={"chat_id": chat_id, "error": str(exc)},
        )
        return
    if not ok:
        typing.release(chat_id)
        logger.warning("typing indicator failed", extra={"chat_id": chat_id})


async def send_typing_stop(signal_client: SignalClient, chat_id: str) -> None:
    """Send a stop indicator. A failure is logged and never raised."""
    try:
        ok = await signal_client.send_typing(chat_id, stop=True)
    except Exception as exc:
        logger.warning(
            "typing indicator failed",
            extra={"chat_id": chat_id, "stop": True, "error": str(exc)},
        )
        return
    if not ok:
        logger.warning("typing indicator failed", extra={"chat_id": chat_id, "stop": True})
