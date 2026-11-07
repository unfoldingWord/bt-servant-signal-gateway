"""Per-chat gate for Signal typing indicators.

signal-cli's ``sendTyping`` shows "typing…" for about 15 seconds unless a stop
is sent. Signal's own clients refresh a still-active indicator every 10
seconds, so a start for the *same* turn is allowed at most once in that
window. A different message in the same chat is a new turn and starts
immediately. A turn marked finished (``complete`` / ``error``) is never
started again, which keeps a late ``status`` from bringing the indicator back.

Signal has one indicator per conversation. Finishing a turn does not send
stop while a turn that started later in that chat is still open, so the
newer question keeps "typing…" until it finishes too.
"""

from __future__ import annotations

import logging
import time
from collections import OrderedDict
from collections.abc import Callable

from bt_signal_gateway.signal_client import SignalClient

logger = logging.getLogger(__name__)

#: Minimum gap between typing-start RPCs for one turn. Matches the cadence
#: Signal uses to keep its own 15-second indicator visible.
TYPING_REFRESH_SECONDS = 10.0

#: Finished ``message_key`` values kept in memory. Oldest are dropped first.
#: A late status can restart typing only after this many newer turns have
#: finished, which is far past any real callback delay, and the process cannot
#: grow the set without bound.
FINISHED_TURN_CAP = 4096


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
        finished_cap: int = FINISHED_TURN_CAP,
    ) -> None:
        self._interval_s = interval_s
        self._now = now
        self._finished_cap = finished_cap
        self._last_sent: dict[str, float] = {}
        self._active_key: dict[str, str] = {}
        self._finished: OrderedDict[str, None] = OrderedDict()
        # message_keys that have started and not yet finished, in start order.
        self._open: dict[str, OrderedDict[str, None]] = {}
        self._ticket: dict[str, int] = {}
        self._next_ticket = 0

    def try_acquire(self, chat_id: str, message_key: str) -> bool:
        """Reserve a typing-start for *chat_id*, or return False when skipped.

        False when *message_key* already finished, or when this same turn was
        started less than ``interval_s`` ago. A different ``message_key`` is a
        new turn and is not throttled. A successful reserve records the time
        immediately so a second callback cannot slip through before the RPC
        returns.
        """
        if message_key in self._finished:
            return False
        now = self._now()
        last = self._last_sent.get(chat_id)
        same_turn = self._active_key.get(chat_id) == message_key
        if same_turn and last is not None and now - last < self._interval_s:
            return False
        self._last_sent[chat_id] = now
        self._active_key[chat_id] = message_key
        self._note_open(chat_id, message_key)
        self._next_ticket += 1
        self._ticket[chat_id] = self._next_ticket
        return True

    def current_ticket(self, chat_id: str) -> int | None:
        """Ticket of the reserve currently held for *chat_id*, if any."""
        return self._ticket.get(chat_id)

    def holds_ticket(self, chat_id: str, ticket: int) -> bool:
        """True when *ticket* is still the chat's current reserve."""
        return self._ticket.get(chat_id) == ticket

    def release(self, chat_id: str, ticket: int) -> None:
        """Drop a reserve so a failed start can be retried on the next status.

        A newer reserve for the same chat is left alone.
        """
        if self._ticket.get(chat_id) != ticket:
            return
        self._last_sent.pop(chat_id, None)
        self._ticket.pop(chat_id, None)

    def has_open_turn(self, chat_id: str) -> bool:
        """True when some turn in *chat_id* has started and not finished."""
        return bool(self._open.get(chat_id))

    def finish(self, chat_id: str, message_key: str) -> bool:
        """Mark *message_key* finished.

        True only the first time, and only when no later turn in this chat is
        still open. Signal has one indicator per conversation, so a stop while
        a newer turn is running would hide "typing…" for that turn. The caller
        sends the stop RPC only when this returns True.

        The chat reserve stays with the newer turn. It is cleared only when
        the stop should go out, so an in-flight start cannot leave the
        indicator up after that stop.
        """
        if message_key in self._finished:
            return False
        keep_up = self._later_turn_still_open(chat_id, message_key)
        self._remember_finished(message_key)
        self._close_turn(chat_id, message_key)
        if keep_up:
            return False
        self._drop_chat_reserve(chat_id)
        return True

    def cancel_chat(self, chat_id: str) -> None:
        """Drop the chat reserve after the relay to the worker fails.

        Does not mark a turn finished: nothing was submitted, so there is no
        late ``status`` to keep dark. Forgets every open turn in the chat so
        a message that never reached the worker cannot keep a later stop from
        being sent. An in-flight start sees the missing ticket and sends a
        stop when its RPC returns.
        """
        self._open.pop(chat_id, None)
        self._drop_chat_reserve(chat_id)

    def _remember_finished(self, message_key: str) -> None:
        self._finished[message_key] = None
        while len(self._finished) > self._finished_cap:
            self._finished.popitem(last=False)

    def _note_open(self, chat_id: str, message_key: str) -> None:
        opened = self._open.setdefault(chat_id, OrderedDict())
        if message_key not in opened:
            opened[message_key] = None

    def _later_turn_still_open(self, chat_id: str, message_key: str) -> bool:
        opened = self._open.get(chat_id)
        if not opened or message_key not in opened:
            return False
        keys = list(opened)
        return keys.index(message_key) < len(keys) - 1

    def _close_turn(self, chat_id: str, message_key: str) -> None:
        opened = self._open.get(chat_id)
        if opened is None:
            return
        opened.pop(message_key, None)
        if not opened:
            self._open.pop(chat_id, None)

    def _drop_chat_reserve(self, chat_id: str) -> None:
        self._last_sent.pop(chat_id, None)
        self._active_key.pop(chat_id, None)
        self._ticket.pop(chat_id, None)


async def send_typing_start(
    signal_client: SignalClient,
    typing: TypingIndicators,
    chat_id: str,
    ticket: int,
) -> None:
    """Send a start indicator. A failure is logged and the reserve released.

    *ticket* is the one :meth:`TypingIndicators.try_acquire` just issued. If
    that reserve is gone when the RPC returns and no turn in the chat is
    still open, a stop is sent so a cancel that won the race cannot leave
    "typing…" up. A newer turn that is still open keeps the indicator.
    """
    if not typing.holds_ticket(chat_id, ticket):
        return
    try:
        ok = await signal_client.send_typing(chat_id, stop=False)
    except Exception as exc:
        typing.release(chat_id, ticket)
        logger.warning(
            "typing indicator failed",
            extra={"chat_id": chat_id, "error": str(exc)},
        )
        return
    if not ok:
        typing.release(chat_id, ticket)
        logger.warning("typing indicator failed", extra={"chat_id": chat_id})
        return
    if not typing.holds_ticket(chat_id, ticket) and not typing.has_open_turn(chat_id):
        await send_typing_stop(signal_client, chat_id)


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
