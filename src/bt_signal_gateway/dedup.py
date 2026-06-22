"""In-memory TTL dedup of worker callbacks keyed by message_key.

The worker does **not** retry idempotently, so it may deliver the same
``complete`` callback more than once. This gateway dedups on ``message_key``:
the first ``complete`` for a key is delivered and the key is marked; subsequent
``complete`` callbacks for the same key within the TTL are dropped.

In-memory is sufficient — a single long-running process holds the inbound SSE
connection (see CLAUDE.md), so there is no cross-instance dedup to coordinate.

Ported from ``../bt-servant-telegram-gateway/src/services/dedup.ts``
(``CompletedKeysMap``).
"""

from __future__ import annotations

import time
from collections.abc import Callable

#: Default interval between lazy expiry sweeps.
_DEFAULT_SWEEP_INTERVAL_MS = 60_000.0


def _monotonic_ms() -> float:
    """Current monotonic clock in milliseconds (immune to wall-clock jumps)."""
    return time.monotonic() * 1000.0


class CompletedKeys:
    """TTL-bounded set of ``message_key`` values that have been delivered.

    ``now`` is injectable so tests can advance time deterministically; it
    returns milliseconds. Expired entries are swept lazily (at most once per
    ``sweep_interval_ms``) on access rather than via a background timer.
    """

    def __init__(
        self,
        *,
        ttl_ms: float,
        sweep_interval_ms: float = _DEFAULT_SWEEP_INTERVAL_MS,
        now: Callable[[], float] = _monotonic_ms,
    ) -> None:
        self._entries: dict[str, float] = {}
        self._ttl_ms = ttl_ms
        self._sweep_interval_ms = sweep_interval_ms
        self._now = now
        self._last_sweep = now()

    def is_completed(self, key: str) -> bool:
        """Return ``True`` if *key* is currently marked within its TTL."""
        now = self._now()
        self._maybe_sweep(now)
        expiry = self._entries.get(key)
        return expiry is not None and expiry > now

    def mark_completed(self, key: str) -> None:
        """Mark *key* as delivered. Idempotent — refreshes the expiry."""
        now = self._now()
        self._maybe_sweep(now)
        self._entries[key] = now + self._ttl_ms

    def size(self) -> int:
        """Number of tracked keys (including not-yet-swept expired ones)."""
        return len(self._entries)

    def _maybe_sweep(self, now: float) -> None:
        if now - self._last_sweep < self._sweep_interval_ms:
            return
        self._last_sweep = now
        expired = [key for key, expiry in self._entries.items() if expiry <= now]
        for key in expired:
            del self._entries[key]
