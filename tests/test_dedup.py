"""Unit tests for the message_key dedup cache."""

from __future__ import annotations

from bt_signal_gateway.dedup import CompletedKeys


class _Clock:
    """Manually-advanced millisecond clock for deterministic TTL tests."""

    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def test_unseen_key_is_not_completed() -> None:
    keys = CompletedKeys(ttl_ms=1000)
    assert keys.is_completed("k1") is False


def test_marked_key_is_completed() -> None:
    keys = CompletedKeys(ttl_ms=1000)
    keys.mark_completed("k1")
    assert keys.is_completed("k1") is True
    assert keys.is_completed("other") is False


def test_key_expires_after_ttl() -> None:
    clock = _Clock()
    keys = CompletedKeys(ttl_ms=1000, now=clock)
    keys.mark_completed("k1")
    assert keys.is_completed("k1") is True

    clock.t = 1500  # past the 1000ms TTL
    assert keys.is_completed("k1") is False


def test_sweep_evicts_expired_entries() -> None:
    clock = _Clock()
    keys = CompletedKeys(ttl_ms=1000, sweep_interval_ms=500, now=clock)
    keys.mark_completed("k1")
    assert keys.size() == 1

    clock.t = 2000  # past TTL and past the sweep interval
    keys.mark_completed("k2")  # triggers a sweep, which evicts k1
    assert keys.size() == 1
    assert keys.is_completed("k1") is False
    assert keys.is_completed("k2") is True
