"""Unit tests for the Signal attachment rate-limit scheduler."""

from __future__ import annotations

import pytest

from bt_signal_gateway.signal_rate_limit import (
    SIGNAL_RATE_LIMIT_BUCKET_CAPACITY,
    SignalAttachmentScheduler,
    SignalSchedulerError,
    _extract_retry_after_seconds,
    _is_signal_rate_limit_error,
    _signal_send_timeout,
    get_scheduler,
)


async def test_acquire_reserves_tokens_under_lock() -> None:
    sched = SignalAttachmentScheduler()
    # Fresh bucket starts full; a small acquire sleeps 0s and deducts the
    # reserved tokens immediately so concurrent callers can't reuse the balance.
    assert sched.tokens == sched.capacity
    slept = await sched.acquire(10)
    assert slept == 0.0
    assert sched.tokens == pytest.approx(sched.capacity - 10)


async def test_concurrent_acquires_do_not_overrun_bucket() -> None:
    import asyncio

    # Two simultaneous 30-token sends against a 50-token bucket: the first
    # reserves 30, the second must wait for refill rather than also passing
    # against the original balance (the bug the reviewer flagged). Fast refill
    # keeps the forced wait down to milliseconds.
    sched = SignalAttachmentScheduler(capacity=50, default_retry_after=0.001)
    slept = await asyncio.gather(sched.acquire(30), sched.acquire(30))
    # Exactly one of the two acquired immediately; the other had to sleep.
    assert sorted(s > 0 for s in slept) == [False, True]
    # Net reservation never let the modeled bucket go negative.
    assert sched.tokens >= 0.0


async def test_acquire_rejects_more_than_capacity() -> None:
    sched = SignalAttachmentScheduler(capacity=5)
    with pytest.raises(SignalSchedulerError):
        await sched.acquire(6)


async def test_report_rpc_duration_only_resets_clock() -> None:
    # Tokens are deducted at acquire(); report_rpc_duration must not deduct
    # again (that would double-count the send).
    sched = SignalAttachmentScheduler()
    await sched.acquire(3)
    after_acquire = sched.tokens
    await sched.report_rpc_duration(1.0, 3)
    assert sched.tokens == pytest.approx(after_acquire)


def test_feedback_calibrates_refill_rate_and_drains() -> None:
    sched = SignalAttachmentScheduler()
    sched.feedback(retry_after=8.0, n_attempted=1)
    # refill_rate becomes 1 token / retry_after seconds; bucket drained to 0.
    assert sched.refill_rate == pytest.approx(1.0 / 8.0)
    assert sched.tokens == 0.0


def test_feedback_without_retry_after_keeps_rate_but_drains() -> None:
    sched = SignalAttachmentScheduler()
    original_rate = sched.refill_rate
    sched.feedback(retry_after=None, n_attempted=1)
    assert sched.refill_rate == original_rate
    assert sched.tokens == 0.0


@pytest.mark.parametrize(
    ("err", "expected"),
    [
        ({"code": -5, "message": "RateLimitException"}, True),
        ({"message": "[429] too many requests"}, True),
        ({"message": "Retry after 4 seconds"}, True),
        ({"message": "RetryLaterException"}, True),
        ({"message": "some other failure"}, False),
        ("plain string error", False),
    ],
)
def test_is_signal_rate_limit_error(err: object, expected: bool) -> None:
    assert _is_signal_rate_limit_error(err) is expected


def test_extract_retry_after_from_structured_field() -> None:
    err = {
        "message": "rate limited",
        "data": {"response": {"results": [{"retryAfterSeconds": 12}]}},
    }
    assert _extract_retry_after_seconds(err) == 12.0


def test_extract_retry_after_from_message() -> None:
    assert _extract_retry_after_seconds({"message": "Retry after 7 seconds"}) == 7.0
    assert _extract_retry_after_seconds("Retry after 3.5 second") == 3.5


def test_extract_retry_after_returns_none_when_absent() -> None:
    assert _extract_retry_after_seconds({"message": "nope"}) is None


@pytest.mark.parametrize(
    ("n", "expected"),
    [(0, 30.0), (1, 60.0), (20, 100.0)],
)
def test_signal_send_timeout_scales(n: int, expected: float) -> None:
    assert _signal_send_timeout(n) == expected


def test_get_scheduler_is_singleton() -> None:
    from bt_signal_gateway.signal_rate_limit import _reset_scheduler

    _reset_scheduler()
    a = get_scheduler()
    b = get_scheduler()
    assert a is b
    assert a.capacity == float(SIGNAL_RATE_LIMIT_BUCKET_CAPACITY)
    _reset_scheduler()
