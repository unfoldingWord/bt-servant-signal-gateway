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


def test_acquire_returns_immediately_when_tokens_available() -> None:
    sched = SignalAttachmentScheduler()
    # Fresh bucket starts full, so acquiring a small amount sleeps 0s.
    assert sched.tokens == sched.capacity

    async def _run() -> float:
        return await sched.acquire(1)

    import asyncio

    assert asyncio.run(_run()) == 0.0


async def test_acquire_rejects_more_than_capacity() -> None:
    sched = SignalAttachmentScheduler(capacity=5)
    with pytest.raises(SignalSchedulerError):
        await sched.acquire(6)


async def test_report_rpc_duration_deducts_tokens() -> None:
    sched = SignalAttachmentScheduler()
    before = sched.tokens
    await sched.report_rpc_duration(1.0, 3)
    assert sched.tokens == pytest.approx(before - 3.0)


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
