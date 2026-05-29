"""Tests for the retry-backoff jitter applied to pending-message scheduling."""

import datetime as dt

import pytest

from aleph.jobs.job_utils import MAX_RETRY_INTERVAL, compute_next_retry_interval


def test_compute_next_retry_interval_zero_attempts_bounded_by_one_second():
    """First retry must fall within [0, 1] seconds (base = 2**0 = 1)."""
    for _ in range(50):
        delay = compute_next_retry_interval(0)
        assert dt.timedelta(0) <= delay <= dt.timedelta(seconds=1)


@pytest.mark.parametrize("attempts", [1, 2, 5, 8, 9])
def test_compute_next_retry_interval_bounded_by_exponential_cap(attempts):
    """Each draw stays within [0, min(2**attempts, MAX_RETRY_INTERVAL)] seconds.

    attempts=9 is the first value where 2**attempts (512) exceeds the cap (300),
    so it doubles as a boundary check that the clamp kicks in.
    """
    cap_seconds = min(2**attempts, MAX_RETRY_INTERVAL)
    cap = dt.timedelta(seconds=cap_seconds)
    for _ in range(50):
        delay = compute_next_retry_interval(attempts)
        assert dt.timedelta(0) <= delay <= cap


def test_compute_next_retry_interval_capped_at_max():
    """Large attempt counts cannot exceed MAX_RETRY_INTERVAL."""
    cap = dt.timedelta(seconds=MAX_RETRY_INTERVAL)
    for _ in range(50):
        delay = compute_next_retry_interval(20)
        assert delay <= cap


def test_compute_next_retry_interval_is_jittered():
    """Successive calls at the same attempt count produce distinct values
    (the jitter actually decorrelates retries)."""
    samples = {compute_next_retry_interval(5) for _ in range(50)}
    # With a continuous uniform draw over a 32-second window, 50 samples
    # should yield many distinct values; collapsing to <5 would be a sign
    # the function reverted to a deterministic formula.
    assert len(samples) >= 5
