from aleph.toolkit.timer import Timer
import time


def test_timer_sleep():
    sleep_duration = 0.5

    with Timer() as t:
        time.sleep(sleep_duration)

    assert t.elapsed() >= sleep_duration
