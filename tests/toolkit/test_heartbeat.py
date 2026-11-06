from pathlib import Path

import pytest

import aleph.toolkit.heartbeat as heartbeat_module
from aleph.toolkit.heartbeat import Heartbeat, read_age


class FakeClock:
    def __init__(self, start: float):
        self.now = start

    def time(self) -> float:
        return self.now

    def monotonic(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch) -> FakeClock:
    fake = FakeClock(start=1_700_000_000.0)
    monkeypatch.setattr(heartbeat_module, "time", fake)
    return fake


def test_beat_creates_file_with_timestamp(tmp_path: Path):
    directory = tmp_path / "does" / "not" / "exist"
    Heartbeat("aleph.jobs.test", directory).beat()

    content = (directory / "aleph.jobs.test").read_text()
    assert float(content) > 0
    # Only the heartbeat itself, no leftover temporary file.
    assert [p.name for p in directory.iterdir()] == ["aleph.jobs.test"]


def test_beat_is_rate_limited(tmp_path: Path, clock: FakeClock):
    heartbeat = Heartbeat("job", tmp_path)
    heartbeat.beat()
    assert float((tmp_path / "job").read_text()) == clock.now

    first = clock.now
    clock.now += 0.5
    heartbeat.beat()
    assert float((tmp_path / "job").read_text()) == first

    clock.now += 0.6
    heartbeat.beat()
    assert float((tmp_path / "job").read_text()) == clock.now


def test_read_age_missing_file(tmp_path: Path):
    assert read_age("job", tmp_path) is None
    assert read_age("job", tmp_path / "missing-dir") is None


def test_read_age_unparsable_file(tmp_path: Path):
    (tmp_path / "job").write_text("not a timestamp")
    assert read_age("job", tmp_path) is None


def test_read_age_reflects_elapsed_time(tmp_path: Path, clock: FakeClock):
    Heartbeat("job", tmp_path).beat()

    assert read_age("job", tmp_path, now=clock.now) == 0
    assert read_age("job", tmp_path, now=clock.now + 42.5) == 42.5
