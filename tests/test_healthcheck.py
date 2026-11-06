import time
from pathlib import Path

from aleph.healthcheck import main

JOBS = ["aleph.jobs.a", "aleph.jobs.b"]


def _write(directory: Path, name: str, age: float) -> None:
    (directory / name).write_text(str(time.time() - age))


def _argv(directory: Path, *extra: str) -> list[str]:
    argv = ["--directory", str(directory), "--max-age", "60"]
    for job in JOBS:
        argv += ["--job", job]
    return argv + list(extra)


def test_all_fresh(tmp_path: Path, capsys):
    _write(tmp_path, "aleph.jobs.a", 3)
    _write(tmp_path, "aleph.jobs.b", 0)

    assert main(_argv(tmp_path)) == 0
    out = capsys.readouterr().out.splitlines()
    assert out == ["aleph.jobs.a: ok 3s", "aleph.jobs.b: ok 0s"]


def test_one_stale(tmp_path: Path, capsys):
    _write(tmp_path, "aleph.jobs.a", 3)
    _write(tmp_path, "aleph.jobs.b", 412)

    assert main(_argv(tmp_path)) == 1
    out = capsys.readouterr().out.splitlines()
    assert out == ["aleph.jobs.a: ok 3s", "aleph.jobs.b: stale 412s"]


def test_missing(tmp_path: Path, capsys):
    _write(tmp_path, "aleph.jobs.a", 3)

    assert main(_argv(tmp_path)) == 1
    assert "aleph.jobs.b: missing" in capsys.readouterr().out


def test_defaults_come_from_config_file(tmp_path: Path, capsys):
    heartbeat_dir = tmp_path / "heartbeats"
    heartbeat_dir.mkdir()
    config_file = tmp_path / "config.yml"
    config_file.write_text(
        "aleph:\n"
        "  jobs:\n"
        "    heartbeat:\n"
        f"      directory: {heartbeat_dir}\n"
        "      max_age_s: 10\n"
    )
    from aleph.toolkit.heartbeat import SUBPROCESS_JOB_NAMES

    for name in SUBPROCESS_JOB_NAMES:
        _write(heartbeat_dir, name, 5)
    assert main(["--config", str(config_file)]) == 0

    _write(heartbeat_dir, SUBPROCESS_JOB_NAMES[0], 20)
    assert main(["--config", str(config_file)]) == 1
    out = capsys.readouterr().out
    assert f"{SUBPROCESS_JOB_NAMES[0]}: stale 20s" in out
