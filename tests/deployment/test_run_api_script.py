import os
import socket
import subprocess
import sys
import textwrap
import time
import urllib.request
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
API_BIND_SH = REPO_ROOT / "deployment" / "scripts" / "api_bind.sh"


def compute_binds(port: int, **env) -> list[str]:
    full_env = {k: v for k, v in os.environ.items() if not k.startswith("CCN_")}
    full_env.update(env)
    result = subprocess.run(
        ["bash", "-c", f'source "{API_BIND_SH}" && compute_api_binds {port}'],
        env=full_env,
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    return result.stdout.split()


@pytest.fixture
def ipv6_file(tmp_path: Path) -> Path:
    path = tmp_path / "if_inet6"
    path.write_text("00000000000000000000000000000001 01 80 10 80       lo\n")
    return path


def test_binds_ipv4_only_without_ipv6(tmp_path: Path):
    empty = tmp_path / "if_inet6"
    empty.write_text("")
    assert compute_binds(4024, CCN_IPV6_PROBE_FILE=str(empty)) == ["0.0.0.0:4024"]
    missing = tmp_path / "does-not-exist"
    assert compute_binds(4024, CCN_IPV6_PROBE_FILE=str(missing)) == ["0.0.0.0:4024"]


def test_binds_dual_stack_socket_with_ipv6(tmp_path: Path, ipv6_file: Path):
    v6only = tmp_path / "bindv6only"
    v6only.write_text("0\n")
    binds = compute_binds(
        4024,
        CCN_IPV6_PROBE_FILE=str(ipv6_file),
        CCN_BINDV6ONLY_FILE=str(v6only),
    )
    assert binds == ["[::]:4024"]


def test_binds_both_families_when_host_forces_v6only(tmp_path: Path, ipv6_file: Path):
    v6only = tmp_path / "bindv6only"
    v6only.write_text("1\n")
    binds = compute_binds(
        4024,
        CCN_IPV6_PROBE_FILE=str(ipv6_file),
        CCN_BINDV6ONLY_FILE=str(v6only),
    )
    assert binds == ["0.0.0.0:4024", "[::]:4024"]


def test_explicit_bind_wins(tmp_path: Path, ipv6_file: Path):
    binds = compute_binds(
        4024,
        CCN_IPV6_PROBE_FILE=str(ipv6_file),
        CCN_CONFIG_API_BIND="127.0.0.1:9000 [::1]:9001",
    )
    assert binds == ["127.0.0.1:9000", "[::1]:9001"]


def test_blank_explicit_bind_falls_back_to_default(tmp_path: Path):
    empty = tmp_path / "if_inet6"
    empty.write_text("")
    binds = compute_binds(
        4024, CCN_IPV6_PROBE_FILE=str(empty), CCN_CONFIG_API_BIND="  \t "
    )
    assert binds == ["0.0.0.0:4024"]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _has_ipv6_loopback() -> bool:
    try:
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as sock:
            sock.bind(("::1", 0))
    except OSError:
        return False
    return True


def _get(url: str) -> str:
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.read().decode()


@pytest.mark.skipif(not _has_ipv6_loopback(), reason="no IPv6 loopback")
def test_gunicorn_serves_ipv4_and_ipv6_with_computed_binds(tmp_path: Path):
    (tmp_path / "tiny_app.py").write_text(
        textwrap.dedent(
            """
            from aiohttp import web

            async def hello(request):
                return web.Response(text="hello")

            async def create_app():
                app = web.Application()
                app.router.add_get("/", hello)
                return app
            """
        )
    )
    port = _free_port()
    # Same computation as the run script, with the real IPv6 probe of this machine.
    binds = compute_binds(port)
    assert any(bind.startswith("[::]") for bind in binds), binds

    command = [sys.executable, "-m", "gunicorn", "tiny_app:create_app"]
    for bind in binds:
        command += ["--bind", bind]
    # The script uses the uvloop variant of this worker; the plain one exercises the same
    # socket handling without making uvloop a requirement of the test.
    command += [
        "--worker-class",
        "aiohttp.worker.GunicornWebWorker",
        "--workers",
        "1",
        "--chdir",
        str(tmp_path),
    ]
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    try:
        deadline = time.monotonic() + 20
        while True:
            if process.poll() is not None:
                output = process.stdout.read() if process.stdout else ""
                pytest.fail(f"gunicorn exited early: {output}")
            try:
                assert _get(f"http://127.0.0.1:{port}/") == "hello"
                break
            except OSError:
                if time.monotonic() > deadline:
                    pytest.fail("gunicorn did not start in time")
                time.sleep(0.2)

        assert _get(f"http://[::1]:{port}/") == "hello"
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
