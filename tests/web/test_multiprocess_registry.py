"""
make_registry(): the default registry in a single process, an aggregate of
every worker's metric files when PROMETHEUS_MULTIPROC_DIR is set.

prometheus_client picks its value class (in memory or mmap file) when it is
first imported, and the analytics metrics cannot be re-created on the default
registry, so multiprocess mode is exercised in a fresh interpreter.
"""

import os
import subprocess
import sys
import textwrap

from prometheus_client import REGISTRY as DEFAULT_REGISTRY

from aleph.web.middlewares.analytics import make_registry

LABELS = {"route": "/multiprocess/test", "method": "GET", "status": "200"}

CHILD_SCRIPT = textwrap.dedent(
    """
    import multiprocessing

    from prometheus_client import generate_latest

    from aleph.web.middlewares.analytics import HTTP_REQUESTS, make_registry

    LABELS = {labels!r}


    def worker():
        HTTP_REQUESTS.labels(**LABELS).inc()


    if __name__ == "__main__":
        HTTP_REQUESTS.labels(**LABELS).inc()
        process = multiprocessing.get_context("fork").Process(target=worker)
        process.start()
        process.join()
        assert process.exitcode == 0, process.exitcode
        print(generate_latest(make_registry()).decode())
    """
).format(labels=LABELS)


def test_single_process_registry_is_default(monkeypatch):
    monkeypatch.delenv("PROMETHEUS_MULTIPROC_DIR", raising=False)
    assert make_registry() is DEFAULT_REGISTRY


def test_multiprocess_registry_sums_workers(tmp_path):
    env = dict(os.environ, PROMETHEUS_MULTIPROC_DIR=str(tmp_path))
    result = subprocess.run(
        [sys.executable, "-c", CHILD_SCRIPT],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr

    expected = (
        'aleph_http_requests_total{method="GET",route="/multiprocess/test",'
        'status="200"} 2.0'
    )
    assert expected in result.stdout, result.stdout
