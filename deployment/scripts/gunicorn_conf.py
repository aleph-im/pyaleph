"""Gunicorn configuration hooks for the Aleph CCN API."""

import os


def child_exit(server, worker):
    """Clean up the Prometheus multiprocess files of a worker that exited.

    A worker killed in the middle of a request (timeout, OOM, restart) would
    otherwise leave its ``gauge_livesum_<pid>.db`` file in
    ``PROMETHEUS_MULTIPROC_DIR`` and the ``aleph_http_requests_in_flight`` gauge
    would drift upward until the next restart. Counters and histograms are kept.
    """
    if not os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
        return

    from prometheus_client import multiprocess

    multiprocess.mark_process_dead(worker.pid)
