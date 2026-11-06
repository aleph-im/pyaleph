"""
`pyaleph-healthcheck`: liveness probe for the job subprocesses of a pyaleph node.

Reads the heartbeat files written by the job loops and exits non-zero if one of
them is missing or too old. Meant for Docker HEALTHCHECK and Kubernetes exec
probes, so it avoids importing anything that touches the database or network.
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional

from configmanager import Config

from aleph.config import get_defaults
from aleph.toolkit.heartbeat import SUBPROCESS_JOB_NAMES, read_age


def parse_args(args: Optional[List[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pyaleph-healthcheck",
        description="Check that the pyaleph job loops are still making progress.",
    )
    parser.add_argument(
        "-c",
        "--config",
        dest="config_file",
        help="pyaleph configuration file, read for the heartbeat directory and max age.",
    )
    parser.add_argument(
        "--directory", type=Path, help="Heartbeat directory (overrides the config)."
    )
    parser.add_argument(
        "--max-age",
        type=float,
        help="Maximum heartbeat age in seconds (overrides the config). "
        "0 only checks that the heartbeats exist.",
    )
    parser.add_argument(
        "--job",
        dest="jobs",
        action="append",
        help="Job to check, may be repeated. Default: all job subprocesses.",
    )
    return parser.parse_args(args)


def main(args: Optional[List[str]] = None) -> int:
    parsed = parse_args(args)

    config = Config(schema=get_defaults())
    if parsed.config_file is not None:
        config.yaml.load(parsed.config_file)

    directory = parsed.directory or Path(config.aleph.jobs.heartbeat.directory.value)
    max_age = (
        parsed.max_age
        if parsed.max_age is not None
        else float(config.aleph.jobs.heartbeat.max_age_s.value)
    )
    jobs = parsed.jobs or list(SUBPROCESS_JOB_NAMES)

    healthy = True
    for job in jobs:
        age = read_age(job, directory)
        if age is None:
            print(f"{job}: missing")
            healthy = False
        elif max_age > 0 and age > max_age:
            print(f"{job}: stale {age:.0f}s")
            healthy = False
        else:
            print(f"{job}: ok {age:.0f}s")

    return 0 if healthy else 1


if __name__ == "__main__":
    sys.exit(main())
