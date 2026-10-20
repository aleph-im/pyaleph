#!/usr/bin/env python3
"""
Export the credit genesis snapshot from the legacy credit tables.

    python scripts/credits_export_genesis.py --config config.yml \
        --period 1792000000 --denylist 0xabc...,0xdef... --out genesis.json

Prints per-address totals, the excluded rows and the sha256 of the file (the
`credits.genesis.hash` once the file is uploaded as a STORE).
"""

import argparse
import datetime as dt
import sys
from collections import defaultdict
from typing import List, Optional

import aleph.config
from aleph.db.connection import make_engine, make_session_factory
from aleph.services.credits.genesis_export import (
    export_genesis,
    genesis_bytes,
    genesis_hash,
)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", help="pyaleph config.yml (database settings)")
    parser.add_argument(
        "--period", type=int, required=True, help="genesis period, unix hour start"
    )
    parser.add_argument(
        "--denylist", default="", help="comma-separated addresses to exclude"
    )
    parser.add_argument("--out", required=True, help="output file")
    args = parser.parse_args(argv)

    if args.period % 3600:
        parser.error("--period must be an hour start")
    period = dt.datetime.fromtimestamp(args.period, tz=dt.timezone.utc)

    config = aleph.config.app_config
    if args.config:
        config.yaml.load(args.config)
    session_factory = make_session_factory(make_engine(config))
    with session_factory() as session:
        kept, excluded = export_genesis(session, period, args.denylist.split(","))

    content = genesis_bytes(kept)
    with open(args.out, "wb") as out:
        out.write(content)

    totals = defaultdict(int)
    for entry in kept:
        totals[entry["address"]] += entry["amount"]
    print(f"Per-address totals ({len(totals)} addresses, {len(kept)} lots):")
    for address in sorted(totals):
        print(f"  {address} {totals[address]}")
    print(f"Excluded rows ({len(excluded)}):")
    for entry in excluded:
        print(
            f"  {entry['reason']} {entry['address']} {entry['amount']} "
            f"{entry['ref']}:{entry['index']} exp={entry['expiration']}"
        )
    print(f"Total credits: {sum(totals.values())}")
    print(f"sha256 {genesis_hash(content)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
