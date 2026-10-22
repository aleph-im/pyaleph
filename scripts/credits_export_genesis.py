#!/usr/bin/env python3
"""
Export the credit genesis snapshot from the legacy credit tables.

    python scripts/credits_export_genesis.py --config config.yml \
        --period 1792000000 --denylist 0xabc...,0xdef... --out genesis.json

The denylist is `aleph.credits.chain.denylist` from the config, plus the
addresses given with `--denylist`.

Prints per-address totals, the excluded rows, the sha256 of the file (the
`credits.genesis.hash` once the file is uploaded as a STORE) and the config
lines to paste. Exits 1 on a database error.
"""

import argparse
import datetime as dt
import sys
from collections import defaultdict
from typing import List, Optional

from sqlalchemy.exc import SQLAlchemyError

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
        "--denylist",
        default="",
        help=(
            "comma-separated addresses to exclude, on top of "
            "aleph.credits.chain.denylist"
        ),
    )
    parser.add_argument("--out", required=True, help="output file")
    args = parser.parse_args(argv)

    if args.period % 3600:
        parser.error("--period must be an hour start")
    period = dt.datetime.fromtimestamp(args.period, tz=dt.timezone.utc)

    config = aleph.config.app_config
    if args.config:
        config.yaml.load(args.config)
    denylist = list(config.aleph.credits.chain.denylist.value or []) + [
        address for address in args.denylist.split(",") if address.strip()
    ]
    try:
        session_factory = make_session_factory(make_engine(config))
        with session_factory() as session:
            kept, excluded = export_genesis(session, period, denylist)
    except SQLAlchemyError as error:
        message = str(error).splitlines()[0] if str(error) else type(error).__name__
        print(f"Database error: {message}", file=sys.stderr)
        return 1

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
    print(f"Denylist ({len(denylist)}): {', '.join(denylist)}")
    print(f"Total credits: {sum(totals.values())}")
    digest = genesis_hash(content)
    print(f"sha256 {digest}")
    print("Config (once the file is uploaded as a STORE with this hash):")
    print(f"  credits.genesis.hash: {digest}")
    print(f"  credits.genesis.period: {args.period}")
    print(f"  credits.attestation.start_period: {args.period}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
