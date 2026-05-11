#!/usr/bin/env python3
"""Backfill credit_balances.running_balance for every address in credit_history.

One-shot maintenance script. Run once after deploying the eager-update writers
so existing addresses get their running_balance populated. Idempotent: a
re-run recomputes from credit_history and overwrites.

Per-address transactions keep the script restartable.

Usage:
    python deployment/scripts/backfill_credit_balances_running_balance.py
"""

import argparse
import logging
import sys
from pathlib import Path

# Allow running this file directly from a repo checkout.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from sqlalchemy import func, select  # noqa: E402
from sqlalchemy.dialects.postgresql import insert as pg_insert  # noqa: E402

import aleph.config  # noqa: E402
from aleph.db.connection import make_engine, make_session_factory  # noqa: E402
from aleph.db.models import AlephCreditBalanceDb, AlephCreditHistoryDb  # noqa: E402
from aleph.types.db_session import DbSessionFactory  # noqa: E402

LOGGER = logging.getLogger("backfill_running_balance")


def _list_addresses(session_factory: DbSessionFactory) -> list[str]:
    with session_factory() as session:
        return list(
            session.execute(select(AlephCreditHistoryDb.address).distinct()).scalars()
        )


def _compute_running_balance(session_factory: DbSessionFactory, address: str) -> int:
    with session_factory() as session:
        result = session.execute(
            select(func.coalesce(func.sum(AlephCreditHistoryDb.amount), 0)).where(
                AlephCreditHistoryDb.address == address
            )
        ).scalar_one()
        return int(result)


def _upsert_running_balance(
    session_factory: DbSessionFactory, address: str, running_balance: int
) -> None:
    with session_factory() as session:
        stmt = pg_insert(AlephCreditBalanceDb).values(
            address=address,
            balance=0,
            running_balance=running_balance,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[AlephCreditBalanceDb.address],
            set_={
                "running_balance": stmt.excluded.running_balance,
                "last_update": func.now(),
            },
        )
        session.execute(stmt)
        session.commit()


def run(session_factory: DbSessionFactory, log_every: int = 100) -> None:
    """Backfill running_balance for all addresses present in credit_history.

    One transaction per address; safe to interrupt and resume.
    """
    addresses = _list_addresses(session_factory)
    total = len(addresses)
    LOGGER.info("backfilling running_balance for %d addresses", total)

    for i, address in enumerate(addresses):
        running = _compute_running_balance(session_factory, address)
        _upsert_running_balance(session_factory, address, running)
        if (i + 1) % log_every == 0:
            LOGGER.info("backfilled %d / %d addresses", i + 1, total)

    LOGGER.info("backfill complete (%d addresses)", total)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        dest="config_file",
        help="Path to the pyaleph YAML config file",
    )
    args = parser.parse_args()

    config = aleph.config.app_config
    if args.config_file is not None:
        config.yaml.load(args.config_file)

    engine = make_engine(config=config, application_name="backfill_running_balance")
    session_factory = make_session_factory(engine)
    run(session_factory)
    return 0


if __name__ == "__main__":
    sys.exit(main())
