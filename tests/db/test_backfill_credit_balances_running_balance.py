"""Tests for the running_balance backfill script."""

import datetime as dt
import importlib.util
from pathlib import Path

import pytest
from sqlalchemy import select

from aleph.db.accessors.balances import (
    update_credit_balances_distribution,
    update_credit_balances_expense,
)
from aleph.db.models import AlephCreditBalanceDb
from aleph.types.db_session import DbSessionFactory


def _load_script_module():
    """Load the backfill script as a module so we can call its `run` function."""
    repo_root = Path(__file__).resolve().parents[2]
    script_path = (
        repo_root
        / "deployment"
        / "scripts"
        / "backfill_credit_balances_running_balance.py"
    )
    spec = importlib.util.spec_from_file_location(
        "backfill_running_balance", script_path
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def backfill_module():
    return _load_script_module()


def _seed_history(session_factory: DbSessionFactory) -> None:
    """Seed credit_history with two addresses then null out running_balance to
    simulate a DB that pre-dates the eager-update writers."""
    dist_ts = dt.datetime(2023, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
    exp_ts = dt.datetime(2023, 1, 2, 12, 0, 0, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        update_credit_balances_distribution(
            session=session,
            credits_list=[
                {
                    "address": "0xbf_a",
                    "amount": 1000,
                    "price": "0.1",
                    "tx_hash": "0xtx",
                    "provider": "p",
                },
                {
                    "address": "0xbf_b",
                    "amount": 500,
                    "price": "0.1",
                    "tx_hash": "0xtx",
                    "provider": "p",
                },
            ],
            token="ALEPH",
            chain="ETH",
            message_hash="msg_bf_dist",
            message_timestamp=dist_ts,
        )
        update_credit_balances_expense(
            session=session,
            credits_list=[{"address": "0xbf_a", "amount": 200, "ref": "r"}],
            message_hash="msg_bf_exp",
            message_timestamp=exp_ts,
        )
        for address in ("0xbf_a", "0xbf_b"):
            row = session.execute(
                select(AlephCreditBalanceDb).where(
                    AlephCreditBalanceDb.address == address
                )
            ).scalar_one()
            row.running_balance = None
        session.commit()


def test_backfill_populates_running_balance_from_history(
    session_factory: DbSessionFactory, backfill_module
):
    _seed_history(session_factory)

    backfill_module.run(session_factory)

    with session_factory() as session:
        row_a = session.execute(
            select(AlephCreditBalanceDb).where(AlephCreditBalanceDb.address == "0xbf_a")
        ).scalar_one()
        row_b = session.execute(
            select(AlephCreditBalanceDb).where(AlephCreditBalanceDb.address == "0xbf_b")
        ).scalar_one()
        assert row_a.running_balance == 800 * 10000
        assert row_b.running_balance == 500 * 10000


def test_backfill_is_idempotent(session_factory: DbSessionFactory, backfill_module):
    _seed_history(session_factory)

    backfill_module.run(session_factory)
    backfill_module.run(session_factory)

    with session_factory() as session:
        row_a = session.execute(
            select(AlephCreditBalanceDb).where(AlephCreditBalanceDb.address == "0xbf_a")
        ).scalar_one()
        assert row_a.running_balance == 800 * 10000


def test_backfill_creates_row_for_address_with_no_credit_balance(
    session_factory: DbSessionFactory, backfill_module
):
    """If credit_history has an address but credit_balances does not, backfill creates the row."""
    dist_ts = dt.datetime(2023, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        update_credit_balances_distribution(
            session=session,
            credits_list=[
                {
                    "address": "0xbf_orphan",
                    "amount": 42,
                    "price": "0.1",
                    "tx_hash": "0xtx",
                    "provider": "p",
                }
            ],
            token="ALEPH",
            chain="ETH",
            message_hash="msg_bf_orphan",
            message_timestamp=dist_ts,
        )
        session.execute(
            AlephCreditBalanceDb.__table__.delete().where(
                AlephCreditBalanceDb.address == "0xbf_orphan"
            )
        )
        session.commit()

    backfill_module.run(session_factory)

    with session_factory() as session:
        row = session.execute(
            select(AlephCreditBalanceDb).where(
                AlephCreditBalanceDb.address == "0xbf_orphan"
            )
        ).scalar_one()
        assert row.running_balance == 42 * 10000
