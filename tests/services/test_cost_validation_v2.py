"""The ingest balance check reads the v2 ledger under `serve_v2`."""

import datetime as dt
from decimal import Decimal

import pytest
from aleph_message.models import PaymentType
from configmanager import Config
from credit_v2_helpers import OWNER, seed_lot, set_last_closed_period

from aleph.db.models import AlephCreditBalanceDb
from aleph.services.cost_validation import validate_balance_for_payment
from aleph.toolkit.constants import DAY
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import InsufficientCreditException

LAST_CLOSED = dt.datetime(2025, 6, 1, 11, tzinfo=dt.timezone.utc)
RATE = Decimal("0.01")  # credits per second


@pytest.fixture
def serve_v2(mock_config: Config) -> Config:
    mock_config.aleph.credits.serve_v2.value = True
    return mock_config


def test_ingest_check_uses_v2_balance(
    session_factory: DbSessionFactory, serve_v2: Config
):
    with session_factory() as session:
        set_last_closed_period(session, LAST_CLOSED)
        session.commit()

        # Legacy tables empty, no v2 lots: rejected.
        with pytest.raises(InsufficientCreditException):
            validate_balance_for_payment(session, OWNER, RATE, PaymentType.credit)

        # One day of runtime in v2 lots (stored lowercased): accepted for the
        # checksummed address.
        seed_lot(session, OWNER.lower(), int(RATE * DAY), period=LAST_CLOSED)
        validate_balance_for_payment(session, OWNER, RATE, PaymentType.credit)


def test_ingest_check_ignores_legacy_balance_under_v2(
    session_factory: DbSessionFactory, serve_v2: Config
):
    with session_factory() as session:
        set_last_closed_period(session, LAST_CLOSED)
        session.add(
            AlephCreditBalanceDb(
                address=OWNER,
                credit_ref="legacy",
                credit_index=0,
                amount_remaining=int(RATE * DAY) * 10,
                expiration_date=None,
                message_timestamp=LAST_CLOSED,
                last_update=LAST_CLOSED,
            )
        )
        session.commit()

        with pytest.raises(InsufficientCreditException):
            validate_balance_for_payment(session, OWNER, RATE, PaymentType.credit)


def test_ingest_check_reads_legacy_balance_when_flag_off(
    session_factory: DbSessionFactory, mock_config: Config
):
    assert mock_config.aleph.credits.serve_v2.value is False
    with session_factory() as session:
        set_last_closed_period(session, LAST_CLOSED)
        seed_lot(session, OWNER.lower(), int(RATE * DAY) * 10, period=LAST_CLOSED)
        session.commit()

        with pytest.raises(InsufficientCreditException):
            validate_balance_for_payment(session, OWNER, RATE, PaymentType.credit)
