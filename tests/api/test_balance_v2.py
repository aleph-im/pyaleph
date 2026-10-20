"""GET /api/v0/addresses/{address}/balance served from the v2 ledger (spec 5.9)."""

import datetime as dt
import logging

import pytest
from aleph_message.models import Chain, ItemType, MessageType, PaymentType
from configmanager import Config
from credit_v2_helpers import (
    OWNER,
    instance_spec,
    seed_instance,
    seed_lot,
    set_last_closed_period,
)
from sqlalchemy import delete

from aleph.db.accessors.credit_ledger import upsert_billing_resource
from aleph.db.models import AlephCreditBalanceDb, MessageDb
from aleph.db.models.credit_ledger import BillingResourceDb, CreditFoldStateDb
from aleph.services.credits import balance as balance_service
from aleph.services.credits.fold import execution_amount
from aleph.services.credits.rates import rate_for
from aleph.types.db_session import DbSessionFactory

# A closed period; the open hour started 10 minutes before NOW.
LAST_CLOSED = dt.datetime(2025, 6, 1, 11, tzinfo=dt.timezone.utc)
SINCE = LAST_CLOSED + dt.timedelta(hours=1)
NOW = SINCE + dt.timedelta(seconds=600)
VM = "1" * 64


def _uri(address: str) -> str:
    return f"/api/v0/addresses/{address}/balance"


@pytest.fixture
def frozen_now(mocker):
    mocker.patch.object(balance_service, "utc_now", return_value=NOW)
    return NOW


@pytest.fixture
def serve_v2(mock_config: Config) -> Config:
    mock_config.aleph.credits.serve_v2.value = True
    return mock_config


def _seed_funded_instance(session_factory: DbSessionFactory, amount: int):
    with session_factory() as session:
        set_last_closed_period(session, LAST_CLOSED)
        seed_lot(session, OWNER.lower(), amount, period=LAST_CLOSED)
        resource = seed_instance(
            session, VM, first_confirmed_at=SINCE - dt.timedelta(days=1)
        )
        rate = rate_for(session, resource, SINCE)
        session.commit()
    assert rate > 0
    return rate


@pytest.mark.asyncio
async def test_balance_v2_subtracts_live_hour_accrual(
    ccn_api_client, session_factory: DbSessionFactory, serve_v2, frozen_now
):
    rate = _seed_funded_instance(session_factory, 1_000_000)
    expected = 1_000_000 - execution_amount(rate, 600)
    assert expected < 1_000_000

    response = await ccn_api_client.get(_uri(OWNER))
    assert response.status == 200, await response.text()
    assert (await response.json())["credit_balance"] == expected

    response = await ccn_api_client.get(_uri(OWNER) + "?closed_only=true")
    assert response.status == 200, await response.text()
    assert (await response.json())["credit_balance"] == 1_000_000

    with session_factory() as session:
        assert balance_service.get_credit_balance_v2(session, OWNER, NOW) == expected
        assert (
            balance_service.get_credit_balance_v2(session, OWNER, NOW, closed_only=True)
            == 1_000_000
        )


@pytest.mark.asyncio
async def test_balance_v2_details_drain_the_accrual(
    ccn_api_client, session_factory: DbSessionFactory, serve_v2, frozen_now
):
    rate = _seed_funded_instance(session_factory, 1_000_000)
    accrual = execution_amount(rate, 600)

    response = await ccn_api_client.get(_uri(OWNER) + "?include_credit_details=true")
    assert response.status == 200, await response.text()
    data = await response.json()
    assert data["credit_balance"] == 1_000_000 - accrual
    assert [d["amount"] for d in data["credit_balance_details"]] == [
        1_000_000 - accrual
    ]


@pytest.mark.asyncio
async def test_balance_never_negative(
    ccn_api_client, session_factory: DbSessionFactory, serve_v2, frozen_now
):
    rate = _seed_funded_instance(session_factory, 1)
    assert execution_amount(rate, 600) > 1

    response = await ccn_api_client.get(_uri(OWNER))
    assert response.status == 200, await response.text()
    assert (await response.json())["credit_balance"] == 0

    with session_factory() as session:
        assert balance_service.get_credit_balance_v2(session, OWNER, NOW) == 0


@pytest.mark.asyncio
async def test_balance_v2_is_case_insensitive_for_evm_addresses(
    ccn_api_client, session_factory: DbSessionFactory, serve_v2, frozen_now
):
    _seed_funded_instance(session_factory, 1_000_000)

    checksummed = await (await ccn_api_client.get(_uri(OWNER))).json()
    lowered = await (await ccn_api_client.get(_uri(OWNER.lower()))).json()
    assert checksummed["credit_balance"] == lowered["credit_balance"]
    assert checksummed["credit_balance"] > 0


@pytest.mark.asyncio
async def test_accrual_window_is_bounded_by_confirmation_and_end_of_life(
    session_factory: DbSessionFactory, serve_v2
):
    with session_factory() as session:
        set_last_closed_period(session, LAST_CLOSED)
        # Confirmed 2 minutes into the open hour, forgotten 5 minutes in.
        resource = seed_instance(
            session,
            VM,
            first_confirmed_at=SINCE + dt.timedelta(seconds=120),
            end_of_life=SINCE + dt.timedelta(seconds=300),
        )
        # Ended before the open hour: no accrual.
        seed_instance(
            session,
            "2" * 64,
            first_confirmed_at=SINCE - dt.timedelta(days=1),
            end_of_life=SINCE - dt.timedelta(seconds=1),
        )
        # Not credit-paid: no accrual.
        seed_instance(
            session,
            "3" * 64,
            first_confirmed_at=SINCE - dt.timedelta(days=1),
            payment_type=PaymentType.hold,
        )
        rate = rate_for(session, resource, SINCE)
        assert balance_service.provisional_accrual(
            session, OWNER, SINCE, NOW
        ) == execution_amount(rate, 180)


@pytest.mark.asyncio
async def test_balance_serves_legacy_when_flag_off(
    ccn_api_client, session_factory: DbSessionFactory, mock_config: Config
):
    assert mock_config.aleph.credits.serve_v2.value is False
    _seed_funded_instance(session_factory, 1_000_000)
    with session_factory() as session:
        session.add(
            AlephCreditBalanceDb(
                address=OWNER,
                credit_ref="legacy",
                credit_index=0,
                amount_remaining=42,
                expiration_date=None,
                message_timestamp=LAST_CLOSED,
                last_update=LAST_CLOSED,
            )
        )
        session.commit()

    response = await ccn_api_client.get(_uri(OWNER))
    assert response.status == 200, await response.text()
    assert (await response.json())["credit_balance"] == 42


@pytest.mark.asyncio
async def test_serve_v2_falls_back_to_legacy_before_the_first_closed_period(
    ccn_api_client,
    session_factory: DbSessionFactory,
    serve_v2,
    frozen_now,
    caplog,
):
    _seed_funded_instance(session_factory, 1_000_000)
    with session_factory() as session:
        set_last_closed_period(session, None)
        session.add(
            AlephCreditBalanceDb(
                address=OWNER,
                credit_ref="legacy",
                credit_index=0,
                amount_remaining=42,
                expiration_date=None,
                message_timestamp=LAST_CLOSED,
                last_update=LAST_CLOSED,
            )
        )
        session.commit()

    with caplog.at_level(logging.ERROR, logger=balance_service.LOGGER.name):
        with session_factory() as session:
            assert not balance_service.check_serve_v2_at_startup(session)
    assert any(r.levelno == logging.ERROR for r in caplog.records)

    response = await ccn_api_client.get(_uri(OWNER))
    assert response.status == 200, await response.text()
    assert (await response.json())["credit_balance"] == 42


@pytest.mark.asyncio
async def test_serve_v2_falls_back_to_legacy_without_a_fold_state_row(
    ccn_api_client, session_factory: DbSessionFactory, serve_v2, frozen_now
):
    _seed_funded_instance(session_factory, 1_000_000)
    with session_factory() as session:
        session.execute(delete(CreditFoldStateDb))
        session.add(
            AlephCreditBalanceDb(
                address=OWNER,
                credit_ref="legacy",
                credit_index=0,
                amount_remaining=42,
                expiration_date=None,
                message_timestamp=LAST_CLOSED,
                last_update=LAST_CLOSED,
            )
        )
        session.commit()
        assert not balance_service.serve_v2_active(session)

    response = await ccn_api_client.get(_uri(OWNER))
    assert response.status == 200, await response.text()
    assert (await response.json())["credit_balance"] == 42


def test_serve_v2_refusal_is_logged_once_per_process(
    session_factory: DbSessionFactory, serve_v2, caplog
):
    with caplog.at_level(logging.ERROR, logger=balance_service.LOGGER.name):
        with session_factory() as session:
            for _ in range(3):
                assert not balance_service.serve_v2_active(session)
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1


def test_billing_owner_is_stored_in_ledger_form_and_found_by_both_forms(
    session_factory: DbSessionFactory, serve_v2
):
    message = MessageDb(
        item_hash=VM,
        chain=Chain.ETH,
        sender=OWNER,
        signature="sig",
        type=MessageType.instance,
        item_type=ItemType.inline,
        time=LAST_CLOSED,
        size=10,
        content=instance_spec()["content"],
        owner=OWNER,
        payment_type="credit",
    )
    with session_factory() as session:
        set_last_closed_period(session, LAST_CLOSED)
        seed_lot(session, OWNER.lower(), 1_000_000, period=LAST_CLOSED)
        # Not stored as a message: the billing row is unconfirmed and accrues
        # from the end of the last closed period.
        upsert_billing_resource(session, message, instance_spec())
        row = session.get(BillingResourceDb, VM)
        assert row is not None and row.owner == OWNER.lower()

        accrual = balance_service.provisional_accrual(session, OWNER, SINCE, NOW)
        assert accrual > 0
        assert (
            balance_service.provisional_accrual(session, OWNER.lower(), SINCE, NOW)
            == accrual
        )
        assert balance_service.get_credit_balance_v2(
            session, OWNER, NOW
        ) == balance_service.get_credit_balance_v2(session, OWNER.lower(), NOW)
