"""
Credit history, its summary and consumed credits served under
`aleph.credits.serve_v2`: legacy `credit_history` rows before the genesis
period, `credit_ledger` rows from it on.
"""

import datetime as dt
from typing import Optional

import pytest
from aleph_message.models import MessageType
from configmanager import Config
from credit_v2_helpers import OWNER, set_last_closed_period

from aleph.db.accessors.balances import (
    update_credit_balances_distribution,
    update_credit_balances_expense,
)
from aleph.db.models.credit_ledger import BillingResourceDb, CreditLedgerDb
from aleph.services.credits.fold import execution_amount
from aleph.services.credits.rates import rate_for
from aleph.toolkit.constants import MiB
from aleph.types.db_session import DbSession, DbSessionFactory

GENESIS = dt.datetime(2026, 4, 1, tzinfo=dt.timezone.utc)
HOUR = dt.timedelta(hours=1)
ADDRESS = OWNER.lower()
VM_1 = "1" * 64
VM_2 = "2" * 64
STORE = "5" * 64
NODE = "a" * 64
TOPUP_TX = "0x" + "e" * 64


def _unix(period: dt.datetime) -> int:
    return int(period.timestamp())


@pytest.fixture
def serve_v2(mock_config: Config) -> Config:
    mock_config.aleph.credits.serve_v2.value = True
    mock_config.aleph.credits.genesis.period.value = _unix(GENESIS)
    return mock_config


def _legacy_distribution(
    session: DbSession, ref: str, amount: int, when: dt.datetime
) -> None:
    update_credit_balances_distribution(
        session=session,
        credits_list=[
            {
                "address": ADDRESS,
                "amount": amount,
                "price": "0.000001",
                "tx_hash": f"0xtx_{ref}",
                "provider": "test_provider",
                "expiration": None,
                "origin": "",
                "ref": f"ref_{ref}",
                "payment_method": "credit_card",
            }
        ],
        token="ALEPH",
        chain="ETH",
        message_hash=ref,
        message_timestamp=when,
    )


def _legacy_expense(
    session: DbSession, ref: str, amount: int, when: dt.datetime, resource: str
) -> None:
    update_credit_balances_expense(
        session=session,
        credits_list=[
            {
                "address": ADDRESS,
                "amount": amount,
                "ref": f"ref_{ref}",
                "execution_id": resource,
                "node_id": "node",
                "price": "0.000001",
            }
        ],
        message_hash=ref,
        message_timestamp=when,
    )


def _ledger(
    session: DbSession,
    period: dt.datetime,
    kind: str,
    key: str,
    amount: int,
    *,
    resource: Optional[str] = None,
    node_id: Optional[str] = None,
    seconds: Optional[int] = None,
    source_ref: Optional[str] = None,
    excluded_reason: Optional[str] = None,
    count: Optional[int] = None,
    size_mib: Optional[int] = None,
) -> None:
    session.add(
        CreditLedgerDb(
            period=period,
            address=ADDRESS,
            kind=kind,
            business_key=key,
            amount=amount,
            resource=resource,
            node_id=node_id,
            seconds=seconds,
            source_ref=source_ref,
            excluded_reason=excluded_reason,
            entry_count=count,
            size_mib=size_mib,
        )
    )


def _seed_history(session_factory: DbSessionFactory) -> None:
    t2 = GENESIS + 2 * HOUR
    with session_factory() as session:
        _legacy_distribution(session, "legacy_dist", 1000, GENESIS - 31 * 24 * HOUR)
        _legacy_expense(session, "legacy_exp", 250, GENESIS - 17 * 24 * HOUR, VM_1)
        # After the genesis period the legacy table is ignored.
        _legacy_distribution(session, "legacy_late", 77, GENESIS + 14 * 24 * HOUR)

        _ledger(session, GENESIS, "genesis", "snap:0", 5000, source_ref="snap")
        _ledger(session, t2, "topup", f"1:{TOPUP_TX}:0", 1000, source_ref=TOPUP_TX)
        _ledger(
            session,
            t2,
            "execution",
            f"{_unix(t2)}:{VM_1}:{NODE}",
            -100,
            resource=VM_1,
            node_id=NODE,
            seconds=3600,
        )
        _ledger(
            session,
            t2,
            "execution",
            f"{_unix(t2)}:{VM_2}:{NODE}",
            0,
            resource=VM_2,
            node_id=NODE,
            excluded_reason="unknown_message",
        )
        # Notional amount of a hold-paid VM: not a balance movement.
        _ledger(
            session,
            t2,
            "hold_equivalent",
            f"{_unix(t2)}:{'9' * 64}:{NODE}",
            42,
            resource="9" * 64,
            node_id=NODE,
        )
        _ledger(
            session,
            GENESIS + 3 * HOUR,
            "storage",
            f"{_unix(GENESIS + 3 * HOUR)}:{ADDRESS}",
            -10,
            seconds=7200,
            count=2,
            size_mib=50,
        )
        set_last_closed_period(session, GENESIS + 3 * HOUR)
        session.commit()


EXPECTED_ORDER = [
    ("storage", GENESIS + 3 * HOUR),
    ("execution", GENESIS + 2 * HOUR),  # VM_2, excluded
    ("execution", GENESIS + 2 * HOUR),  # VM_1
    ("topup", GENESIS + 2 * HOUR),
    ("genesis", GENESIS),
    ("credit_expense", GENESIS - 17 * 24 * HOUR),
    ("credit_card", GENESIS - 31 * 24 * HOUR),
]


def _uri(suffix: str = "") -> str:
    return f"/api/v0/addresses/{ADDRESS}/credit_history{suffix}"


def _key(entry):
    # Page mode serializes datetimes as ISO strings, cursor mode as timestamps.
    timestamp = entry["message_timestamp"]
    if isinstance(timestamp, str):
        when = dt.datetime.fromisoformat(timestamp)
    else:
        when = dt.datetime.fromtimestamp(timestamp, tz=dt.timezone.utc)
    return (entry["payment_method"], when)


@pytest.mark.asyncio
async def test_credit_history_merges_legacy_and_v2_in_time_order(
    ccn_api_client, session_factory: DbSessionFactory, serve_v2
):
    _seed_history(session_factory)

    response = await ccn_api_client.get(_uri())
    assert response.status == 200, await response.text()
    data = await response.json()
    entries = data["credit_history"]
    assert data["pagination_total"] == len(EXPECTED_ORDER)
    assert [_key(e) for e in entries] == EXPECTED_ORDER

    storage, excluded, execution, topup, genesis = entries[:5]
    assert storage["amount"] == -10
    assert storage["expense_count"] == 2
    assert float(storage["expense_size_mib"]) == 50
    assert storage["seconds"] == 7200
    assert excluded["amount"] == 0
    assert excluded["excluded_reason"] == "unknown_message"
    assert excluded["origin"] == VM_2
    assert execution["amount"] == -100
    assert execution["origin"] == VM_1
    assert execution["node_id"] == NODE
    assert execution["excluded_reason"] is None
    assert topup["tx_hash"] == TOPUP_TX
    assert topup["origin_ref"] == TOPUP_TX
    assert topup["credit_ref"] == TOPUP_TX
    assert topup["credit_index"] == 0
    assert genesis["amount"] == 5000
    # Legacy rows keep their legacy fields.
    assert entries[-1]["credit_ref"] == "legacy_dist"
    assert entries[-1]["provider"] == "test_provider"

    # Ascending order is the exact reverse.
    response = await ccn_api_client.get(_uri(), params={"sort_order": "1"})
    data = await response.json()
    assert [_key(e) for e in data["credit_history"]] == EXPECTED_ORDER[::-1]

    # Filters apply to both sources.
    response = await ccn_api_client.get(_uri(), params={"direction": "outgoing"})
    data = await response.json()
    assert [e["amount"] for e in data["credit_history"]] == [-10, -100, -250]
    response = await ccn_api_client.get(_uri(), params={"resource": VM_1})
    data = await response.json()
    assert [e["amount"] for e in data["credit_history"]] == [-100, -250]
    response = await ccn_api_client.get(_uri(), params={"payment_method": "execution"})
    data = await response.json()
    assert len(data["credit_history"]) == 2


@pytest.mark.asyncio
async def test_credit_history_v2_cursor_walks_the_union(
    ccn_api_client, session_factory: DbSessionFactory, serve_v2
):
    _seed_history(session_factory)

    seen: list = []
    cursor = ""
    for _ in range(10):
        response = await ccn_api_client.get(
            _uri(), params={"cursor": cursor, "pagination": "2"}
        )
        assert response.status == 200, await response.text()
        data = await response.json()
        seen.extend(_key(e) for e in data["credit_history"])
        cursor = data["next_cursor"]
        if cursor is None:
            break
    assert seen == EXPECTED_ORDER


@pytest.mark.asyncio
async def test_credit_history_summary_sums_both_sources(
    ccn_api_client, session_factory: DbSessionFactory, serve_v2
):
    _seed_history(session_factory)

    response = await ccn_api_client.get(_uri("/summary"))
    assert response.status == 200, await response.text()
    assert await response.json() == {
        "address": ADDRESS,
        "entry_count": 7,
        "total_amount": 5000 + 1000 - 100 - 10 + 1000 - 250,
        "total_incoming": 7000,
        "total_outgoing": -360,
    }


def _store_spec(size_bytes: int):
    return {
        "type": "STORE",
        "content": {
            "address": OWNER,
            "time": 1701099523.849,
            "item_type": "ipfs",
            "item_hash": "Qm" + "a" * 44,
            "payment": {"chain": "ETH", "type": "credit"},
        },
        "volume_sizes_bytes": {},
        "file_size_bytes": size_bytes,
    }


@pytest.mark.asyncio
async def test_consumed_credits_for_store_is_computed_from_rates(
    ccn_api_client, session_factory: DbSessionFactory, serve_v2
):
    with session_factory() as session:
        _legacy_expense(session, "legacy_store", 7, GENESIS - 24 * HOUR, STORE)
        _legacy_expense(session, "legacy_store_late", 9, GENESIS + 24 * HOUR, STORE)
        resource = BillingResourceDb(
            item_hash=STORE,
            type=MessageType.store,
            owner=ADDRESS,
            payment_type="credit",
            first_confirmed_at=GENESIS + dt.timedelta(minutes=30),
            end_of_life=None,
            cost_spec=_store_spec(100 * MiB),
        )
        session.add(resource)
        session.flush()
        rate = rate_for(session, resource, GENESIS)
        # Periods GENESIS .. GENESIS+2h are closed; the open hour is not counted.
        set_last_closed_period(session, GENESIS + 2 * HOUR)
        session.commit()
    assert rate > 0
    expected = 7 + execution_amount(rate, 1800) + 2 * execution_amount(rate, 3600)

    response = await ccn_api_client.get(f"/api/v0/messages/{STORE}/consumed_credits")
    assert response.status == 200, await response.text()
    assert await response.json() == {"item_hash": STORE, "consumed_credits": expected}


@pytest.mark.asyncio
async def test_consumed_credits_for_vm_sums_execution_rows(
    ccn_api_client, session_factory: DbSessionFactory, serve_v2
):
    _seed_history(session_factory)
    with session_factory() as session:
        _ledger(
            session,
            GENESIS + 3 * HOUR,
            "execution",
            f"{_unix(GENESIS + 3 * HOUR)}:{VM_1}:{NODE}",
            -60,
            resource=VM_1,
            node_id=NODE,
        )
        session.commit()

    response = await ccn_api_client.get(f"/api/v0/messages/{VM_1}/consumed_credits")
    assert response.status == 200, await response.text()
    assert (await response.json())["consumed_credits"] == 250 + 100 + 60


@pytest.mark.asyncio
async def test_credit_history_legacy_path_without_serve_v2(
    ccn_api_client, session_factory: DbSessionFactory, mock_config: Config
):
    mock_config.aleph.credits.genesis.period.value = _unix(GENESIS)
    _seed_history(session_factory)

    response = await ccn_api_client.get(_uri())
    assert response.status == 200, await response.text()
    entries = (await response.json())["credit_history"]
    assert [e["credit_ref"] for e in entries] == [
        "legacy_late",
        "legacy_exp",
        "legacy_dist",
    ]
    assert "excluded_reason" not in entries[0]
