"""The v2 credit endpoints under /api/v0/credits (spec 10)."""

import datetime as dt
from decimal import Decimal
from typing import Any, Dict, Optional

import pytest
from aleph_message.models import ItemType, MessageType
from configmanager import Config
from credit_v2_helpers import OWNER, seed_lot, set_last_closed_period
from sqlalchemy import update

from aleph.db.models import MessageDb
from aleph.db.models.credit_ledger import (
    CreditAttestationDb,
    CreditFoldStateDb,
    CreditLedgerDb,
    CreditPeriodQuoteDb,
    CreditTopupDb,
)
from aleph.services.credits.fold import period_hash, write_checkpoint
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.web.controllers.app_state_getters import APP_STATE_CREDIT_QUOTER

H0 = dt.datetime(2025, 6, 1, 0, tzinfo=dt.timezone.utc)
HOUR = dt.timedelta(hours=1)
NODE_A = "a" * 64
NODE_B = "b" * 64
VM_1 = "1" * 64
VM_2 = "2" * 64
VM_3 = "3" * 64
OTHER = "0x00000000000000000000000000000000000000aa"


def _unix(period: dt.datetime) -> int:
    return int(period.timestamp())


def _row(
    session: DbSession,
    period: dt.datetime,
    kind: str,
    key: str,
    amount: int,
    *,
    address: Optional[str] = OWNER.lower(),
    resource: Optional[str] = None,
    node_id: Optional[str] = None,
    seconds: Optional[int] = None,
    rate: Optional[Decimal] = None,
    source_ref: Optional[str] = None,
    excluded_reason: Optional[str] = None,
) -> None:
    session.add(
        CreditLedgerDb(
            period=period,
            address=address,
            kind=kind,
            business_key=key,
            amount=amount,
            resource=resource,
            node_id=node_id,
            seconds=seconds,
            rate=rate,
            source_ref=source_ref,
            excluded_reason=excluded_reason,
        )
    )


def _execution(
    session: DbSession,
    period: dt.datetime,
    vm: str,
    node: str,
    amount: int,
    *,
    kind: str = "execution",
    excluded_reason: Optional[str] = None,
) -> None:
    signed = -amount if kind == "execution" else amount
    _row(
        session,
        period,
        kind,
        f"{_unix(period)}:{vm}:{node}",
        0 if excluded_reason else signed,
        resource=vm,
        node_id=node,
        seconds=3600,
        rate=Decimal("0.5"),
        excluded_reason=excluded_reason,
    )


@pytest.fixture(autouse=True)
def fold_starts_at_h0(mock_config: Config) -> None:
    """The fold's first period is H0: nothing before it is ever folded."""
    mock_config.aleph.credits.attestation.start_period.value = _unix(H0)


def _set_fold_state(
    session: DbSession,
    last_closed: Optional[dt.datetime],
    dirty_from: Optional[dt.datetime] = None,
) -> None:
    session.execute(
        update(CreditFoldStateDb)
        .where(CreditFoldStateDb.id == 1)
        .values(last_closed_period=last_closed, dirty_from_period=dirty_from)
    )


# --- /credits/periods/{period} ------------------------------------------------


@pytest.mark.asyncio
async def test_period_endpoint_reports_hash_and_totals(
    ccn_api_client, session_factory: DbSessionFactory
):
    attestation = "f" * 64
    sender = "0x4D52380D3191274a04846c89c069E6C3F2Ed94e4"
    with session_factory() as session:
        seed_lot(session, OWNER.lower(), 1_000, period=H0, key="1:0xtx:0")
        _execution(session, H0, VM_1, NODE_A, 100)
        _execution(session, H0, VM_2, NODE_B, 40)
        _execution(session, H0, VM_3, NODE_A, 0, excluded_reason="unknown_message")
        session.add(
            MessageDb(
                item_hash=attestation,
                type=MessageType.post,
                chain="ETH",
                sender=sender,
                signature=None,
                item_type=ItemType.inline,
                item_content="{}",
                content={},
                time=H0 + HOUR,
                channel="ALEPH-METERING",
                size=2,
                first_confirmed_at=H0 + HOUR,
            )
        )
        session.add(
            CreditAttestationDb(
                item_hash=attestation, period=H0, sender=sender, status="active"
            )
        )
        session.flush()
        global_hash = write_checkpoint(session, H0)
        expected_hash = period_hash(session, H0)
        _set_fold_state(session, H0 + HOUR)
        session.commit()

    response = await ccn_api_client.get(f"/api/v0/credits/periods/{_unix(H0)}")
    assert response.status == 200, await response.text()
    data = await response.json()
    assert data["period"] == _unix(H0)
    assert data["status"] == "closed"
    assert data["period_hash"] == expected_hash
    assert data["checkpoint_hash"] == global_hash
    assert data["attestation"] == {"item_hash": attestation, "sender": sender}
    assert data["totals"] == {
        "topup": {"rows": 1, "amount": 1_000},
        "execution": {"rows": 3, "amount": -140},
    }

    # The open hour after the last closed one: no hash yet.
    open_period = _unix(H0 + 2 * HOUR)
    response = await ccn_api_client.get(f"/api/v0/credits/periods/{open_period}")
    assert response.status == 200, await response.text()
    data = await response.json()
    assert data["status"] == "open"
    assert data["period_hash"] is None
    assert data["attestation"] is None
    assert data["totals"] == {}


@pytest.mark.asyncio
async def test_period_endpoint_reports_dirty_periods(
    ccn_api_client, session_factory: DbSessionFactory
):
    with session_factory() as session:
        _set_fold_state(session, H0 + 2 * HOUR, dirty_from=H0 + HOUR)
        session.commit()

    statuses = []
    for period in (H0, H0 + HOUR, H0 + 2 * HOUR, H0 + 3 * HOUR):
        response = await ccn_api_client.get(f"/api/v0/credits/periods/{_unix(period)}")
        assert response.status == 200, await response.text()
        statuses.append((await response.json())["status"])
    assert statuses == ["closed", "dirty", "dirty", "open"]


@pytest.mark.asyncio
async def test_period_endpoint_rejects_unaligned_periods(ccn_api_client):
    response = await ccn_api_client.get(f"/api/v0/credits/periods/{_unix(H0) + 1}")
    assert response.status == 400
    response = await ccn_api_client.get("/api/v0/credits/periods/yesterday")
    assert response.status == 422


# --- /credits/revenue ---------------------------------------------------------


def _revenue_uri(start: dt.datetime, end: dt.datetime) -> str:
    return f"/api/v0/credits/revenue?from={_unix(start)}&to={_unix(end)}"


@pytest.mark.asyncio
async def test_revenue_groups_by_node_and_includes_price(
    ccn_api_client, session_factory: DbSessionFactory
):
    with session_factory() as session:
        _execution(session, H0, VM_1, NODE_A, 100)
        _execution(session, H0, VM_2, NODE_A, 50)
        _execution(session, H0, VM_3, NODE_B, 30)
        _execution(session, H0, "4" * 64, NODE_B, 70, kind="hold_equivalent")
        # Exclusions never count, whatever their kind.
        _execution(session, H0, "5" * 64, NODE_B, 0, excluded_reason="over_cap")
        _row(session, H0, "storage", f"{_unix(H0)}:{OWNER.lower()}", -12)
        _row(session, H0, "storage", f"{_unix(H0)}:{OTHER}", -8, address=OTHER)
        _execution(session, H0 + HOUR, VM_1, NODE_A, 100)
        # Period 2 is beyond the last closed one: never returned.
        _execution(session, H0 + 2 * HOUR, VM_1, NODE_A, 100)
        session.add(
            CreditPeriodQuoteDb(
                period=H0,
                aleph_per_credit=Decimal("0.000012345678901234"),
                block_number=22_000_000,
            )
        )
        _set_fold_state(session, H0 + HOUR)
        session.commit()

    response = await ccn_api_client.get(_revenue_uri(H0, H0 + 3 * HOUR))
    assert response.status == 200, await response.text()
    data = await response.json()
    assert data == {
        "periods": [
            {
                "period": _unix(H0),
                "credit_price_aleph": "0.000012345678901234",
                "nodes": {
                    NODE_A: {"execution": 150, "hold_equivalent": 0},
                    NODE_B: {"execution": 30, "hold_equivalent": 70},
                },
                "storage": 20,
                "execution_total": 180,
            },
            {
                "period": _unix(H0 + HOUR),
                "credit_price_aleph": None,
                "nodes": {NODE_A: {"execution": 100, "hold_equivalent": 0}},
                "storage": 0,
                "execution_total": 100,
            },
        ]
    }


@pytest.mark.asyncio
async def test_revenue_lists_empty_closed_periods(
    ccn_api_client, session_factory: DbSessionFactory
):
    with session_factory() as session:
        _set_fold_state(session, H0 + HOUR)
        session.commit()

    # H0 - HOUR is before the fold's first period: never folded, not listed.
    response = await ccn_api_client.get(_revenue_uri(H0 - HOUR, H0 + 5 * HOUR))
    assert response.status == 200, await response.text()
    periods = (await response.json())["periods"]
    assert [p["period"] for p in periods] == [_unix(H0), _unix(H0 + HOUR)]
    assert periods[0] == {
        "period": _unix(H0),
        "credit_price_aleph": None,
        "nodes": {},
        "storage": 0,
        "execution_total": 0,
    }

    response = await ccn_api_client.get(f"/api/v0/credits/periods/{_unix(H0 - HOUR)}")
    assert response.status == 200, await response.text()
    data = await response.json()
    assert data["status"] == "open"
    assert data["period_hash"] is None

    # An empty range is valid.
    response = await ccn_api_client.get(_revenue_uri(H0, H0))
    assert response.status == 200, await response.text()
    assert await response.json() == {"periods": []}


@pytest.mark.asyncio
async def test_revenue_and_periods_without_a_first_period(
    ccn_api_client, session_factory: DbSessionFactory, mock_config: Config
):
    mock_config.aleph.credits.attestation.start_period.value = None
    with session_factory() as session:
        _set_fold_state(session, H0 + HOUR)
        session.commit()

    response = await ccn_api_client.get(_revenue_uri(H0, H0 + 2 * HOUR))
    assert response.status == 200, await response.text()
    assert await response.json() == {"periods": []}
    response = await ccn_api_client.get(f"/api/v0/credits/periods/{_unix(H0)}")
    assert (await response.json())["status"] == "open"


@pytest.mark.asyncio
async def test_revenue_stops_at_the_dirty_mark(
    ccn_api_client, session_factory: DbSessionFactory
):
    with session_factory() as session:
        for hour in range(3):
            _execution(session, H0 + hour * HOUR, VM_1, NODE_A, 10)
        _set_fold_state(session, H0 + 2 * HOUR, dirty_from=H0 + HOUR)
        expected_hash = period_hash(session, H0 + HOUR)
        session.commit()

    response = await ccn_api_client.get(_revenue_uri(H0, H0 + 3 * HOUR))
    assert response.status == 200, await response.text()
    periods = (await response.json())["periods"]
    assert [p["period"] for p in periods] == [_unix(H0)]

    response = await ccn_api_client.get(f"/api/v0/credits/periods/{_unix(H0 + HOUR)}")
    data = await response.json()
    assert data["status"] == "dirty"
    # The last folded hash is still shown.
    assert data["period_hash"] == expected_hash


@pytest.mark.asyncio
async def test_revenue_price_is_never_in_scientific_notation(
    ccn_api_client, session_factory: DbSessionFactory
):
    with session_factory() as session:
        session.add(
            CreditPeriodQuoteDb(
                period=H0,
                aleph_per_credit=Decimal("0.000000012345678901"),
                block_number=22_000_000,
            )
        )
        _set_fold_state(session, H0)
        session.commit()

    response = await ccn_api_client.get(_revenue_uri(H0, H0 + HOUR))
    assert response.status == 200, await response.text()
    price = (await response.json())["periods"][0]["credit_price_aleph"]
    assert price == "0.000000012345678901"
    assert str(Decimal(price)) != price  # str() would have used an exponent


@pytest.mark.asyncio
async def test_revenue_is_empty_before_the_first_closed_period(
    ccn_api_client, session_factory: DbSessionFactory
):
    response = await ccn_api_client.get(_revenue_uri(H0, H0 + HOUR))
    assert response.status == 200, await response.text()
    assert await response.json() == {"periods": []}


@pytest.mark.asyncio
async def test_revenue_rejects_ranges_over_744_periods(ccn_api_client):
    response = await ccn_api_client.get(_revenue_uri(H0, H0 + 745 * HOUR))
    assert response.status == 422
    response = await ccn_api_client.get(_revenue_uri(H0, H0 + 744 * HOUR))
    assert response.status == 200, await response.text()


@pytest.mark.asyncio
async def test_revenue_validates_bounds(ccn_api_client):
    start = _unix(H0)
    # Unaligned bounds: 400.
    for query in (
        f"from={start + 1}&to={start + 3600}",
        f"from={start}&to={start + 1}",
    ):
        response = await ccn_api_client.get(f"/api/v0/credits/revenue?{query}")
        assert response.status == 400, query
    # Missing or non-integer bounds, inverted range, future `to`: 422.
    future = int(dt.datetime.now(dt.timezone.utc).timestamp()) // 3600 * 3600 + 3600
    for query in (
        f"from={start}",
        f"from=abc&to={start}",
        f"from={start + 3600}&to={start}",
        f"from={future - 3600}&to={future}",
    ):
        response = await ccn_api_client.get(f"/api/v0/credits/revenue?{query}")
        assert response.status == 422, query
    # The current hour start is an allowed `to`.
    response = await ccn_api_client.get(
        f"/api/v0/credits/revenue?from={future - 7200}&to={future - 3600}"
    )
    assert response.status == 200, await response.text()


# --- /credits/exclusions ------------------------------------------------------


@pytest.mark.asyncio
async def test_exclusions_filter_by_reason(
    ccn_api_client, session_factory: DbSessionFactory
):
    with session_factory() as session:
        _execution(session, H0, VM_1, NODE_A, 100)
        _execution(session, H0, VM_2, NODE_A, 0, excluded_reason="unknown_message")
        _execution(session, H0, VM_3, NODE_B, 0, excluded_reason="over_cap")
        _execution(
            session, H0 + HOUR, VM_2, NODE_A, 0, excluded_reason="unknown_message"
        )
        _row(
            session,
            H0 + HOUR,
            "transfer_out",
            "c" * 64 + ":0",
            0,
            address=OTHER,
            source_ref="c" * 64,
            excluded_reason="insufficient_balance",
        )
        session.commit()

    response = await ccn_api_client.get(
        "/api/v0/credits/exclusions", params={"reason": "unknown_message"}
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    assert data["pagination_total"] == 2
    assert [(e["period"], e["resource"]) for e in data["exclusions"]] == [
        (_unix(H0 + HOUR), VM_2),
        (_unix(H0), VM_2),
    ]
    assert {e["excluded_reason"] for e in data["exclusions"]} == {"unknown_message"}

    response = await ccn_api_client.get(
        "/api/v0/credits/exclusions", params={"period": str(_unix(H0))}
    )
    data = await response.json()
    assert sorted(e["excluded_reason"] for e in data["exclusions"]) == [
        "over_cap",
        "unknown_message",
    ]

    # Address filter, normalized: the checksummed form finds lowercased rows.
    response = await ccn_api_client.get(
        "/api/v0/credits/exclusions",
        params={"address": "0x00000000000000000000000000000000000000AA"},
    )
    data = await response.json()
    assert [e["kind"] for e in data["exclusions"]] == ["transfer_out"]
    assert data["exclusions"][0]["source_ref"] == "c" * 64

    response = await ccn_api_client.get(
        "/api/v0/credits/exclusions", params={"pagination": "1001"}
    )
    assert response.status == 422


# --- /credits/quote -----------------------------------------------------------


class _FakeEth:
    async def get_block(self, identifier: Any) -> Dict[str, Any]:
        assert identifier == "finalized"
        return {"number": 21_000_000}


class _FakeWeb3:
    eth = _FakeEth()


class _FakeQuoter:
    """The TopupIndexer quote interface, without RPC."""

    def __init__(self, cfg):
        self.cfg = cfg
        self.web3 = _FakeWeb3()
        self.calls = []

    async def quote(self, token, raw_amount, block_number, apply_bonus=True):
        self.calls.append((token, raw_amount, block_number, apply_bonus))
        if token.lower() == self.cfg.usdc.lower():
            return 1, 1
        return 3, 2


@pytest.fixture
def fake_quoter(ccn_test_aiohttp_app, mock_config: Config):
    from aleph.services.credits.topups import TopupConfig

    quoter = _FakeQuoter(
        TopupConfig.from_config(mock_config, require_start_block=False)
    )
    ccn_test_aiohttp_app[APP_STATE_CREDIT_QUOTER] = quoter
    return quoter


@pytest.mark.asyncio
async def test_quote_usdc_is_par(fake_quoter, ccn_api_client):
    response = await ccn_api_client.get(
        "/api/v0/credits/quote", params={"token": "usdc", "amount": "2500000"}
    )
    assert response.status == 200, await response.text()
    assert await response.json() == {
        "credits": 2_500_000,
        "quote_num": "1",
        "quote_den": "1",
        "block_number": 21_000_000,
    }
    assert fake_quoter.calls == [(fake_quoter.cfg.usdc, 2_500_000, 21_000_000, True)]


@pytest.mark.asyncio
async def test_quote_aleph_applies_the_quote(fake_quoter, ccn_api_client):
    response = await ccn_api_client.get(
        "/api/v0/credits/quote", params={"token": "ALEPH", "amount": "11"}
    )
    assert response.status == 200, await response.text()
    assert (await response.json())["credits"] == 11 * 3 // 2
    assert fake_quoter.calls[0][0] == fake_quoter.cfg.aleph

    response = await ccn_api_client.get(
        "/api/v0/credits/quote", params={"token": "doge", "amount": "1"}
    )
    assert response.status == 422
    response = await ccn_api_client.get(
        "/api/v0/credits/quote", params={"token": "eth", "amount": "-1"}
    )
    assert response.status == 422


# --- /credits/topups ----------------------------------------------------------


def _topup(
    tx_hash: str, log_index: int, from_address: str, status: str, credits: int
) -> CreditTopupDb:
    return CreditTopupDb(
        chain_id=1,
        tx_hash=tx_hash,
        log_index=log_index,
        block_number=21_000_000 + log_index,
        block_time=H0 + dt.timedelta(minutes=log_index),
        from_address=from_address,
        token="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        raw_amount=Decimal(credits),
        quote_num=Decimal(1) if credits else None,
        quote_den=Decimal(1) if credits else None,
        credits=credits or None,
        status=status,
        ledger_period=H0 if credits else None,
    )


@pytest.mark.asyncio
async def test_topups_by_address(ccn_api_client, session_factory: DbSessionFactory):
    with session_factory() as session:
        session.add(_topup("0x" + "1" * 64, 1, OWNER, "credited", 5_000))
        session.add(_topup("0x" + "2" * 64, 2, OWNER, "unquoted", 0))
        session.add(_topup("0x" + "3" * 64, 3, OTHER, "credited", 7))
        session.commit()

    # Any case of the address finds its rows.
    response = await ccn_api_client.get(
        "/api/v0/credits/topups", params={"address": OWNER.lower()}
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    assert data["pagination_total"] == 2
    topups = data["topups"]
    assert [t["tx_hash"] for t in topups] == ["0x" + "2" * 64, "0x" + "1" * 64]
    assert topups[1]["status"] == "credited"
    assert topups[1]["credits"] == 5_000
    assert topups[1]["raw_amount"] == "5000"
    assert topups[1]["ledger_period"] == _unix(H0)
    assert topups[0]["status"] == "unquoted"
    assert topups[0]["credits"] is None

    response = await ccn_api_client.get("/api/v0/credits/topups")
    assert response.status == 422


# --- /credit_balances (ruling 1) ----------------------------------------------


@pytest.mark.asyncio
async def test_credit_balances_listing_serves_v2_lots(
    ccn_api_client, session_factory: DbSessionFactory, mock_config: Config
):
    mock_config.aleph.credits.serve_v2.value = True
    with session_factory() as session:
        set_last_closed_period(session, H0)
        seed_lot(session, OWNER.lower(), 300, period=H0, key="1:0xa:0")
        seed_lot(session, OWNER.lower(), 200, period=H0, key="1:0xb:0")
        seed_lot(
            session,
            OWNER.lower(),
            999,
            period=H0,
            key="1:0xc:0",
            expiration=H0,  # expired
        )
        seed_lot(session, OTHER, 50, period=H0, key="1:0xd:0")
        session.commit()

    response = await ccn_api_client.get("/api/v0/credit_balances")
    assert response.status == 200, await response.text()
    data = await response.json()
    assert data["credit_balances"] == [
        {"address": OTHER, "credits": 50},
        {"address": OWNER.lower(), "credits": 500},
    ]
    assert data["pagination_total"] == 2
    assert data["pagination_item"] == "credit_balances"

    response = await ccn_api_client.get(
        "/api/v0/credit_balances", params={"min_balance": "100"}
    )
    data = await response.json()
    assert data["credit_balances"] == [{"address": OWNER.lower(), "credits": 500}]
    assert data["pagination_total"] == 1

    # Cursor mode pages through the same rows.
    response = await ccn_api_client.get(
        "/api/v0/credit_balances", params={"cursor": "", "pagination": "1"}
    )
    data = await response.json()
    assert data["credit_balances"] == [{"address": OTHER, "credits": 50}]
    assert data["next_cursor"]
    response = await ccn_api_client.get(
        "/api/v0/credit_balances",
        params={"cursor": data["next_cursor"], "pagination": "1"},
    )
    data = await response.json()
    assert data["credit_balances"] == [{"address": OWNER.lower(), "credits": 500}]
    assert data["next_cursor"] is None
