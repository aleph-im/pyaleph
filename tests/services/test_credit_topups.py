import datetime as dt
from decimal import Decimal

import pytest
from web3 import Web3

from aleph.db.accessors.credit_ledger import get_fold_state, unquoted_topups
from aleph.db.models.credit_ledger import CreditTopupDb
from aleph.services.credits.topups import (
    EXCLUDED_SELECTORS,
    PROCESS_SELECTOR,
    TRANSFER_TOPIC,
    TopupConfig,
    TopupIndexer,
)

CONTRACT = "0x6b55F32Ea969910838defd03746Ced5E2AE8cB8B"
USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
ALEPH = "0x27702a26126e0B3702af63Ee09aC4d1A084EF628"
FEED = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"
POOL = "0x1111111111111111111111111111111111111111"
POOL_MANAGER = "0x000000000004444c5dc75cB358380D2e3dE08A90"
USER = "0x2222222222222222222222222222222222222222"
T0 = 1_800_000_000  # timestamp of block 0 in the fake chain


def pad(address):
    return "0x" + address.lower().removeprefix("0x").rjust(64, "0")


def make_log(address, sender, amount, block=95, log_index=0, tx="0x" + "ab" * 32):
    return {
        "address": address,
        "topics": [TRANSFER_TOPIC, pad(sender), pad(CONTRACT)],
        "data": "0x" + amount.to_bytes(32, "big").hex(),
        "blockNumber": block,
        "logIndex": log_index,
        "transactionHash": tx,
    }


def make_cfg(**kw):
    base = dict(
        chain_id=1,
        start_block=1,
        contract=CONTRACT,
        usdc=USDC,
        aleph=ALEPH,
        chainlink_eth_usd=FEED,
        aleph_weth_pool=POOL,
        denylist=[POOL_MANAGER],
        chunk_blocks=2000,
        aleph_bonus=Decimal("0.2"),
    )
    base.update(kw)
    return TopupConfig(**base)


def make_web3(mocker, logs, finalized=100, tx=None, eth_usd=3000 * 10**8, ticks=None):
    web3 = mocker.MagicMock()

    async def get_block(ident):
        if ident == "finalized":
            return {"number": finalized}
        return {"number": ident, "timestamp": T0 + ident * 12}

    web3.eth.get_block = mocker.AsyncMock(side_effect=get_block)

    async def get_logs(flt):
        addr = flt["address"].lower()
        return [
            lg
            for lg in logs
            if lg["address"].lower() == addr
            and flt["fromBlock"] <= lg["blockNumber"] <= flt["toBlock"]
        ]

    web3.eth.get_logs = mocker.AsyncMock(side_effect=get_logs)
    web3.eth.get_transaction = mocker.AsyncMock(
        return_value=tx or {"to": USER, "input": "0x"}
    )

    feed = mocker.MagicMock()
    feed.functions.latestRoundData.return_value.call = mocker.AsyncMock(
        return_value=(1, eth_usd, 0, 0, 1)
    )
    pool = mocker.MagicMock()
    pool.functions.observe.return_value.call = mocker.AsyncMock(
        return_value=(list(ticks or (0, 0)), [0, 0])
    )
    pool.functions.token0.return_value.call = mocker.AsyncMock(return_value=ALEPH)
    contracts = {FEED.lower(): feed, POOL.lower(): pool}
    web3.eth.contract = mocker.MagicMock(
        side_effect=lambda address, abi: contracts[address.lower()]
    )
    web3.feed, web3.pool = feed, pool
    return web3


async def run_index(session_factory, web3, cfg=None):
    indexer = TopupIndexer(web3, cfg or make_cfg())
    with session_factory() as session:
        state = get_fold_state(session)
        n = await indexer.index(session, state)
        session.commit()
    return indexer, n


def all_rows(session_factory):
    with session_factory() as session:
        rows = session.query(CreditTopupDb).order_by(CreditTopupDb.log_index).all()
        session.expunge_all()
        return rows


@pytest.mark.asyncio
async def test_usdc_transfer_is_credited_at_par(mocker, session_factory):
    web3 = make_web3(mocker, [make_log(USDC, USER, 1_000_000)])
    _, n = await run_index(session_factory, web3)
    assert n == 1
    (row,) = all_rows(session_factory)
    assert row.status == "credited"
    assert row.credits == 1_000_000
    assert (row.quote_num, row.quote_den) == (1, 1)
    assert row.from_address == USER
    assert row.ledger_period == dt.datetime.fromtimestamp(
        T0 + 95 * 12, tz=dt.timezone.utc
    ).replace(minute=0, second=0)


@pytest.mark.asyncio
async def test_pool_manager_transfer_is_excluded(mocker, session_factory):
    web3 = make_web3(mocker, [make_log(USDC, POOL_MANAGER, 5_000_000)])
    await run_index(session_factory, web3)
    (row,) = all_rows(session_factory)
    assert row.status == "excluded_denylist"
    assert row.credits is None


@pytest.mark.asyncio
async def test_process_tx_transfer_is_excluded(mocker, session_factory):
    tx = {"to": CONTRACT, "input": "0x" + PROCESS_SELECTOR + "00" * 128}
    web3 = make_web3(mocker, [make_log(USDC, USER, 5_000_000)], tx=tx)
    await run_index(session_factory, web3)
    (row,) = all_rows(session_factory)
    assert row.status == "excluded_process_tx"
    assert row.credits is None


@pytest.mark.asyncio
async def test_eth_transfer_uses_chainlink_at_block(mocker, session_factory):
    web3 = make_web3(mocker, [make_log(CONTRACT, USER, 10**18, block=97)])
    await run_index(session_factory, web3)
    (row,) = all_rows(session_factory)
    assert row.status == "credited"
    assert row.credits == 3_000_000_000
    call = web3.feed.functions.latestRoundData.return_value.call
    assert call.call_args.kwargs["block_identifier"] == 97


@pytest.mark.asyncio
async def test_aleph_transfer_applies_bonus(mocker, session_factory):
    # tick 0: 1 ALEPH == 1 WETH; ETH at 3000 USD -> 3_000_000_000 credits per ALEPH
    web3 = make_web3(mocker, [make_log(ALEPH, USER, 10**18)])
    indexer, _ = await run_index(session_factory, web3)
    (row,) = all_rows(session_factory)
    assert row.credits == 3_600_000_000
    num, den = await indexer.quote(ALEPH, 10**18, 95, apply_bonus=False)
    assert 10**18 * num // den == 3_000_000_000
    call = web3.pool.functions.observe.return_value.call
    assert call.call_args.kwargs["block_identifier"] == 95
    web3.pool.functions.observe.assert_called_with([1800, 0])


@pytest.mark.asyncio
async def test_quote_failure_leaves_unquoted_and_advances_cursor(
    mocker, session_factory
):
    web3 = make_web3(mocker, [make_log(CONTRACT, USER, 10**18)])
    web3.feed.functions.latestRoundData.return_value.call.side_effect = RuntimeError(
        "missing trie node"
    )
    await run_index(session_factory, web3)
    (row,) = all_rows(session_factory)
    assert row.status == "unquoted"
    assert row.quote_num is None and row.credits is None
    with session_factory() as session:
        assert get_fold_state(session).chain_cursor_block == 100


@pytest.mark.asyncio
async def test_cursor_never_passes_finalized(mocker, session_factory):
    web3 = make_web3(mocker, [], finalized=100)
    with session_factory() as session:
        state = get_fold_state(session)
        state.chain_cursor_block = 90
        session.commit()
    await run_index(session_factory, web3)
    with session_factory() as session:
        state = get_fold_state(session)
        assert state.chain_cursor_block == 100
        assert state.chain_cursor_time == dt.datetime.fromtimestamp(
            T0 + 100 * 12, tz=dt.timezone.utc
        )
    flt = web3.eth.get_logs.call_args.args[0]
    assert flt["toBlock"] == 100


@pytest.mark.asyncio
async def test_requote_fixes_unquoted_and_marks_period_dirty(mocker, session_factory):
    web3 = make_web3(mocker, [make_log(CONTRACT, USER, 10**18)])
    call = web3.feed.functions.latestRoundData.return_value.call
    good = call.return_value
    call.side_effect = RuntimeError("boom")
    indexer, _ = await run_index(session_factory, web3)
    call.side_effect = None
    call.return_value = good

    period = dt.datetime.fromtimestamp(T0 + 95 * 12, tz=dt.timezone.utc).replace(
        minute=0, second=0
    )
    with session_factory() as session:
        get_fold_state(session).last_closed_period = period
        assert len(unquoted_topups(session)) == 1
        assert await indexer.requote_unquoted(session) == 1
        session.commit()
    (row,) = all_rows(session_factory)
    assert row.status == "credited"
    assert row.credits == 3_000_000_000
    assert row.ledger_period == period
    with session_factory() as session:
        assert get_fold_state(session).dirty_from_period == period


@pytest.mark.asyncio
async def test_withdraw_selector_excluded_and_unrelated_not(mocker, session_factory):
    withdraw = Web3.keccak(text="withdraw(address,address,uint128)").hex()
    withdraw = withdraw.removeprefix("0x")[:8]
    assert withdraw in EXCLUDED_SELECTORS
    tx = {"to": CONTRACT, "input": "0x" + withdraw + "00" * 96}
    web3 = make_web3(mocker, [make_log(USDC, USER, 5_000_000)], tx=tx)
    await run_index(session_factory, web3)
    assert all_rows(session_factory)[0].status == "excluded_process_tx"


@pytest.mark.asyncio
async def test_unrelated_selector_is_credited(mocker, session_factory):
    tx = {"to": CONTRACT, "input": "0xdeadbeef" + "00" * 64}
    web3 = make_web3(mocker, [make_log(USDC, USER, 5_000_000)], tx=tx)
    await run_index(session_factory, web3)
    assert all_rows(session_factory)[0].status == "credited"


def test_topup_config_from_config(mock_config):
    import aleph.config

    cfg = TopupConfig.from_config(aleph.config.app_config)
    assert cfg.contract == CONTRACT
    assert cfg.aleph_bonus == Decimal("0.2")
    assert cfg.chunk_blocks == 2000
    assert cfg.chain_id == aleph.config.app_config.ethereum.chain_id.value
