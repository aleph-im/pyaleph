"""
Tests for the Ethereum sync RPC timeout + web3 client recycle behaviour.

Root cause covered here: the single long-lived AsyncWeb3 client can wedge on a
stale TCP connection, making ``eth.get_logs`` / ``eth.block_number`` hang
forever. The connector must bound those awaits with ``asyncio.wait_for`` and,
on timeout, rebuild its web3 client so the next retry uses a fresh connection.

Every async test that exercises a potential hang is guarded with an outer
``asyncio.wait_for(..., timeout=5)`` so the suite can never hang.
"""

import asyncio

import pytest
from eth_typing import BlockNumber

from aleph.chains.ethereum import EthereumConnector


def _make_connector(mocker, get_logs=None, block_number=None):
    """
    Build an EthereumConnector with a mock web3 client, using the safe
    defaults added to ``__init__`` for the fields a minimal construction does
    not need.
    """
    web3_client = mocker.MagicMock()
    if get_logs is not None:
        web3_client.eth.get_logs = get_logs

    # block_number is awaited as an attribute (``await ...eth.block_number``),
    # so assign an awaitable directly on this mock *instance*. Do not patch
    # ``type(web3_client.eth)`` — that mutates the shared MagicMock class and
    # leaks the property across tests.
    if block_number is not None:
        web3_client.eth.block_number = block_number()

    # provider.disconnect() is awaited in the reset path; make it awaitable so
    # the disconnect actually runs instead of being swallowed as a TypeError.
    web3_client.provider.disconnect = mocker.AsyncMock()

    contract = mocker.MagicMock()
    contract.address = "0x" + "0" * 40

    connector = EthereumConnector(
        web3_client=web3_client,
        contract=contract,
        authorized_emitters=[],
        max_gas_price=0,
        start_height=BlockNumber(0),
        max_block_range=100,
        session_factory=mocker.MagicMock(),
        pending_tx_publisher=mocker.AsyncMock(),
        chain_data_service=mocker.AsyncMock(),
        # Intentionally leave rpc_url/chain_id/contract_address unset so the
        # reset path skips rebuilding (no real network), while the timeout
        # still surfaces.
        client_timeout=0.05,
    )
    return connector


@pytest.mark.asyncio
async def test_get_logs_hang_raises_timeout_and_triggers_reset(mocker):
    async def hang(*_args, **_kwargs):
        await asyncio.Event().wait()

    connector = _make_connector(mocker, get_logs=hang)
    reset_spy = mocker.spy(connector, "_reset_web3_client")

    # The hung get_logs must surface as a timeout, not as TooManyLogsInRange.
    with pytest.raises(asyncio.TimeoutError) as exc_info:
        await asyncio.wait_for(
            connector._get_logs_in_block_range(BlockNumber(0), BlockNumber(10)),
            timeout=5,
        )

    # A timeout must trigger the client reset. The actual client rebuild is
    # covered by test_reset_rebuilds_client_when_params_set; here rpc_url etc.
    # are unset so the reset is a no-op beyond being invoked.
    assert reset_spy.await_count == 1
    # The timeout must be catchable by `except Exception` in the retry loop.
    assert isinstance(exc_info.value, Exception)


@pytest.mark.asyncio
async def test_block_number_hang_raises_timeout_and_triggers_reset(mocker):
    async def hang():
        await asyncio.Event().wait()

    connector = _make_connector(mocker, block_number=hang)
    reset_spy = mocker.spy(connector, "_reset_web3_client")

    async def drain():
        async for _log in connector._get_all_logs_in_batches(
            BlockNumber(0), max_block_range=100
        ):
            pass

    with pytest.raises(asyncio.TimeoutError) as exc_info:
        await asyncio.wait_for(drain(), timeout=5)

    # A timeout must trigger the client reset (full rebuild covered by
    # test_reset_rebuilds_client_when_params_set).
    assert reset_spy.await_count == 1
    assert isinstance(exc_info.value, Exception)


@pytest.mark.asyncio
async def test_timeout_propagates_as_exception_for_retry_loop(mocker):
    async def hang(*_args, **_kwargs):
        await asyncio.Event().wait()

    connector = _make_connector(mocker, get_logs=hang)

    raised: BaseException | None = None
    try:
        await asyncio.wait_for(
            connector._get_logs_in_block_range(BlockNumber(0), BlockNumber(10)),
            # The inner client_timeout (0.05s) is what must fire; a tight outer
            # safety net makes that explicit while still preventing a hang.
            timeout=1,
        )
    except Exception as e:  # mirrors fetch_sync_events_task's handler
        raised = e

    assert raised is not None
    assert isinstance(raised, asyncio.TimeoutError)
    assert isinstance(raised, Exception)


@pytest.mark.asyncio
async def test_reset_rebuilds_client_when_params_set(mocker):
    """
    With rpc_url/chain_id/contract_address set, ``_reset_web3_client`` must
    disconnect the old provider and replace BOTH the web3 client and the
    contract with freshly built ones. Pure-mock, no network.
    """
    old_web3_client = mocker.MagicMock()
    old_web3_client.provider.disconnect = mocker.AsyncMock()
    old_contract = mocker.MagicMock()

    new_web3_client = mocker.MagicMock()
    new_contract = mocker.MagicMock()

    mocker.patch("aleph.chains.ethereum.make_web3_client", return_value=new_web3_client)
    mocker.patch(
        "aleph.chains.ethereum.get_contract",
        new=mocker.AsyncMock(return_value=new_contract),
    )

    connector = EthereumConnector(
        web3_client=old_web3_client,
        contract=old_contract,
        authorized_emitters=[],
        max_gas_price=0,
        start_height=BlockNumber(0),
        max_block_range=100,
        session_factory=mocker.MagicMock(),
        pending_tx_publisher=mocker.AsyncMock(),
        chain_data_service=mocker.AsyncMock(),
        # Dummy-but-set values so the rebuild path runs.
        rpc_url="http://localhost:8545",
        chain_id=1,
        client_timeout=0.05,
        contract_address="0x" + "0" * 40,
    )

    await connector._reset_web3_client()

    old_web3_client.provider.disconnect.assert_awaited_once()
    assert connector.web3_client is new_web3_client
    assert connector.contract is new_contract
