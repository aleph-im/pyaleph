import json
from pathlib import Path

import pytest
import pytest_asyncio
from aleph_message.models import Chain
from configmanager import Config
from eth_account import Account
from hexbytes import HexBytes
from web3 import AsyncHTTPProvider, AsyncWeb3
from web3.middleware import ExtraDataToPOAMiddleware
from web3.types import RPCEndpoint

from aleph.chains.ethereum import EthereumConnector, get_contract
from aleph.db.accessors.chains import upsert_chain_sync_status
from aleph.db.models import MessageDb
from aleph.toolkit.timestamp import timestamp_to_datetime, utc_now
from aleph.types.chain_sync import ChainEventType
from aleph.types.db_session import DbSessionFactory


@pytest_asyncio.fixture
async def web3_client(mock_config: Config):
    eth_api_url = mock_config.ethereum.api_url.value
    w3 = AsyncWeb3(AsyncHTTPProvider(eth_api_url))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    mock_config.ethereum.chain_id.value = await w3.eth.chain_id
    # Anvil funds 10 addresses by default, this is one of them
    mock_config.ethereum.private_key.value = (
        "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
    )

    yield w3
    await w3.provider.disconnect()


@pytest.fixture
def ethereum_sc_abi():
    abi_path = (
        Path(__file__).parent.parent.parent
        / "src/aleph/chains/assets/ethereum_sc_abi.json"
    )
    return json.loads(abi_path.read_text())


# Bytecode provided by the user (from Etherscan)
ALEPH_SYNC_BYTECODE = "0x608060405234801561001057600080fd5b50600436106100365760003560e01c8063128f72c51461003b57806358666c041461018d575b600080fd5b61018b6004803603604081101561005157600080fd5b810190808035906020019064010000000081111561006e57600080fd5b82018360208201111561008057600080fd5b803590602001918460018302840111640100000000831117156100a257600080fd5b91908080601f016020809104026020016040519081016040528093929190818152602001838380828437600081840152601f19601f8201169050808301925050505050505091929192908035906020019064010000000081111561010557600080fd5b82018360208201111561011757600080fd5b8035906020019184600183028401116401000000008311171561013957600080fd5b91908080601f016020809104026020016040519081016040528093929190818152602001838380828437600081840152601f19601f820116905080830192505050505050509192919290505050610248565b005b610246600480360360208110156101a357600080fd5b81019080803590602001906401000000008111156101c057600080fd5b8201836020820111156101d257600080fd5b803590602001918460018302840111640100000000831117156101f457600080fd5b91908080601f016020809104026020016040519081016040528093929190818152602001838380828437600081840152601f19601f820116905080830192505050505050509192919290505050610391565b005b7f548eda6c65f4bd05085e450712b22cfb19c58816b453f69294a94e78db9ab32b42338484604051808581526020018473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020018060200180602001838103835285818151815260200191508051906020019080838360005b838110156102e95780820151818401526020810190506102ce565b50505050905090810190601f1680156103165780820380516001836020036101000a031916815260200191505b50838103825284818151815260200191508051906020019080838360005b8381101561034f578082015181840152602081019050610334565b50505050905090810190601f16801561037c5780820380516001836020036101000a031916815260200191505b50965050505050505060405180910390a15050565b7f2cc768a3de0c950ea2a38e55a9b143cc6fd5a8eec27e36993b542fb16ab591e1423383604051808481526020018373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200180602001828103825283818151815260200191508051906020019080838360005b8381101561042d578082015181840152602081019050610412565b50505050905090810190601f16801561045a5780820380516001836020036101000a031916815260200191505b5094505050505060405180910390a15056fea265627a7a723158206d0449e9e697b867c893cbe3b2832f7deca73ad51111ddb373647a442e48015c64736f6c63430005110032"


@pytest_asyncio.fixture
async def deployed_contract(
    web3_client: AsyncWeb3, mock_config: Config, ethereum_sc_abi
):
    if not await web3_client.is_connected():
        pytest.fail(f"Anvil node not found at {mock_config.ethereum.api_url.value}")

    test_address = AsyncWeb3.to_checksum_address(
        "0x5FbDB2315678afecb367f032d93F642f64180aa3"
    )
    await web3_client.provider.make_request(
        RPCEndpoint("anvil_setCode"), [test_address, ALEPH_SYNC_BYTECODE]
    )

    mock_config.ethereum.sync_contract.value = test_address
    return web3_client.eth.contract(address=test_address, abi=ethereum_sc_abi)


@pytest.mark.asyncio
async def test_get_contract(web3_client: AsyncWeb3):
    contract_address = AsyncWeb3.to_checksum_address("0x" + "0" * 40)
    contract = await get_contract(
        web3_client=web3_client, contract_address=contract_address
    )
    assert contract.w3 == web3_client


@pytest.mark.asyncio
async def test_broadcast_messages(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
    web3_client: AsyncWeb3,
    deployed_contract,
):
    mock_config.ethereum.max_gas_price.value = 100_000_000_000

    pending_tx_publisher = mocker.AsyncMock()
    chain_data_service = mocker.AsyncMock()

    mock_payload = mocker.MagicMock()
    jdata = {
        "protocol": "on_chain_sync",
        "version": 1,
        "content": {"test": "data"},
    }
    mock_payload.json.return_value = json.dumps(jdata)
    chain_data_service.prepare_sync_event_payload = mocker.AsyncMock(
        return_value=mock_payload
    )

    connector = await EthereumConnector.new(
        config=mock_config,
        session_factory=session_factory,
        pending_tx_publisher=pending_tx_publisher,
        chain_data_service=chain_data_service,
    )

    account = Account.from_key(HexBytes(mock_config.ethereum.private_key.value))
    messages = [
        MessageDb(
            item_hash="hash",
            type="STORE",
            chain=Chain.ETH,
            sender="sender",
            signature="sig",
            item_type="inline",
            item_content="content",
            content={"address": "sender", "time": 1600000000},
            time=timestamp_to_datetime(1600000000),
            size=0,
        )
    ]

    gas_price = await web3_client.eth.gas_price

    response = await connector.broadcast_messages(
        account=account,
        messages=messages,
        nonce=await web3_client.eth.get_transaction_count(account.address),
    )

    receipt = await web3_client.eth.wait_for_transaction_receipt(response)
    assert receipt["status"] == 1
    print(f"Gas used by broadcast_messages: {receipt['gasUsed']}")
    print(f"Cost: {receipt['gasUsed'] * gas_price / 10**18} ETH")

    gas_estimate = await deployed_contract.functions.doEmit(
        json.dumps(jdata)
    ).estimate_gas(
        {
            "from": account.address,
        }
    )
    print(f"Estimated gas for doEmit: {gas_estimate}")

    chain_data_service.prepare_sync_event_payload.assert_called_once()


class StopTestException(Exception):
    pass


@pytest.mark.asyncio
async def test_fetch_ethereum_sync_events(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
    web3_client: AsyncWeb3,
    deployed_contract,
):
    mock_config.ethereum.max_gas_price.value = 100_000_000_000
    mock_config.ethereum.message_delay.value = 0.1
    mock_config.ethereum.client_timeout.value = 1

    account = Account.from_key(HexBytes(mock_config.ethereum.private_key.value))
    mock_config.ethereum.authorized_emitters.value = [account.address]

    pending_tx_publisher = mocker.AsyncMock()
    # We want to stop the infinite loop after one call
    pending_tx_publisher.add_and_publish_pending_tx.side_effect = StopTestException

    chain_data_service = mocker.AsyncMock()
    mock_payload = mocker.MagicMock()
    # The expected data structure for SyncEvent message
    jdata = {
        "protocol": "on_chain_sync",
        "version": 1,
        "content": {"test": "data"},
    }
    mock_payload.json.return_value = json.dumps(jdata)
    chain_data_service.prepare_sync_event_payload = mocker.AsyncMock(
        return_value=mock_payload
    )

    connector = await EthereumConnector.new(
        config=mock_config,
        session_factory=session_factory,
        pending_tx_publisher=pending_tx_publisher,
        chain_data_service=chain_data_service,
    )

    # 1. Broadcast a message to emit SyncEvent
    messages = [
        MessageDb(
            item_hash="hash",
            type="STORE",
            chain=Chain.ETH,
            sender="sender",
            signature="sig",
            item_type="inline",
            item_content="content",
            content={"address": "sender", "time": 1600000000},
            time=timestamp_to_datetime(1600000000),
            size=0,
        )
    ]

    response = await connector.broadcast_messages(
        account=account,
        messages=messages,
        nonce=await web3_client.eth.get_transaction_count(account.address),
    )
    receipt = await web3_client.eth.wait_for_transaction_receipt(response)

    # 0. Set initial height to current to avoid picking up old events
    # We set it to receipt.blockNumber - 1 so that fetcher picks up the block where we just emitted
    with session_factory() as session:
        upsert_chain_sync_status(
            session=session,
            chain=Chain.ETH,
            sync_type=ChainEventType.SYNC,
            height=receipt["blockNumber"] - 1,
            update_datetime=utc_now(),
        )
        session.commit()

    # 2. Fetch the event
    with pytest.raises(StopTestException):
        await connector.fetch_ethereum_sync_events()

    # 3. Verify
    pending_tx_publisher.add_and_publish_pending_tx.assert_called_once()
    call_args = pending_tx_publisher.add_and_publish_pending_tx.call_args
    assert call_args.kwargs["tx"].content == jdata["content"]
    assert call_args.kwargs["tx"].publisher == account.address


@pytest.mark.asyncio
async def test_fetch_ethereum_sync_events_repeated_sync(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
    web3_client: AsyncWeb3,
    deployed_contract,
):
    mock_config.ethereum.max_gas_price.value = 100_000_000_000
    mock_config.ethereum.message_delay.value = 0.1
    mock_config.ethereum.client_timeout.value = 1

    account = Account.from_key(HexBytes(mock_config.ethereum.private_key.value))
    mock_config.ethereum.authorized_emitters.value = [account.address]

    pending_tx_publisher = mocker.AsyncMock()

    chain_data_service = mocker.AsyncMock()
    mock_payload = mocker.MagicMock()
    # The expected data structure for SyncEvent message
    jdata = {
        "protocol": "on_chain_sync",
        "version": 1,
        "content": {"test": "data"},
    }
    mock_payload.json.return_value = json.dumps(jdata)
    chain_data_service.prepare_sync_event_payload = mocker.AsyncMock(
        return_value=mock_payload
    )

    connector = await EthereumConnector.new(
        config=mock_config,
        session_factory=session_factory,
        pending_tx_publisher=pending_tx_publisher,
        chain_data_service=chain_data_service,
    )

    messages = [
        MessageDb(
            item_hash="hash",
            type="STORE",
            chain=Chain.ETH,
            sender="sender",
            signature="sig",
            item_type="inline",
            item_content="content",
            content={"address": "sender", "time": 1600000000},
            time=timestamp_to_datetime(1600000000),
            size=0,
        )
    ]

    # 1. First sync
    response = await connector.broadcast_messages(
        account=account,
        messages=messages,
        nonce=await web3_client.eth.get_transaction_count(account.address),
    )
    receipt = await web3_client.eth.wait_for_transaction_receipt(response)

    # Set initial height
    with session_factory() as session:
        upsert_chain_sync_status(
            session=session,
            chain=Chain.ETH,
            sync_type=ChainEventType.SYNC,
            height=receipt["blockNumber"] - 1,
            update_datetime=utc_now(),
        )
        session.commit()

    # We want to stop the infinite loop after one call
    pending_tx_publisher.add_and_publish_pending_tx.side_effect = StopTestException
    with pytest.raises(StopTestException):
        await connector.fetch_ethereum_sync_events()

    assert pending_tx_publisher.add_and_publish_pending_tx.call_count == 1
    pending_tx_publisher.add_and_publish_pending_tx.reset_mock()

    # Update sync status in DB then move on to next block for next sync message
    with session_factory() as session:
        upsert_chain_sync_status(
            session=session,
            chain=Chain.ETH,
            sync_type=ChainEventType.SYNC,
            height=receipt["blockNumber"],
            update_datetime=utc_now(),
        )
        session.commit()

    await web3_client.provider.make_request(RPCEndpoint("anvil_mine"), [1])

    # 2. Second sync
    jdata2 = {
        "protocol": "on_chain_sync",
        "version": 1,
        "content": {"test": "data2"},
    }
    mock_payload2 = mocker.MagicMock()
    mock_payload2.json.return_value = json.dumps(jdata2)
    chain_data_service.prepare_sync_event_payload.return_value = mock_payload2

    messages2 = [
        MessageDb(
            item_hash="hash2",
            type="STORE",
            chain=Chain.ETH,
            sender="sender",
            signature="sig2",
            item_type="inline",
            item_content="content2",
            content={"address": "sender", "time": 1600000001},
            time=timestamp_to_datetime(1600000001),
            size=0,
        )
    ]

    _ = await connector.broadcast_messages(
        account=account,
        messages=messages2,
        nonce=await web3_client.eth.get_transaction_count(account.address),
    )

    pending_tx_publisher.add_and_publish_pending_tx.side_effect = StopTestException
    with pytest.raises(StopTestException):
        await connector.fetch_ethereum_sync_events()

    assert pending_tx_publisher.add_and_publish_pending_tx.call_count == 1
    call_args = pending_tx_publisher.add_and_publish_pending_tx.call_args
    assert call_args.kwargs["tx"].content == jdata2["content"]


@pytest.mark.asyncio
async def test_fetch_ethereum_sync_events_sync_failure(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
    web3_client: AsyncWeb3,
    deployed_contract,
):
    mock_config.ethereum.max_gas_price.value = 100_000_000_000
    mock_config.ethereum.message_delay.value = 0.1
    mock_config.ethereum.client_timeout.value = 1

    account = Account.from_key(HexBytes(mock_config.ethereum.private_key.value))
    mock_config.ethereum.authorized_emitters.value = [account.address]

    pending_tx_publisher = mocker.AsyncMock()

    chain_data_service = mocker.AsyncMock()
    mock_payload = mocker.MagicMock()
    jdata = {
        "protocol": "on_chain_sync",
        "version": 1,
        "content": {"test": "data"},
    }
    mock_payload.json.return_value = json.dumps(jdata)
    chain_data_service.prepare_sync_event_payload = mocker.AsyncMock(
        return_value=mock_payload
    )

    connector = await EthereumConnector.new(
        config=mock_config,
        session_factory=session_factory,
        pending_tx_publisher=pending_tx_publisher,
        chain_data_service=chain_data_service,
    )

    # 1. Sync one message successfully
    messages = [
        MessageDb(
            item_hash="hash",
            type="STORE",
            chain=Chain.ETH,
            sender="sender",
            signature="sig",
            item_type="inline",
            item_content="content",
            content={"address": "sender", "time": 1600000000},
            time=timestamp_to_datetime(1600000000),
            size=0,
        )
    ]
    response = await connector.broadcast_messages(
        account=account,
        messages=messages,
        nonce=await web3_client.eth.get_transaction_count(account.address),
    )
    receipt = await web3_client.eth.wait_for_transaction_receipt(response)

    with session_factory() as session:
        upsert_chain_sync_status(
            session=session,
            chain=Chain.ETH,
            sync_type=ChainEventType.SYNC,
            height=receipt["blockNumber"] - 1,
            update_datetime=utc_now(),
        )
        session.commit()

    pending_tx_publisher.add_and_publish_pending_tx.side_effect = StopTestException
    with pytest.raises(StopTestException):
        await connector.fetch_ethereum_sync_events()

    assert pending_tx_publisher.add_and_publish_pending_tx.call_count == 1
    pending_tx_publisher.add_and_publish_pending_tx.reset_mock()

    # Now last_synced_height in DB should be receipt.blockNumber - 1
    # because we stopped before _request_transactions could update the height in DB
    last_height = await connector.get_last_height(ChainEventType.SYNC)
    assert last_height == receipt["blockNumber"] - 1

    # 2. Trigger an exception in fetch_ethereum_sync_events
    # We can mock _request_transactions to raise an exception
    with mocker.patch.object(
        connector, "_request_transactions", side_effect=Exception("RPC Error")
    ):
        # fetch_sync_events_task calls fetch_ethereum_sync_events and catches Exception
        # We need to mock asyncio.sleep to avoid waiting 10 seconds
        with mocker.patch("asyncio.sleep", side_effect=StopTestException):
            with pytest.raises(StopTestException):
                await connector.fetch_sync_events_task(poll_interval=1)

    # 3. Verify it would restart from last_height
    # Check that it still has the correct height in DB
    assert await connector.get_last_height(ChainEventType.SYNC) == last_height


@pytest.mark.asyncio
async def test_fetch_ethereum_sync_events_batching(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
    web3_client: AsyncWeb3,
    deployed_contract,
):
    mock_config.ethereum.chain_id.value = 31337
    mock_config.ethereum.private_key.value = (
        "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
    )
    mock_config.ethereum.max_gas_price.value = 100_000_000_000
    mock_config.ethereum.message_delay.value = 0.1
    mock_config.ethereum.client_timeout.value = 1

    account = Account.from_key(HexBytes(mock_config.ethereum.private_key.value))
    mock_config.ethereum.authorized_emitters.value = [account.address]

    pending_tx_publisher = mocker.AsyncMock()
    chain_data_service = mocker.AsyncMock()

    # Create 3 messages
    jdatas = [
        {"protocol": "on_chain_sync", "version": 1, "content": {"test": f"data{i}"}}
        for i in range(3)
    ]

    async def mock_prepare_sync_event_payload(session, messages):
        m = mocker.MagicMock()
        # Find which message we are preparing
        for jdata in jdatas:
            if jdata["content"]["test"] == messages[0].item_hash:
                m.json.return_value = json.dumps(jdata)
                return m
        return m

    chain_data_service.prepare_sync_event_payload.side_effect = (
        mock_prepare_sync_event_payload
    )

    connector = await EthereumConnector.new(
        config=mock_config,
        session_factory=session_factory,
        pending_tx_publisher=pending_tx_publisher,
        chain_data_service=chain_data_service,
    )
    # Set max_block_range to 1 to force the connector to sync in multiple iterations
    connector.max_block_range = 1

    start_height = await web3_client.eth.block_number

    for i in range(3):
        messages = [
            MessageDb(
                item_hash=f"data{i}",
                type="STORE",
                chain=Chain.ETH,
                sender="sender",
                signature=f"sig{i}",
                item_type="inline",
                item_content=f"content{i}",
                content={"address": "sender", "time": 1600000000 + i},
                time=timestamp_to_datetime(1600000000 + i),
                size=0,
            )
        ]
        response = await connector.broadcast_messages(
            account=account,
            messages=messages,
            nonce=await web3_client.eth.get_transaction_count(account.address),
        )
        await web3_client.eth.wait_for_transaction_receipt(response)

    with session_factory() as session:
        upsert_chain_sync_status(
            session=session,
            chain=Chain.ETH,
            sync_type=ChainEventType.SYNC,
            height=start_height,
            update_datetime=utc_now(),
        )
        session.commit()

    # We expect 3 calls to add_and_publish_pending_tx, then StopTestException
    pending_tx_publisher.add_and_publish_pending_tx.side_effect = [
        None,
        None,
        StopTestException,
    ]

    with pytest.raises(StopTestException):
        await connector.fetch_ethereum_sync_events()

    assert pending_tx_publisher.add_and_publish_pending_tx.call_count == 3
