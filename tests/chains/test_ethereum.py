import json
from pathlib import Path

import pytest
import pytest_asyncio
from aleph_message.models import Chain
from configmanager import Config
from eth_account import Account
from hexbytes import HexBytes
from web3 import AsyncHTTPProvider, AsyncWeb3, Web3
from web3.middleware import ExtraDataToPOAMiddleware

from aleph.chains.ethereum import EthereumConnector, get_contract
from aleph.db.accessors.chains import upsert_chain_sync_status
from aleph.db.models import MessageDb
from aleph.toolkit.timestamp import timestamp_to_datetime, utc_now
from aleph.types.chain_sync import ChainEventType
from aleph.types.db_session import DbSessionFactory


@pytest_asyncio.fixture
async def web3(mock_config: Config):
    eth_api_url = mock_config.ethereum.api_url.value
    w3 = AsyncWeb3(AsyncHTTPProvider(eth_api_url))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
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
async def deployed_contract(web3, mock_config: Config, ethereum_sc_abi):
    if not await web3.is_connected():
        pytest.fail(f"Anvil node not found at {mock_config.ethereum.api_url.value}")

    test_address = "0x5FbDB2315678afecb367f032d93F642f64180aa3"
    await web3.provider.make_request("anvil_setCode", [test_address, ALEPH_SYNC_BYTECODE])

    return web3.eth.contract(address=test_address, abi=ethereum_sc_abi)


@pytest.mark.asyncio
async def test_get_contract(mock_config: Config, web3: AsyncWeb3):
    mock_config.ethereum.sync_contract.value = "0x" + "0" * 40
    contract = await get_contract(config=mock_config, web3=web3)
    assert contract.w3 == web3


@pytest.mark.asyncio
async def test_broadcast_messages(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
    web3: AsyncWeb3,
    deployed_contract,
):
    mock_config.ethereum.chain_id.value = await web3.eth.chain_id
    mock_config.ethereum.private_key.value = (
        "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
    )
    mock_config.ethereum.sync_contract.value = deployed_contract.address
    mock_config.ethereum.max_gas_price.value = 100000000000

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

    connector = EthereumConnector(
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

    gas_price = await web3.eth.gas_price

    response = await connector.broadcast_messages(
        config=mock_config,
        web3=web3,
        contract=deployed_contract,
        account=account,
        messages=messages,
        nonce=await web3.eth.get_transaction_count(account.address),
    )

    receipt = await web3.eth.wait_for_transaction_receipt(response)
    assert receipt.status == 1
    print(f"Gas used by broadcast_messages: {receipt.gasUsed}")
    print(f"Cost: {receipt.gasUsed * gas_price / 10**18} ETH")

    gas_estimate = await deployed_contract.functions.doEmit(json.dumps(jdata)).estimate_gas(
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
    web3: AsyncWeb3,
    deployed_contract,
):
    mock_config.ethereum.chain_id.value = 31337
    mock_config.ethereum.private_key.value = (
        "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
    )
    mock_config.ethereum.sync_contract.value = deployed_contract.address
    mock_config.ethereum.max_gas_price.value = 100000000000
    mock_config.ethereum.archive_delay.value = 0.1
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

    connector = EthereumConnector(
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
        config=mock_config,
        web3=web3,
        contract=deployed_contract,
        account=account,
        messages=messages,
        nonce=await web3.eth.get_transaction_count(account.address),
    )
    receipt = await web3.eth.wait_for_transaction_receipt(response)

    # 0. Set initial height to current to avoid picking up old events
    # We set it to receipt.blockNumber - 1 so that fetcher picks up the block where we just emitted
    with session_factory() as session:
        upsert_chain_sync_status(
            session=session,
            chain=Chain.ETH,
            sync_type=ChainEventType.SYNC,
            height=receipt.blockNumber - 1,
            update_datetime=utc_now(),
        )
        session.commit()

    # 2. Fetch the event
    with pytest.raises(StopTestException):
        await connector.fetch_ethereum_sync_events(config=mock_config)

    # 3. Verify
    pending_tx_publisher.add_and_publish_pending_tx.assert_called_once()
    call_args = pending_tx_publisher.add_and_publish_pending_tx.call_args
    assert call_args.kwargs["tx"].content == jdata["content"]
    assert call_args.kwargs["tx"].publisher == account.address
