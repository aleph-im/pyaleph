import pytest
from aleph_message.models import Chain, StoreContent, MessageType, ItemType

from aleph.chains.chaindata import ChainDataService
from aleph.db.models import ChainTxDb
from aleph.schemas.chains.tezos_indexer_response import MessageEventPayload
from aleph.schemas.pending_messages import parse_message
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.db_session import DbSessionFactory


@pytest.mark.asyncio
async def test_smart_contract_protocol_ipfs_store(
    mocker, session_factory: DbSessionFactory
):
    payload = MessageEventPayload(
        timestamp=1668611900,
        addr="KT1VBeLD7hzKpj17aRJ3Kc6QQFeikCEXi7W6",
        msgtype="STORE_IPFS",
        msgcontent="QmaMLRsvmDRCezZe2iebcKWtEzKNjBaQfwcu7mcpdm8eY2",
    )

    tx = ChainTxDb(
        hash="oorMNgusX6RxZ4NhzYriVDN8HDeMBNkjD3E8kx9a7j7dRRDGkzz",
        chain=Chain.TEZOS,
        height=584664,
        datetime=timestamp_to_datetime(1668611900),
        publisher="KT1BfL57oZfptdtMFZ9LNakEPvuPPA2urdSW",
        protocol=ChainSyncProtocol.SMART_CONTRACT,
        protocol_version=1,
        content=payload.dict(),
    )

    chain_data_service = ChainDataService(
        session_factory=session_factory, storage_service=mocker.AsyncMock()
    )

    pending_messages = await chain_data_service.get_tx_messages(tx)
    assert len(pending_messages) == 1
    pending_message_dict = pending_messages[0]

    # Check that the message is valid
    pending_message = parse_message(pending_message_dict)


    assert (
        pending_message.item_hash
        == "c83c515d48a8df8538f3a13eb2ee31b30b8f80c820ef2771c34e4b0b9e97e00f"
    )
    assert pending_message.sender == payload.addr
    assert pending_message.chain == Chain.TEZOS
    assert pending_message.signature is None
    assert pending_message.type == MessageType.store
    assert pending_message.item_type == ItemType.inline
    assert pending_message.channel is None

    message_content = StoreContent.parse_raw(pending_message.item_content)
    assert message_content.item_hash == payload.message_content
    assert message_content.item_type == ItemType.ipfs
    assert message_content.address == payload.addr
    assert message_content.time == payload.timestamp
