from typing import List

import pytest
from aleph_message.models import ItemType, Chain, MessageType

from aleph.db.accessors.pending_messages import (
    count_pending_messages,
    get_next_pending_messages,
)
from aleph.db.models import PendingMessageDb, ChainTxDb
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.db_session import DbSessionFactory
import datetime as dt


@pytest.fixture
def fixture_pending_messages():
    return [
        PendingMessageDb(
            id=404,
            item_hash="448b3c6f6455e6f4216b01b43522bddc3564a14c04799ed0ce8af4857c7ba15f",
            type=MessageType.forget,
            chain=Chain.ETH,
            sender="0xaC033C1cA5C49Eff98A1D9a56BeDBC4840010BA4",
            signature="0x3619c016987c4221c85842ce250f3e50a9b8e42c04d4f9fbdfdfad9941d6c5195a502a4f63289429513bf152d24d0a7bb0533701ec3c7bbca91b18ce7eaa7dee1b",
            item_type=ItemType.inline,
            item_content='{"address":"0xaC033C1cA5C49Eff98A1D9a56BeDBC4840010BA4","time":1648215809.0270267,"hashes":["fea0e00f73102aa951794a3ea85f6f1bbfd3decb804fb73232f2a645a379ae54"],"reason":"This well thought-out content offends me!"}',
            channel="INTEGRATION_TESTS",
            time=dt.datetime(2022, 10, 7, 17, 5),
            retries=0,
            check_message=True,
            tx=ChainTxDb(
                hash="0x1234",
                chain=Chain.ETH,
                height=8000,
                datetime=dt.datetime(2022, 10, 7, 17, 4),
                publisher="0xdeadb00t",
                protocol=ChainSyncProtocol.OFF_CHAIN,
                protocol_version=1,
                content="Qmsomething",
            ),
            reception_time=dt.datetime(2022, 10, 7, 17, 5, 10),
            fetched=True,
        ),
        PendingMessageDb(
            id=27,
            item_hash="53c2b16aa84b10878982a2920844625546f5db32337ecd9dd15928095a30381c",
            type=MessageType.aggregate,
            chain=Chain.ETH,
            sender="0x51A58800b26AA1451aaA803d1746687cB88E0501",
            signature="0x06b1cf4d70b40c858a2e3b424888ea0b7c59dc952b257496643095dc1190e964226e23ea75ad052538cdeb6d0f91a436e198c9cac552e18a166bee6ad88f1a5c1b",
            item_type=ItemType.inline,
            item_content='{"address":"0x720F319A9c3226dCDd7D8C49163D79EDa1084E98","time":1644857371.391834,"key":"test_reference","content":{"a":1,"b":2}}',
            channel="INTEGRATION_TESTS",
            time=dt.datetime(2022, 10, 7, 22, 10),
            retries=3,
            check_message=True,
            reception_time=dt.datetime(2022, 10, 7, 22, 10, 10),
            fetched=True,
        ),
        PendingMessageDb(
            id=42,
            item_hash="588ac154509de449b0915844fa1117c72b9136eaaabd078494ea5c5c39cd14b2",
            type=MessageType.store,
            chain=Chain.SOL,
            sender="BCma9zC6WmtCzS95sPauUGKMQmhAqe6eRboUmRZF1gR3",
            signature='{"signature":"4smt7h5q28Q8mZKtR8cLv1mJrYqhLCtT5warMrqauAv4NUGfWWDmaPKYB7kGmPWTKoVtmwuPXz88CSRGm3MgDjNF","publicKey":"BCma9zC6WmtCzS95sPauUGKMQmhAqe6eRboUmRZF1gR3"}',
            item_type=ItemType.inline,
            item_content='{"address":"0x720F319A9c3226dCDd7D8C49163D79EDa1084E98","time":1644857371.391834,"key":"test_reference","content":{"a":1,"b":2}}',
            channel="TEST",
            time=dt.datetime(2022, 10, 7, 21, 53),
            retries=0,
            check_message=True,
            tx=ChainTxDb(
                hash="0x4321",
                chain=Chain.TEZOS,
                height=1001,
                datetime=dt.datetime(2022, 10, 7, 21, 50),
                publisher="0xabadbabe",
                protocol=ChainSyncProtocol.OFF_CHAIN,
                protocol_version=1,
                content="Qmsomething",
            ),
            reception_time=dt.datetime(2022, 10, 7, 21, 53, 10),
            fetched=True,
        ),
    ]


@pytest.mark.asyncio
async def test_count_pending_messages(
    session_factory: DbSessionFactory, fixture_pending_messages: List[PendingMessageDb]
):
    with session_factory() as session:
        session.add_all(fixture_pending_messages)
        session.commit()

    with session_factory() as session:
        count_all = count_pending_messages(session=session)
        assert count_all == 3

        # Only one message is linked to an ETH transaction
        count_eth = count_pending_messages(session=session, chain=Chain.ETH)
        assert count_eth == 1

        # Only one message is linked to a TEZOS transaction
        count_tezos = count_pending_messages(session=session, chain=Chain.TEZOS)
        assert count_tezos == 1

        # No message should be linked to any Solana transaction
        count_sol = count_pending_messages(session=session, chain=Chain.SOL)
        assert count_sol == 0


@pytest.mark.asyncio
async def test_get_pending_messages(
    session_factory: DbSessionFactory, fixture_pending_messages: List[PendingMessageDb]
):
    with session_factory() as session:
        session.add_all(fixture_pending_messages)
        session.commit()

    with session_factory() as session:
        pending_messages = list(get_next_pending_messages(session=session))

        assert len(pending_messages) == 3
        # Check the order of messages
        assert [m.id for m in pending_messages] == [404, 42, 27]

        # Exclude hashes
        pending_messages = list(
            get_next_pending_messages(
                session=session,
                exclude_item_hashes={
                    "588ac154509de449b0915844fa1117c72b9136eaaabd078494ea5c5c39cd14b2"
                },
            )
        )
        assert len(pending_messages) == 2
        assert [m.id for m in pending_messages] == [404, 27]
