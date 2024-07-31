import datetime as dt

import pytest
from aleph_message.models import Chain, MessageType, ItemType, ItemHash
from configmanager import Config
from more_itertools import one
from sqlalchemy import select

from aleph.db.accessors.files import count_file_pins, get_file
from aleph.db.accessors.messages import get_message_status, get_forgotten_message
from aleph.db.accessors.posts import get_post
from aleph.db.models import (
    PendingMessageDb,
    StoredFileDb,
    MessageDb,
    MessageStatusDb,
    FilePinDb,
    MessageFilePinDb,
    GracePeriodFilePinDb,
)
from aleph.handlers.content.aggregate import AggregateMessageHandler
from aleph.handlers.content.forget import ForgetMessageHandler
from aleph.handlers.content.post import PostMessageHandler
from aleph.handlers.content.vm import VmMessageHandler
from aleph.handlers.content.store import StoreMessageHandler
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType
from aleph.types.message_processing_result import ProcessedMessage, RejectedMessage
from aleph.types.message_status import MessageStatus
from message_test_helpers import (
    process_pending_messages,
)


@pytest.fixture
def forget_handler(mocker) -> ForgetMessageHandler:
    vm_handler = VmMessageHandler()
    content_handlers = {
        MessageType.aggregate: AggregateMessageHandler(),
        MessageType.instance: vm_handler,
        MessageType.post: PostMessageHandler(
            balances_addresses=["nope"],
            balances_post_type="no-balances-in-tests",
        ),
        MessageType.program: vm_handler,
        MessageType.store: StoreMessageHandler(
            storage_service=mocker.AsyncMock(), grace_period=24
        ),
    }
    return ForgetMessageHandler(content_handlers=content_handlers)


@pytest.mark.asyncio
async def test_forget_post_message(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    mock_config: Config,
):
    target_message_dict = {
        "chain": "ETH",
        "channel": "TEST",
        "sender": "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
        "type": "POST",
        "time": 1652786281.9810653,
        "item_type": "inline",
        "item_content": '{"address":"0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106","time":1652786281.980474,"content":{"body":"This message will be destroyed"},"type":"test"}',
        "item_hash": "fc1e7b1edc2348eb78303fb1342e31e5ad3820249629032d37f8223e754a5f8e",
        "signature": "0xdd8f7061d3c8e7019110b6dc0697c71ae8da5295e26f1d20c265bcb78fc616a05d3927f72888a459c048a297ff17c748ad3803e5f95bf000e3e4c0feba350e101c",
        "content": {
            "address": "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
            "time": 1652786281.980474,
            "content": {"body": "This message will be destroyed"},
            "type": "test",
        },
    }
    forget_message_dict = {
        "chain": "ETH",
        "channel": "TEST",
        "sender": "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
        "type": "FORGET",
        "time": 1652786534.1139255,
        "item_type": "inline",
        "item_content": '{"address":"0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106","time":1652786534.1138077,"hashes":["fc1e7b1edc2348eb78303fb1342e31e5ad3820249629032d37f8223e754a5f8e"]}',
        "item_hash": "431a0d2f79ecfa859949d2a09f67068ce7ebd4eb777d179ad958be6c79abc66b",
        "signature": "0x409cdef65af51d6a508a1fdc56c0baa6d1abac7f539ab5f290e3245c522a4c766b930c4196d9f5d8c8c94a4d36c4b65bf04a2773f058f03803b9b0bca2fd85a51b",
        "content": {
            "address": "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
            "time": 1652786534.1138077,
            "hashes": [
                "fc1e7b1edc2348eb78303fb1342e31e5ad3820249629032d37f8223e754a5f8e"
            ],
        },
    }

    pending_message = PendingMessageDb.from_message_dict(
        target_message_dict, reception_time=dt.datetime(2022, 1, 1), fetched=True
    )
    pending_forget_message = PendingMessageDb.from_message_dict(
        forget_message_dict, reception_time=dt.datetime(2022, 1, 2), fetched=True
    )

    with session_factory() as session:
        target_message_result = one(
            await process_pending_messages(
                message_processor=message_processor,
                pending_messages=[pending_message],
                session=session,
            )
        )
        assert isinstance(target_message_result, ProcessedMessage)
        target_message = target_message_result.message

        # Sanity check
        post = get_post(session=session, item_hash=ItemHash(target_message.item_hash))
        assert post

        # Now process, the forget message
        forget_message_result = one(
            await process_pending_messages(
                message_processor=message_processor,
                pending_messages=[pending_forget_message],
                session=session,
            )
        )
        assert isinstance(forget_message_result, ProcessedMessage)
        forget_message = forget_message_result.message

        target_message_status = get_message_status(
            session=session, item_hash=ItemHash(target_message.item_hash)
        )
        assert target_message_status
        assert target_message_status.status == MessageStatus.FORGOTTEN

        forget_message_status = get_message_status(
            session=session, item_hash=ItemHash(forget_message.item_hash)
        )
        assert forget_message_status
        assert forget_message_status.status == MessageStatus.PROCESSED

        forgotten_message = get_forgotten_message(
            session=session, item_hash=ItemHash(target_message.item_hash)
        )
        assert forgotten_message

        # Check that the post was deleted
        post = get_post(session=session, item_hash=ItemHash(target_message.item_hash))
        assert post is None


@pytest.mark.asyncio
async def test_forget_store_message(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    mock_config: Config,
):
    file_hash = "5ccdd7bccfbc5955e2e40166dd0cdea0b093154fd87bc2bea57e7c768cde2f21"

    pending_message = PendingMessageDb(
        item_hash="f6fc4884e3ec3624bd3f60a3c37abf83a130777086061b1a373e659f2bab4d06",
        chain=Chain.ETH,
        sender="0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
        signature="0x7b87c29388a7a452353f9cae8718b66158fb5bdc93f032964226745ee04919092550791b93f79e5ee1981f2d9d6e5ac0cae0d28b68bb63fe0fcbd79015a6f3ea1b",
        type=MessageType.store,
        time=timestamp_to_datetime(1652794362.573859),
        item_type=ItemType.inline,
        item_content='{"address":"0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106","time":1652794362.5736332,"item_type":"storage","item_hash":"5ccdd7bccfbc5955e2e40166dd0cdea0b093154fd87bc2bea57e7c768cde2f21","mime_type":"text/plain"}',
        channel=Channel("TEST"),
        retries=0,
        next_attempt=dt.datetime(2023, 1, 1),
        check_message=True,
        fetched=True,
        reception_time=dt.datetime(2022, 1, 1),
    )

    pending_forget_message = PendingMessageDb(
        item_hash="5e40c8e2197e0678b5fba9cb1679e3a80fa6aeaa1a440d94f059525295fa32d3",
        chain=Chain.ETH,
        sender="0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
        signature="0xc342e671be10894bf707b86c3f7538cdb7e4bb5760e234f8d07f8b3dfde015492337bd8756f169e37ac691b74c765415e96b6e1813238912e10ea54cc003887d1b",
        type=MessageType.forget,
        time=timestamp_to_datetime(1652794384.3102906),
        item_type=ItemType.inline,
        item_content='{"address":"0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106","time":1652794384.3101473,"hashes":["f6fc4884e3ec3624bd3f60a3c37abf83a130777086061b1a373e659f2bab4d06"]}',
        channel=Channel("TEST"),
        retries=0,
        next_attempt=dt.datetime(2023, 1, 2),
        check_message=True,
        fetched=True,
        reception_time=dt.datetime(2022, 1, 2),
    )

    storage_engine = message_processor.message_handler.storage_service.storage_engine
    await storage_engine.write(
        filename=file_hash,
        content=b"Test",
    )

    with session_factory() as session:
        target_message_result = one(
            await process_pending_messages(
                message_processor=message_processor,
                pending_messages=[pending_message],
                session=session,
            )
        )
        assert isinstance(target_message_result, ProcessedMessage)

        # Sanity check
        nb_references = count_file_pins(session=session, file_hash=file_hash)
        assert nb_references == 1

        forget_message_result = one(
            await process_pending_messages(
                message_processor=message_processor,
                pending_messages=[pending_forget_message],
                session=session,
            )
        )
        assert isinstance(forget_message_result, ProcessedMessage)

        # Check that the file is pinned with a grace period
        file = get_file(session=session, file_hash=file_hash)
        assert file
        assert file.pins

        assert len(file.pins) == 1
        assert isinstance(file.pins[0], GracePeriodFilePinDb)


@pytest.mark.asyncio
async def test_forget_forget_message(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    mock_config: Config,
):
    """
    Checks that users cannot forget a FORGET message.

    * The target message should remain in the DB as a processed message
    * The FORGET message should be rejected.
    """

    target_message = MessageDb(
        item_hash="e3b24727335e34016247c0d37e2b0203bb8c2d76deddafc1700b4cf0e13845c5",
        chain=Chain.ETH,
        sender="0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
        signature="0xabfa661aab1a9f58955940ea213387de4773f8b1f244c2236cd4ac5ba7bf2ba902e17074bc4b289ba200807bb40951f4249668b055dc15af145b8842ecfad0601c",
        item_type=ItemType.storage,
        type=MessageType.forget,
        item_content=None,
        content={
            "address": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
            "time": 1645794065.439,
            "hashes": ["QmTQPocJ8n3r7jhwYxmCDR5bJ4SNsEhdVm8WwkNbGctgJF"],
            "reason": "None",
        },
        size=154,
        time=timestamp_to_datetime(1645794065.439),
        channel=Channel("TEST"),
    )

    pending_forget_message = PendingMessageDb(
        item_hash="884dd713e94fa0350239b67e65eecaa54361df8af0e3f6d0e42e0f8de059e15a",
        chain=Chain.ETH,
        sender="0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
        signature="0x7dc7a45aab12d78367c085799d06ef2e98fce31f76ca06975ce570fe4d92008f66f307bf68ed3ca450d04d4e779776ca13a1e7851cb48915bd390389ae4afd1b1c",
        type=MessageType.forget,
        time=timestamp_to_datetime(1639058312.376),
        item_type=ItemType.inline,
        item_content='{"address":"0xB68B9D4f3771c246233823ed1D3Add451055F9Ef","time":1639058312.376,"hashes":["e3b24727335e34016247c0d37e2b0203bb8c2d76deddafc1700b4cf0e13845c5"],"reason":"None"}',
        channel=Channel("TEST"),
        retries=0,
        next_attempt=dt.datetime(2023, 1, 1),
        check_message=True,
        fetched=True,
        reception_time=dt.datetime(2022, 1, 2),
    )

    with session_factory() as session:
        session.add(target_message)
        session.add(
            MessageStatusDb(
                item_hash=target_message.item_hash,
                status=MessageStatus.PROCESSED,
                reception_time=dt.datetime(2022, 1, 1),
            )
        )
        session.commit()

        processed_message_results = list(
            await process_pending_messages(
                message_processor=message_processor,
                pending_messages=[pending_forget_message],
                session=session,
            )
        )

        # The message should have been rejected
        for result in processed_message_results:
            assert isinstance(result, RejectedMessage)

        target_message_status = get_message_status(
            session=session, item_hash=ItemHash(target_message.item_hash)
        )
        assert target_message_status
        assert target_message_status.status == MessageStatus.PROCESSED

        forget_message_status = get_message_status(
            session=session, item_hash=ItemHash(pending_forget_message.item_hash)
        )
        assert forget_message_status
        assert forget_message_status.status == MessageStatus.REJECTED


@pytest.mark.asyncio
async def test_forget_store_multi_users(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    mock_config: Config,
):
    """
    Tests that a file stored by two different users is not deleted if one of the users
    deletes the content with a FORGET message.

    * User 1 should be able to forget his own STORE message
    * The message from user 2 storing the same file should be unaffected
    * The file should still be present in local storage
    * The reference to the file from user 2 should still be present in the DB.
    """

    file_hash = "05a123fe17aa6addeef5a97d1665878d10f076d84309d5ae674d4bb292b484c3"
    file_size = 220916
    file_content = b"Test forget STORE multi-users"

    store_message_user1 = MessageDb(
        item_hash="50635384e43c7af6b3297f6571644c30f3f07ac681bfd14b9c556c63e661a69e",
        chain=Chain.ETH,
        sender="0x971300C78A38e0F85E60A3b04ae3fA70b4276B64",
        signature="0x71263de6b8d1ea4c0b028f5892287505f6ee73dfa165d1455ca665ffdf5318955345c193a5df2f5c4eb2185947689d7bf5be36155b00711572fec5f27764625c1b",
        item_type=ItemType.storage,
        type=MessageType.store,
        item_content=None,
        content={
            "address": "0x971300C78A38e0F85E60A3b04ae3fA70b4276B64",
            "time": 1651757380.8522494,
            "item_type": "storage",
            "item_hash": file_hash,
            "size": file_size,
            "content_type": "file",
        },
        size=230,
        time=timestamp_to_datetime(1646123806),
        channel=Channel("TESTS_FORGET"),
    )

    store_message_user2 = MessageDb(
        item_hash="dbe8199004b052108ec19618f43af1d2baf5c04974d0aec1c4de2d02c44a2483",
        chain=Chain.ETH,
        sender="0xaC033C1cA5C49Eff98A1D9a56BeDBC4840010BA4",
        signature="0x4c9ef501e1e4f4b0a05c1eebfa1063837a82788f80deeb59808d25ff481c855157dd65102eaa365e33c7572a78d551cf25075f49d00ebb60c8506c0a6647ab761b",
        item_type=ItemType.storage,
        type=MessageType.store,
        item_content=None,
        content={
            "address": "0xaC033C1cA5C49Eff98A1D9a56BeDBC4840010BA4",
            "time": 1651757416.2203836,
            "item_type": "storage",
            "item_hash": file_hash,
            "size": file_size,
            "content_type": "file",
        },
        size=230,
        time=timestamp_to_datetime(1646123806),
        channel=Channel("TESTS_FORGET"),
    )

    pending_forget_message = PendingMessageDb(
        item_hash="0223e74dbae53b45da6a443fa18fd2a25f88677c82ed2de93f17ab24f78f58cf",
        chain=Chain.ETH,
        sender="0x971300C78A38e0F85E60A3b04ae3fA70b4276B64",
        signature="0x6682e797c424c8e5def6758867e25f08279afc3e976dbaaefdb9f650eee18d26595fc4e2f18fd4cdd853558140ecbb824e0ea8d221e12267862903fa904fabee1c",
        type=MessageType.forget,
        time=timestamp_to_datetime(1639058312.376),
        item_type=ItemType.inline,
        item_content='{"address": "0x971300C78A38e0F85E60A3b04ae3fA70b4276B64", "time": 1651757583.4974332, "hashes": ["50635384e43c7af6b3297f6571644c30f3f07ac681bfd14b9c556c63e661a69e"], "reason": "I do not like this file"}',
        channel=Channel("TESTS_FORGET"),
        retries=0,
        next_attempt=dt.datetime(2023, 1, 1),
        check_message=True,
        fetched=True,
        reception_time=dt.datetime(2022, 1, 2),
    )

    storage_engine = message_processor.message_handler.storage_service.storage_engine
    await storage_engine.write(filename=file_hash, content=file_content)

    with session_factory() as session:
        # Add messages, file references, etc
        session.add(store_message_user1)
        session.add(store_message_user2)
        session.add(
            MessageStatusDb(
                item_hash=store_message_user1.item_hash,
                status=MessageStatus.PROCESSED,
                reception_time=dt.datetime(2022, 1, 1),
            )
        )
        session.add(
            MessageStatusDb(
                item_hash=store_message_user2.item_hash,
                status=MessageStatus.PROCESSED,
                reception_time=dt.datetime(2022, 1, 2),
            )
        )
        session.add(
            StoredFileDb(
                hash=file_hash,
                size=file_size,
                type=FileType.FILE,
            )
        )
        session.add(
            MessageFilePinDb(
                file_hash=file_hash,
                owner=store_message_user1.sender,
                item_hash=store_message_user1.item_hash,
                created=store_message_user1.time,
            )
        )
        session.add(
            MessageFilePinDb(
                file_hash=file_hash,
                owner=store_message_user2.sender,
                item_hash=store_message_user2.item_hash,
                created=store_message_user2.time,
            )
        )

        # Process the FORGET message
        forget_message_result = one(
            await process_pending_messages(
                message_processor=message_processor,
                pending_messages=[pending_forget_message],
                session=session,
            )
        )
        assert isinstance(forget_message_result, ProcessedMessage)

        message1_status = get_message_status(
            session=session, item_hash=ItemHash(store_message_user1.item_hash)
        )
        assert message1_status
        assert message1_status.status == MessageStatus.FORGOTTEN

        # Check that the second message and its linked objects are still there
        message2_status = get_message_status(
            session=session, item_hash=ItemHash(store_message_user2.item_hash)
        )
        assert message2_status
        assert message2_status.status == MessageStatus.PROCESSED
        file_pin = session.execute(
            select(FilePinDb).where(FilePinDb.file_hash == file_hash)
        ).scalar_one()
        assert file_pin.item_hash == store_message_user2.item_hash
        assert file_pin.owner == store_message_user2.sender

        # Check that the file is still there
        content = await storage_engine.read(filename=file_hash)
        assert content == file_content

@pytest.mark.asyncio
async def test_fully_forget_newer_first(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    mock_config: Config,
):
    file_hash = "7acb40cd206a0be1eff8c5ad59dff7cfaca860be75b0d7de8646bff458b5d3ed"

    pending_message1 = PendingMessageDb(
        item_hash="8eeb2af0e5503f1d2140dcd7deda4e1bb7fcc2899ce4d8138a5cacb29ea5de31",
        chain=Chain.ETH,
        sender="0xd463495a6FEaC9921FD0C3a595B81E7B2C02B57d",
        signature="0x80a55df25183a849e40d797c5e3059163593fe1910451ee7c8975cfcadeeb6763bb2537e5aead138f1b2a5216d6d74fe13b2f80731797dcb08256ea7c6c54f891c",
        type=MessageType.store,
        time=timestamp_to_datetime(1722420694.323802),
        item_type=ItemType.inline,
        item_content='{"address":"0xd463495a6FEaC9921FD0C3a595B81E7B2C02B57d","time":1722420694.323802,"item_type":"storage","item_hash":"7acb40cd206a0be1eff8c5ad59dff7cfaca860be75b0d7de8646bff458b5d3ed"}',
        channel=None,
        retries=0,
        next_attempt=dt.datetime(2023, 1, 1),
        check_message=True,
        fetched=True,
        reception_time=dt.datetime(2022, 1, 1),
    )

    pending_message2 = PendingMessageDb(
        item_hash="356ea9d231e036c442cc6d330475101c0c39c83bef7e945d0b9cfa4156715519",
        chain=Chain.ETH,
        sender="0xd463495a6FEaC9921FD0C3a595B81E7B2C02B57d",
        signature="0xa7b0318668d30af3bbb3348f2d7750e5d58ea3750294d2f6facb5f40f47956787f5926ce94c61e9e08a5e6967291c5783295a55be43700d93a1ce398c40c40731b",
        type=MessageType.store,
        time=timestamp_to_datetime(1722420696.4441047),
        item_type=ItemType.inline,
        item_content='{"address":"0xd463495a6FEaC9921FD0C3a595B81E7B2C02B57d","time":1722420696.4441047,"item_type":"storage","item_hash":"7acb40cd206a0be1eff8c5ad59dff7cfaca860be75b0d7de8646bff458b5d3ed"}',
        channel=None,
        retries=0,
        next_attempt=dt.datetime(2023, 1, 2),
        check_message=True,
        fetched=True,
        reception_time=dt.datetime(2022, 1, 2),
    )

    pending_forget_message_newer = PendingMessageDb(
        item_hash="9505db13e325effbf45d53271de868af1103bc7e1975e650fd6ac729fa0d7e93",
        chain=Chain.ETH,
        sender="0xd463495a6FEaC9921FD0C3a595B81E7B2C02B57d",
        signature="0x9be4eab3f551935311bba7443044c1837f2f471223f44f83e1e9bcb1b35517300f1058b356b5664cf4baa150699384981c71b20c9b375904ae8dafecf3e0640d1b",
        type=MessageType.forget,
        time=timestamp_to_datetime(1722422471.7349474),
        item_type=ItemType.inline,
        item_content='{"address":"0xd463495a6FEaC9921FD0C3a595B81E7B2C02B57d","time":1722422471.7349474,"hashes":["356ea9d231e036c442cc6d330475101c0c39c83bef7e945d0b9cfa4156715519"],"aggregates":[],"reason":"test"}',
        channel=None,
        retries=0,
        next_attempt=dt.datetime(2023, 1, 3),
        check_message=True,
        fetched=True,
        reception_time=dt.datetime(2022, 1, 3),
    )

    pending_forget_message_older = PendingMessageDb(
        item_hash="6e16647f516de5b6562ff45e0ce31452549149ae36775753d324d0e7d821daf0",
        chain=Chain.ETH,
        sender="0xd463495a6FEaC9921FD0C3a595B81E7B2C02B57d",
        signature="0x4bbf9429f2171cfb11357ed4ad4c10541bc5151ee4ee3d039f400b7f767a2a772e6111531dfb78c9bda9d06492a5759308c29b0f285bd567d3d58d923ed3ffc51c",
        type=MessageType.forget,
        time=timestamp_to_datetime(1722422487.5738375),
        item_type=ItemType.inline,
        item_content='{"address":"0xd463495a6FEaC9921FD0C3a595B81E7B2C02B57d","time":1722422487.5738375,"hashes":["8eeb2af0e5503f1d2140dcd7deda4e1bb7fcc2899ce4d8138a5cacb29ea5de31"],"aggregates":[],"reason":"test"}',
        channel=None,
        retries=0,
        next_attempt=dt.datetime(2023, 1, 4),
        check_message=True,
        fetched=True,
        reception_time=dt.datetime(2022, 1, 4),
    )


    storage_engine = message_processor.message_handler.storage_service.storage_engine
    await storage_engine.write(
        filename=file_hash,
        content=b"Test",
    )

    with session_factory() as session:
        # Process both store messages
        await process_pending_messages(
            message_processor=message_processor,
            pending_messages=[pending_message1, pending_message2],
            session=session,
        )

        # Ensure there are two pins
        nb_references = count_file_pins(session=session, file_hash=file_hash)
        assert nb_references == 2

        # Forget the newer pin first
        await process_pending_messages(
            message_processor=message_processor,
            pending_messages=[pending_forget_message_newer],
            session=session,
        )

        # Check that only one pin remains (from pending_message1)
        file = get_file(session=session, file_hash=file_hash)
        assert file
        assert file.pins
        assert len(file.pins) == 1
        assert file.pins[0].item_hash == "8eeb2af0e5503f1d2140dcd7deda4e1bb7fcc2899ce4d8138a5cacb29ea5de31"

        # Now forget the older pin
        await process_pending_messages(
            message_processor=message_processor,
            pending_messages=[pending_forget_message_older],
            session=session,
        )

@pytest.mark.asyncio
async def test_fully_forget_older_first(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    mock_config: Config,
):
    file_hash = "7acb40cd206a0be1eff8c5ad59dff7cfaca860be75b0d7de8646bff458b5d3ed"

    pending_message1 = PendingMessageDb(
        item_hash="8eeb2af0e5503f1d2140dcd7deda4e1bb7fcc2899ce4d8138a5cacb29ea5de31",
        chain=Chain.ETH,
        sender="0xd463495a6FEaC9921FD0C3a595B81E7B2C02B57d",
        signature="0x80a55df25183a849e40d797c5e3059163593fe1910451ee7c8975cfcadeeb6763bb2537e5aead138f1b2a5216d6d74fe13b2f80731797dcb08256ea7c6c54f891c",
        type=MessageType.store,
        time=timestamp_to_datetime(1722420694.323802),
        item_type=ItemType.inline,
        item_content='{"address":"0xd463495a6FEaC9921FD0C3a595B81E7B2C02B57d","time":1722420694.323802,"item_type":"storage","item_hash":"7acb40cd206a0be1eff8c5ad59dff7cfaca860be75b0d7de8646bff458b5d3ed"}',
        channel=None,
        retries=0,
        next_attempt=dt.datetime(2023, 1, 1),
        check_message=True,
        fetched=True,
        reception_time=dt.datetime(2022, 1, 1),
    )

    pending_message2 = PendingMessageDb(
        item_hash="356ea9d231e036c442cc6d330475101c0c39c83bef7e945d0b9cfa4156715519",
        chain=Chain.ETH,
        sender="0xd463495a6FEaC9921FD0C3a595B81E7B2C02B57d",
        signature="0xa7b0318668d30af3bbb3348f2d7750e5d58ea3750294d2f6facb5f40f47956787f5926ce94c61e9e08a5e6967291c5783295a55be43700d93a1ce398c40c40731b",
        type=MessageType.store,
        time=timestamp_to_datetime(1722420696.4441047),
        item_type=ItemType.inline,
        item_content='{"address":"0xd463495a6FEaC9921FD0C3a595B81E7B2C02B57d","time":1722420696.4441047,"item_type":"storage","item_hash":"7acb40cd206a0be1eff8c5ad59dff7cfaca860be75b0d7de8646bff458b5d3ed"}',
        channel=None,
        retries=0,
        next_attempt=dt.datetime(2023, 1, 2),
        check_message=True,
        fetched=True,
        reception_time=dt.datetime(2022, 1, 2),
    )

    pending_forget_message_older = PendingMessageDb(
        item_hash="6e16647f516de5b6562ff45e0ce31452549149ae36775753d324d0e7d821daf0",
        chain=Chain.ETH,
        sender="0xd463495a6FEaC9921FD0C3a595B81E7B2C02B57d",
        signature="0x4bbf9429f2171cfb11357ed4ad4c10541bc5151ee4ee3d039f400b7f767a2a772e6111531dfb78c9bda9d06492a5759308c29b0f285bd567d3d58d923ed3ffc51c",
        type=MessageType.forget,
        time=timestamp_to_datetime(1722422487.5738375),
        item_type=ItemType.inline,
        item_content='{"address":"0xd463495a6FEaC9921FD0C3a595B81E7B2C02B57d","time":1722422487.5738375,"hashes":["8eeb2af0e5503f1d2140dcd7deda4e1bb7fcc2899ce4d8138a5cacb29ea5de31"],"aggregates":[],"reason":"test"}',
        channel=None,
        retries=0,
        next_attempt=dt.datetime(2023, 1, 4),
        check_message=True,
        fetched=True,
        reception_time=dt.datetime(2022, 1, 4),
    )


    pending_forget_message_newer = PendingMessageDb(
        item_hash="9505db13e325effbf45d53271de868af1103bc7e1975e650fd6ac729fa0d7e93",
        chain=Chain.ETH,
        sender="0xd463495a6FEaC9921FD0C3a595B81E7B2C02B57d",
        signature="0x9be4eab3f551935311bba7443044c1837f2f471223f44f83e1e9bcb1b35517300f1058b356b5664cf4baa150699384981c71b20c9b375904ae8dafecf3e0640d1b",
        type=MessageType.forget,
        time=timestamp_to_datetime(1722422471.7349474),
        item_type=ItemType.inline,
        item_content='{"address":"0xd463495a6FEaC9921FD0C3a595B81E7B2C02B57d","time":1722422471.7349474,"hashes":["356ea9d231e036c442cc6d330475101c0c39c83bef7e945d0b9cfa4156715519"],"aggregates":[],"reason":"test"}',
        channel=None,
        retries=0,
        next_attempt=dt.datetime(2023, 1, 3),
        check_message=True,
        fetched=True,
        reception_time=dt.datetime(2022, 1, 3),
    )

    storage_engine = message_processor.message_handler.storage_service.storage_engine
    await storage_engine.write(
        filename=file_hash,
        content=b"Test",
    )

    with session_factory() as session:
        # Process both store messages
        await process_pending_messages(
            message_processor=message_processor,
            pending_messages=[pending_message1, pending_message2],
            session=session,
        )

        # Ensure there are two pins
        nb_references = count_file_pins(session=session, file_hash=file_hash)
        assert nb_references == 2

        # Forget the older pin first
        await process_pending_messages(
            message_processor=message_processor,
            pending_messages=[pending_forget_message_older],
            session=session,
        )

        # Check that only one pin remains (from pending_message2)
        file = get_file(session=session, file_hash=file_hash)
        assert file
        assert file.pins
        assert len(file.pins) == 1
        assert file.pins[0].item_hash == "356ea9d231e036c442cc6d330475101c0c39c83bef7e945d0b9cfa4156715519"

        # Now forget the newer pin
        await process_pending_messages(
            message_processor=message_processor,
            pending_messages=[pending_forget_message_newer],
            session=session,
        )
