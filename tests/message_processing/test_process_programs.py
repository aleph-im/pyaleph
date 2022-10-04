import itertools
import json

import pytest
from aleph_message.models import ItemType, Chain, MessageType
from aleph_message.models.program import MachineType, VolumePersistence
from configmanager import Config
from more_itertools import one
from sqlalchemy import select

from aleph.db.models import (
    PendingMessageDb,
    MessageStatusDb,
    ProgramDb,
    ImmutableVolumeDb,
    EphemeralVolumeDb,
    PersistentVolumeDb,
)
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import MessageStatus


@pytest.fixture
def fixture_program_message(session_factory: DbSessionFactory) -> PendingMessageDb:
    pending_message = PendingMessageDb(
        item_hash="734a1287a2b7b5be060312ff5b05ad1bcf838950492e3428f2ac6437a1acad26",
        type=MessageType.program,
        chain=Chain.ETH,
        sender="0x7083b90eBA420832A03C6ac7e6328d37c72e0260",
        signature="0x5c5c757f35403e9b6d6b1c5dc0be349284d76ded4cfa9edc6ad8522212f4235448bc80432ea7f8c5f4da315d6ce6902072d4eb7bbbebd969841f08a3df61c2971c",
        item_type=ItemType.inline,
        item_content='{"address":"0x7083b90eBA420832A03C6ac7e6328d37c72e0260","time":1655123939.12433,"type":"vm-function","allow_amend":false,"code":{"encoding":"squashfs","entrypoint":"python run.py","ref":"53ee77caeb7d6e0e982abf010b3d6ea2dbc1225e157e09283e3a9d7da757e193","use_latest":true},"variables":{"LD_LIBRARY_PATH":"/opt/extra_lib","DB_FOLDER":"/data","RPC_ENDPOINT":"https://rpc.tzkt.io/ithacanet","TRUSTED_RPC_ENDPOINT":"https://rpc.tzkt.io/ithacanet","WELL_CONTRACT":"KT1ReVgfaUqHzWWiNRfPXQxf7TaBLVbxrztw","PORT":"8080","CONCURRENT_JOB":"5","BATCH_SIZE":"10","UNTIL_BLOCK":"201396","PUBSUB":"{\\"namespace\\": \\"tznms\\",\\"uuid\\": \\"tz_uid_1\\",\\"hook_url\\": \\"_domain_or_ip_addess\\",\\"pubsub_server\\": \\"domain_or_ip_address\\",\\"secret_shared_key\\": \\"112secret_key\\",\\"channel\\": \\"storage\\",\\"running_mode\\": \\"readonly\\"}"},"on":{"http":true},"environment":{"reproducible":false,"internet":true,"aleph_api":true,"shared_cache":false},"resources":{"vcpus":4,"memory":4000,"seconds":300},"runtime":{"ref":"bd79839bf96e595a06da5ac0b6ba51dea6f7e2591bb913deccded04d831d29f4","use_latest":true,"comment":"Aleph Alpine Linux with Python 3.8"},"volumes":[{"comment":"Extra lib","mount":"/opt/extra_lib","ref":"d7cecdeccc916280f8bcbf0c0e82c3638332da69ece2cbc806f9103a0f8befea","use_latest":true},{"comment":"Python Virtual Environment","mount":"/opt/packages","ref":"1000ebe0b61e41d5e23c10f6eb140e837188158598049829f2820f830139fc7d","use_latest":true},{"comment":"Data storage","mount":"/data","persistence":"host","name":"data","size_mib":128}]}',
        time=timestamp_to_datetime(1671637391),
        channel=None,
        reception_time=timestamp_to_datetime(1671637391),
        fetched=True,
        check_message=True,
        retries=0,
    )
    with session_factory() as session:
        session.add(pending_message)
        session.add(
            MessageStatusDb(
                item_hash=pending_message.item_hash,
                status=MessageStatus.PENDING,
                reception_time=pending_message.reception_time,
            )
        )
        session.commit()

    return pending_message


@pytest.fixture
def fixture_program_message_with_subscriptions(
    session_factory: DbSessionFactory,
) -> PendingMessageDb:
    pending_message = PendingMessageDb(
        item_hash="cad11970efe9b7478300fd04d7cc91c646ca0a792b9cc718650f86e1ccfac73e",
        type=MessageType.program,
        chain=Chain.ETH,
        sender="0xb5F010860b0964090d5414406273E6b3A8726E96",
        signature="0x93a4bff97ceb935091ac1daa57b4c0470256b945bde7ead5bd04e2a7139fe74343941e4a84ff563fd2c22da9599a0250ddda3f3217931d64f25de79b80bd2da11c",
        item_type=ItemType.inline,
        time=timestamp_to_datetime(1671637391),
        item_content='{"address":"0xb5F010860b0964090d5414406273E6b3A8726E96","time":1632489197.833036,"type":"vm-function","allow_amend":false,"code":{"encoding":"zip","entrypoint":"main:app","ref":"200af5241b583796441b249889500d8d9ee98cac5cbcc41076a4584c355a9ca5","use_latest":true},"on":{"http":true,"message":[{"sender":"0xE221373557Cc8e6094dB6cC3E8EFeb90003dE9ea","channel":"TEST"}]},"environment":{"reproducible":false,"internet":true,"aleph_api":true,"shared_cache":false},"resources":{"vcpus":1,"memory":128,"seconds":30},"runtime":{"ref":"c6dd36dbc94620159ffacde84cba102ede6cef7381e2e360c0c3b04423ba3eaa","use_latest":true,"comment":"Aleph Alpine Linux with Python 3.8"},"volumes":[]}',
        channel=None,
        reception_time=timestamp_to_datetime(1671637391),
        fetched=True,
        check_message=True,
        retries=0,
    )
    with session_factory() as session:
        session.add(pending_message)
        session.add(
            MessageStatusDb(
                item_hash=pending_message.item_hash,
                status=MessageStatus.PENDING,
                reception_time=pending_message.reception_time,
            )
        )
        session.commit()

    return pending_message


@pytest.mark.asyncio
async def test_process_program(
    session_factory: DbSessionFactory,
    mock_config: Config,
    message_processor: PendingMessageProcessor,
    fixture_program_message: PendingMessageDb,
):

    pipeline = message_processor.make_pipeline()
    # Exhaust the iterator
    _ = [message async for message in pipeline]

    assert fixture_program_message.item_content
    content_dict = json.loads(fixture_program_message.item_content)

    with session_factory() as session:
        program: ProgramDb = session.execute(select(ProgramDb)).scalar_one()

        assert program.owner == fixture_program_message.sender
        assert program.type == MachineType.vm_function
        assert not program.allow_amend
        assert program.replaces is None
        assert program.http_trigger

        assert program.resources_vcpus == content_dict["resources"]["vcpus"]
        assert program.resources_memory == content_dict["resources"]["memory"]
        assert program.resources_seconds == content_dict["resources"]["seconds"]

        assert program.environment_internet
        assert program.environment_aleph_api
        assert not program.environment_reproducible
        assert not program.environment_shared_cache

        assert program.variables
        assert len(program.variables) == 10

        runtime = program.runtime
        assert runtime.ref == content_dict["runtime"]["ref"]
        assert runtime.use_latest == content_dict["runtime"]["use_latest"]
        assert runtime.comment == content_dict["runtime"]["comment"]

        code_volume = program.code_volume
        assert code_volume.ref == content_dict["code"]["ref"]
        assert code_volume.encoding == content_dict["code"]["encoding"]
        assert code_volume.entrypoint == content_dict["code"]["entrypoint"]
        assert code_volume.use_latest == content_dict["code"]["use_latest"]

        assert len(program.volumes) == 3

        volumes_by_type = {
            type: list(volumes_iter)
            for type, volumes_iter in itertools.groupby(
                sorted(program.volumes, key=lambda v: str(v.__class__)),
                key=lambda v: v.__class__,
            )
        }
        assert EphemeralVolumeDb not in volumes_by_type
        assert len(volumes_by_type[PersistentVolumeDb]) == 1
        assert len(volumes_by_type[ImmutableVolumeDb]) == 2

        persistent_volume: PersistentVolumeDb = one(volumes_by_type[PersistentVolumeDb])  # type: ignore[assignment]
        assert persistent_volume.name == "data"
        assert persistent_volume.mount == "/data"
        assert persistent_volume.size_mib == 128
        assert persistent_volume.persistence == VolumePersistence.host


@pytest.mark.asyncio
async def test_program_with_subscriptions(
    session_factory: DbSessionFactory,
    mock_config: Config,
    message_processor: PendingMessageProcessor,
    fixture_program_message_with_subscriptions: PendingMessageDb,
):
    program_message = fixture_program_message_with_subscriptions

    pipeline = message_processor.make_pipeline()
    # Exhaust the iterator
    _ = [message async for message in pipeline]

    assert program_message.item_content
    content_dict = json.loads(program_message.item_content)

    with session_factory() as session:
        program: ProgramDb = session.execute(select(ProgramDb)).scalar_one()
        message_triggers = program.message_triggers
        assert message_triggers
        assert len(message_triggers) == 1
        message_trigger = message_triggers[0]

        assert message_trigger["channel"] == "TEST"
        assert message_trigger["sender"] == "0xE221373557Cc8e6094dB6cC3E8EFeb90003dE9ea"
