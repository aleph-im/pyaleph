from typing import List, Set

from aleph_message.models import ProgramContent
from aleph_message.models.program import (
    AbstractVolume,
    ImmutableVolume,
    EphemeralVolume,
    PersistentVolume,
)

from aleph.db.accessors.programs import delete_program
from aleph.db.models import (
    MessageDb,
    CodeVolumeDb,
    DataVolumeDb,
    ExportVolumeDb,
    ProgramDb,
    MachineVolumeBaseDb,
    ImmutableVolumeDb,
    EphemeralVolumeDb,
    PersistentVolumeDb,
    RuntimeDb,
)
from aleph.handlers.content.content_handler import ContentHandler
from aleph.types.db_session import DbSession
from aleph.types.message_status import InternalError


def map_volume(volume: AbstractVolume) -> MachineVolumeBaseDb:
    comment = volume.comment
    mount = volume.mount

    if isinstance(volume, ImmutableVolume):
        return ImmutableVolumeDb(
            comment=comment, mount=mount, ref=volume.ref, use_latest=volume.use_latest
        )
    elif isinstance(volume, EphemeralVolume):
        return EphemeralVolumeDb(comment=comment, mount=mount, size_mib=volume.size_mib)
    elif isinstance(volume, PersistentVolume):
        return PersistentVolumeDb(
            comment=comment,
            mount=mount,
            persistence=volume.persistence,
            name=volume.name,
            size_mib=volume.size_mib,
        )
    else:
        raise InternalError(f"Unsupported volume type: {volume.__class__.__name__}")


def program_message_to_db(message: MessageDb):
    content = message.parsed_content
    assert isinstance(content, ProgramContent)

    code_volume = CodeVolumeDb(
        encoding=content.code.encoding,
        entrypoint=content.code.entrypoint,
        ref=content.code.ref,
        use_latest=content.code.use_latest,
    )

    runtime = RuntimeDb(
        ref=content.runtime.ref,
        use_latest=content.runtime.use_latest,
        comment=content.runtime.comment,
    )

    if content.data:
        data_volume = DataVolumeDb(
            encoding=content.data.encoding,
            mount=content.data.mount,
            ref=content.data.ref,
            use_latest=content.data.use_latest,
        )
    else:
        data_volume = None

    if content.export:
        export_volume = ExportVolumeDb(encoding=content.export.encoding)
    else:
        export_volume = None

    volumes = [map_volume(volume) for volume in content.volumes]

    cpu_architecture = None
    cpu_vendor = None
    node_owner = None
    node_address_regex = None

    if content.requirements:
        if cpu := content.requirements.cpu:
            cpu_architecture = cpu.architecture
            cpu_vendor = cpu.vendor

        if node := content.requirements.node:
            node_owner = node.owner
            node_address_regex = node.address_regex

    if content.on.message:
        message_triggers = [subscription.dict() for subscription in content.on.message]
    else:
        message_triggers = None

    program = ProgramDb(
        owner=content.address,
        item_hash=message.item_hash,
        type=content.type,
        allow_amend=content.allow_amend,
        metadata_=content.metadata,
        variables=content.variables,
        http_trigger=content.on.http,
        message_triggers=message_triggers,
        persistent=bool(content.on.persistent),
        environment_reproducible=content.environment.reproducible,
        environment_internet=content.environment.internet,
        environment_aleph_api=content.environment.aleph_api,
        environment_shared_cache=content.environment.shared_cache,
        resources_vcpus=content.resources.vcpus,
        resources_memory=content.resources.memory,
        resources_seconds=content.resources.seconds,
        cpu_architecture=cpu_architecture,
        cpu_vendor=cpu_vendor,
        node_owner=node_owner,
        node_address_regex=node_address_regex,
        code_volume=code_volume,
        runtime=runtime,
        data_volume=data_volume,
        export_volume=export_volume,
        volumes=volumes,
    )
    return program


class ProgramMessageHandler(ContentHandler):
    @staticmethod
    async def process_program_message(session: DbSession, message: MessageDb):
        program = program_message_to_db(message)
        session.add(program)

    async def process(self, session: DbSession, messages: List[MessageDb]) -> None:
        for message in messages:
            await self.process_program_message(session=session, message=message)

    async def forget_message(self, session: DbSession, message: MessageDb) -> Set[str]:
        delete_program(session=session, item_hash=message.item_hash)
        return set()
