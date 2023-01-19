import logging
from typing import List, Set

from aleph_message.models import ProgramContent
from aleph_message.models.program import (
    AbstractVolume,
    ImmutableVolume,
    EphemeralVolume,
    PersistentVolume,
)

from aleph.db.accessors.files import find_file_tags, find_file_pins
from aleph.db.accessors.programs import (
    delete_program,
    get_program,
    upsert_program_version,
    delete_program_updates,
    refresh_program_version,
    is_program_amend_allowed,
)
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
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSession
from aleph.types.files import FileTag
from aleph.types.message_status import (
    InternalError,
    InvalidMessageFormat,
    ProgramRefNotFound,
    ProgramVolumeNotFound,
    ProgramUpdateNotAllowed,
    ProgramCannotUpdateUpdate,
)
from aleph.types.vms import ProgramVersion


LOGGER = logging.getLogger(__name__)


def _get_program_content(message: MessageDb) -> ProgramContent:
    content = message.parsed_content
    if not isinstance(content, ProgramContent):
        raise InvalidMessageFormat(
            f"Unexpected content type for program message: {message.item_hash}"
        )
    return content


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
    content = _get_program_content(message)

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
        created=timestamp_to_datetime(content.time),
    )
    return program


def find_missing_volumes(session: DbSession, content: ProgramContent) -> Set[FileTag]:
    tags_to_check = set()
    pins_to_check = set()

    def add_ref_to_check(_volume):
        if _volume.use_latest:
            tags_to_check.add(_volume.ref)
        else:
            pins_to_check.add(_volume.ref)

    add_ref_to_check(content.code)
    add_ref_to_check(content.runtime)
    if content.data:
        add_ref_to_check(content.data)

    for volume in content.volumes:
        if isinstance(volume, ImmutableVolume):
            add_ref_to_check(volume)

    # For each volume, if use_latest is set check the tags and otherwise check
    # the file pins.

    file_tags_db = set(find_file_tags(session=session, tags=tags_to_check))
    file_pins_db = set(find_file_pins(session=session, item_hashes=pins_to_check))

    return (pins_to_check - file_pins_db) | (tags_to_check - file_tags_db)


class ProgramMessageHandler(ContentHandler):
    async def check_dependencies(self, session: DbSession, message: MessageDb) -> None:
        content = _get_program_content(message)

        missing_volumes = find_missing_volumes(session=session, content=content)
        if missing_volumes:
            raise ProgramVolumeNotFound([volume for volume in missing_volumes])

        if (ref := content.replaces) is not None:
            original_program = get_program(session=session, item_hash=ref)
            if original_program is None:
                raise ProgramRefNotFound(ref)

            if original_program.replaces is not None:
                raise ProgramCannotUpdateUpdate()

            is_amend_allowed = is_program_amend_allowed(
                session=session, program_hash=ref
            )
            if is_amend_allowed is None:
                raise InternalError(f"Could not find current version of program {ref}")

            if not is_amend_allowed:
                raise ProgramUpdateNotAllowed()

    @staticmethod
    async def process_program_message(session: DbSession, message: MessageDb):
        program = program_message_to_db(message)
        session.add(program)

        program_ref = program.replaces or program.item_hash
        upsert_program_version(
            session=session,
            program_hash=program.item_hash,
            owner=program.owner,
            current_version=ProgramVersion(program_ref),
            last_updated=program.created,
        )

    async def process(self, session: DbSession, messages: List[MessageDb]) -> None:
        for message in messages:
            await self.process_program_message(session=session, message=message)

    async def forget_message(self, session: DbSession, message: MessageDb) -> Set[str]:
        content = _get_program_content(message)

        LOGGER.debug("Deleting program %s...", message.item_hash)

        delete_program(session=session, item_hash=message.item_hash)

        if content.replaces:
            update_hashes = set()
            refresh_program_version(session=session, program_hash=message.item_hash)
        else:
            update_hashes = set(
                delete_program_updates(session=session, program_hash=message.item_hash)
            )

        return update_hashes
