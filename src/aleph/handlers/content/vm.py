import logging
from typing import List, Set, overload

from aleph_message.models import ProgramContent, ExecutableContent, InstanceContent
from aleph_message.models.execution.volume import (
    AbstractVolume,
    ImmutableVolume,
    EphemeralVolume,
    PersistentVolume,
)

from aleph.db.accessors.files import find_file_tags, find_file_pins
from aleph.db.accessors.vms import (
    delete_vm,
    get_program,
    upsert_vm_version,
    delete_vm_updates,
    refresh_vm_version,
    is_vm_amend_allowed,
)
from aleph.db.models import (
    MessageDb,
    CodeVolumeDb,
    DataVolumeDb,
    ExportVolumeDb,
    MachineVolumeBaseDb,
    ImmutableVolumeDb,
    EphemeralVolumeDb,
    PersistentVolumeDb,
    RuntimeDb,
    VmInstanceDb,
    ProgramDb,
    RootfsVolumeDb,
    VmBaseDb,
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
from aleph.types.vms import VmVersion

LOGGER = logging.getLogger(__name__)


def _get_vm_content(message: MessageDb) -> ExecutableContent:
    content = message.parsed_content
    if not isinstance(content, ExecutableContent):
        raise InvalidMessageFormat(
            f"Unexpected content type for program message: {message.item_hash}"
        )
    return content


@overload
def _map_content_to_db_model(item_hash: str, content: InstanceContent) -> VmInstanceDb:
    ...


# For some reason, mypy is not happy with the overload resolution here.
# This seems linked to multiple inheritance of Pydantic base models, a deeper investigation
# is required.
@overload
def _map_content_to_db_model(item_hash: str, content: ProgramContent) -> ProgramDb:  # type: ignore[misc]
    ...


def _map_content_to_db_model(item_hash, content):
    db_cls = ProgramDb if isinstance(content, ProgramContent) else VmInstanceDb

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

    return db_cls(
        owner=content.address,
        item_hash=item_hash,
        allow_amend=content.allow_amend,
        metadata_=content.metadata,
        variables=content.variables,
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
        volumes=volumes,
        created=timestamp_to_datetime(content.time),
    )


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


def vm_message_to_db(message: MessageDb) -> VmBaseDb:
    content = _get_vm_content(message)
    vm = _map_content_to_db_model(message.item_hash, content)

    if isinstance(vm, ProgramDb):
        vm.program_type = content.type
        vm.persistent = bool(content.on.persistent)
        vm.http_trigger = content.on.http

        if content.on.message:
            vm.message_triggers = [
                subscription.dict() for subscription in content.on.message
            ]

        vm.code_volume = CodeVolumeDb(
            encoding=content.code.encoding,
            entrypoint=content.code.entrypoint,
            ref=content.code.ref,
            use_latest=content.code.use_latest,
        )

        vm.runtime = RuntimeDb(
            ref=content.runtime.ref,
            use_latest=content.runtime.use_latest,
            comment=content.runtime.comment,
        )

        if content.data:
            vm.data_volume = DataVolumeDb(
                encoding=content.data.encoding,
                mount=content.data.mount,
                ref=content.data.ref,
                use_latest=content.data.use_latest,
            )

        if content.export:
            vm.export_volume = ExportVolumeDb(encoding=content.export.encoding)

    elif isinstance(content, InstanceContent):
        vm.rootfs = RootfsVolumeDb(
            parent=content.rootfs.parent,
            size_mib=content.rootfs.size_mib,
            persistence=content.rootfs.persistence,
            comment=content.rootfs.comment,
        )
        vm.cloud_config = content.cloud_config

    else:
        raise TypeError(f"Unexpected VM message content type: {type(content)}")

    return vm


def find_missing_volumes(
    session: DbSession, content: ExecutableContent
) -> Set[FileTag]:
    tags_to_check = set()
    pins_to_check = set()

    def add_ref_to_check(_volume):
        if _volume.use_latest:
            tags_to_check.add(_volume.ref)
        else:
            pins_to_check.add(_volume.ref)

    if isinstance(content, ProgramContent):
        add_ref_to_check(content.code)
        add_ref_to_check(content.runtime)
        if content.data:
            add_ref_to_check(content.data)

    elif isinstance(content, InstanceContent):
        if rootfs_parent := content.rootfs.parent:
            tags_to_check.add(FileTag(rootfs_parent))

    for volume in content.volumes:
        if isinstance(volume, ImmutableVolume):
            add_ref_to_check(volume)

        if isinstance(volume, PersistentVolume):
            # Assume `use_latest` for persistent volume parents
            if parent := volume.parent:
                tags_to_check.add(FileTag(parent))

    # For each volume, if use_latest is set check the tags and otherwise check
    # the file pins.

    file_tags_db = set(find_file_tags(session=session, tags=tags_to_check))
    file_pins_db = set(find_file_pins(session=session, item_hashes=pins_to_check))

    return (pins_to_check - file_pins_db) | (tags_to_check - file_tags_db)


class VmMessageHandler(ContentHandler):
    """
    Handles both PROGRAM and INSTANCE messages.

    The implementation for both is very similar, making it simpler to implement both
    in the same handler.

    """

    async def check_dependencies(self, session: DbSession, message: MessageDb) -> None:
        content = _get_vm_content(message)

        missing_volumes = find_missing_volumes(session=session, content=content)
        if missing_volumes:
            raise ProgramVolumeNotFound([volume for volume in missing_volumes])

        if (ref := content.replaces) is not None:
            original_program = get_program(session=session, item_hash=ref)
            if original_program is None:
                raise ProgramRefNotFound(ref)

            if original_program.replaces is not None:
                raise ProgramCannotUpdateUpdate()

            is_amend_allowed = is_vm_amend_allowed(session=session, vm_hash=ref)
            if is_amend_allowed is None:
                raise InternalError(f"Could not find current version of program {ref}")

            if not is_amend_allowed:
                raise ProgramUpdateNotAllowed()

    @staticmethod
    async def process_vm_message(session: DbSession, message: MessageDb):
        vm = vm_message_to_db(message)
        session.add(vm)

        program_ref = vm.replaces or vm.item_hash
        upsert_vm_version(
            session=session,
            vm_hash=vm.item_hash,
            owner=vm.owner,
            current_version=VmVersion(program_ref),
            last_updated=vm.created,
        )

    async def process(self, session: DbSession, messages: List[MessageDb]) -> None:
        for message in messages:
            await self.process_vm_message(session=session, message=message)

    async def forget_message(self, session: DbSession, message: MessageDb) -> Set[str]:
        content = _get_vm_content(message)

        LOGGER.debug("Deleting program %s...", message.item_hash)

        delete_vm(session=session, vm_hash=message.item_hash)

        if content.replaces:
            update_hashes = set()
        else:
            update_hashes = set(
                delete_vm_updates(session=session, vm_hash=message.item_hash)
            )

        refresh_vm_version(session=session, vm_hash=message.item_hash)

        return update_hashes
