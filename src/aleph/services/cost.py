from decimal import Decimal
from typing import Optional, Union

from aleph_message.models import ExecutableContent, InstanceContent, ProgramContent
from aleph_message.models.execution.volume import ImmutableVolume

from aleph.db.accessors.files import get_file_tag, get_message_file_pin
from aleph.db.models import StoredFileDb, FileTagDb, MessageFilePinDb
from aleph.toolkit.constants import GiB, MiB
from aleph.types.db_session import DbSession
from aleph.types.files import FileTag


def _get_file_from_ref(
    session: DbSession, ref: str, use_latest: bool
) -> Optional[StoredFileDb]:

    tag_or_pin: Optional[Union[MessageFilePinDb, FileTagDb]]

    if use_latest:
        tag_or_pin = get_file_tag(session=session, tag=FileTag(ref))
    else:
        tag_or_pin = get_message_file_pin(session=session, item_hash=ref)

    if tag_or_pin:
        return tag_or_pin.file

    return None


def get_volume_size(session: DbSession, content: ExecutableContent) -> Decimal:
    ref_volumes = []
    sized_volumes = []

    if isinstance(content, InstanceContent):
        sized_volumes.append(content.rootfs)
    elif isinstance(content, ProgramContent):
        ref_volumes += [content.code, content.data, content.runtime]

    for volume in content.volumes:
        if isinstance(volume, ImmutableVolume):
            ref_volumes.append(volume)
        else:
            sized_volumes.append(volume)

    total_volume_size: Decimal = Decimal(0)

    for volume in ref_volumes:
        file = _get_file_from_ref(
            session=session, ref=volume.ref, use_latest=volume.use_latest
        )
        if file is None:
            raise RuntimeError(f"Could not find entry in file tags for {volume.ref}.")
        total_volume_size += Decimal(file.size)

    for volume in sized_volumes:
        total_volume_size += Decimal(volume.size_mib * MiB)

    return total_volume_size


def get_additional_storage_price(
    content: ExecutableContent, session: DbSession
) -> Decimal:
    is_microvm = isinstance(content, ProgramContent) and not content.on.persistent
    nb_compute_units = content.resources.vcpus
    free_storage_per_compute_unit = 2 * GiB if is_microvm else 20 * GiB

    total_volume_size = get_volume_size(session, content)
    additional_storage = max(
        total_volume_size - (free_storage_per_compute_unit * nb_compute_units), 0
    )
    price = (additional_storage * 20) / MiB
    return Decimal(price)


def compute_cost(session: DbSession, content: ExecutableContent) -> Decimal:
    is_microvm = isinstance(content, ProgramContent) and not content.on.persistent
    compute_unit_cost: Decimal = Decimal("200.0") if is_microvm else Decimal("2000.0")

    return (compute_unit_cost * content.resources.vcpus) + get_additional_storage_price(
        content, session
    )
