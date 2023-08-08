from aleph_message.models import InstanceContent, ExecutableContent
from decimal import Decimal

from aleph.db.accessors.files import get_file_tag
from aleph.types.db_session import DbSession


def get_volume_size(content: ExecutableContent, session: DbSession) -> Decimal:
    total_volume_size: Decimal = Decimal(0)
    for volume in content.volumes:
        if hasattr(volume, "ref") and volume.ref:
            file_tag= get_file_tag(session=session, tag=volume.ref)
            if file_tag and file_tag.file:
                total_volume_size += Decimal(file_tag.file.size)
        else:
            if hasattr(volume, "size_mib"):
                total_volume_size += Decimal(volume.size_mib * (1024 * 1024))
    if hasattr(content.rootfs, "size_mib"):
        total_volume_size += Decimal(content.rootfs.size_mib * (1024 * 1024))
    return total_volume_size


def get_additional_storage_price(content: ExecutableContent, session: DbSession) -> Decimal:
    size_plus = get_volume_size(content, session) / (1024 * 1024)
    additional_storage = (size_plus * 1024 * 1024) - (
            20_000_000_000 * content.resources.vcpus
    )
    price = (additional_storage * 20) / 1_000_000
    return Decimal(price)


def compute_cost(session: DbSession, content: ExecutableContent) -> Decimal:
    compute_unit_cost: Decimal = Decimal("2000.0")
    return (compute_unit_cost * content.resources.vcpus) + Decimal(
        get_additional_storage_price(content, session)
    )
