import math
from decimal import Decimal
from typing import Optional, Union, List

from aleph_message.models import ExecutableContent, InstanceContent, ProgramContent
from aleph_message.models.execution.volume import ImmutableVolume, EphemeralVolume, PersistentVolume
from aleph_message.models.execution.instance import RootfsVolume

from aleph.db.accessors.files import get_file_tag, get_message_file_pin
from aleph.db.models import StoredFileDb, FileTagDb, MessageFilePinDb
from aleph.toolkit.constants import GiB, MiB
from aleph.types.db_session import DbSession
from aleph.types.files import FileTag


MINUTE = 60
HOUR = 60 * MINUTE

COMPUTE_UNIT_TOKEN_TO_HOLD_ON_DEMAND = Decimal("200")
COMPUTE_UNIT_TOKEN_TO_HOLD_PERSISTENT = Decimal("2000")
COMPUTE_UNIT_PRICE_PER_HOUR_ON_DEMAND = Decimal("0.011")
COMPUTE_UNIT_PRICE_PER_HOUR_PERSISTENT = Decimal("0.11")
STORAGE_INCLUDED_PER_COMPUTE_UNIT_ON_DEMAND = Decimal("2") * GiB
STORAGE_INCLUDED_PER_COMPUTE_UNIT_PERSISTENT = Decimal("20") * GiB

EXTRA_STORAGE_TOKEN_TO_HOLD = 1 / (Decimal('20') * MiB)  # Hold 1 token for 20 MiB
EXTRA_STORAGE_PRICE_PER_HOUR = Decimal("0.000000977")
EXTRA_STORAGE_PRICE_PER_SECOND = EXTRA_STORAGE_PRICE_PER_HOUR / Decimal(HOUR)


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


def get_volume_size(session: DbSession, content: ExecutableContent) -> int:
    ref_volumes = []
    sized_volumes: List[Union[EphemeralVolume, PersistentVolume, RootfsVolume]] = []

    if isinstance(content, InstanceContent):
        sized_volumes.append(content.rootfs)
    elif isinstance(content, ProgramContent):
        ref_volumes += [content.code, content.runtime]
        if content.data:
            ref_volumes.append(content.data)

    for volume in content.volumes:
        if isinstance(volume, ImmutableVolume):
            ref_volumes.append(volume)
        else:
            sized_volumes.append(volume)

    total_volume_size: int = 0

    for volume in ref_volumes:
        if hasattr(volume, "ref") and hasattr(volume, "use_latest"):
            file = _get_file_from_ref(
                session=session, ref=volume.ref, use_latest=volume.use_latest
            )
            if file is None:
                raise RuntimeError(f"Could not find entry in file tags for {volume.ref}.")
            total_volume_size += file.size
        else:
            raise RuntimeError(f"Could not find reference hash for {volume}.")

    for volume in sized_volumes:
        total_volume_size += volume.size_mib * MiB

    return total_volume_size


def get_additional_storage_price(
    content: ExecutableContent, session: DbSession
) -> Decimal:
    nb_compute_units = Decimal(content.resources.vcpus)

    is_on_demand = isinstance(content, ProgramContent) and not content.on.persistent
    included_storage_per_compute_unit = (
        STORAGE_INCLUDED_PER_COMPUTE_UNIT_ON_DEMAND
        if is_on_demand
        else STORAGE_INCLUDED_PER_COMPUTE_UNIT_PERSISTENT
    )

    total_volume_size = get_volume_size(session, content)
    additional_storage = max(
        Decimal(total_volume_size) - (included_storage_per_compute_unit * nb_compute_units), Decimal(0)
    )
    return Decimal(additional_storage) * EXTRA_STORAGE_TOKEN_TO_HOLD


def _get_nb_compute_units(content: ExecutableContent) -> int:
    cpu = content.resources.vcpus
    memory = math.ceil(content.resources.memory / 2048)
    nb_compute_units = cpu if cpu >= memory else memory
    return nb_compute_units


def _get_compute_unit_multiplier(content: ExecutableContent) -> int:
    compute_unit_multiplier = 1
    if (
        isinstance(content, ProgramContent)
        and not content.on.persistent
        and content.environment.internet
    ):
        compute_unit_multiplier += 1
    return compute_unit_multiplier


def compute_cost(session: DbSession, content: ExecutableContent) -> Decimal:
    is_on_demand = isinstance(content, ProgramContent) and not content.on.persistent
    compute_unit_cost = (
        COMPUTE_UNIT_TOKEN_TO_HOLD_ON_DEMAND
        if is_on_demand
        else COMPUTE_UNIT_TOKEN_TO_HOLD_PERSISTENT
    )

    compute_units_required = _get_nb_compute_units(content)
    compute_unit_multiplier = _get_compute_unit_multiplier(content)

    compute_unit_price = (
        Decimal(compute_units_required) * compute_unit_multiplier * compute_unit_cost
    )
    price = compute_unit_price + get_additional_storage_price(content, session)
    return Decimal(price)


def compute_flow_cost(session: DbSession, content: ExecutableContent) -> Decimal:
    # TODO: Use PAYMENT_PRICING_AGGREGATE when possible
    is_on_demand = isinstance(content, ProgramContent) and not content.on.persistent
    compute_unit_cost_hour = (
        COMPUTE_UNIT_PRICE_PER_HOUR_ON_DEMAND
        if is_on_demand
        else COMPUTE_UNIT_PRICE_PER_HOUR_PERSISTENT
    )

    compute_unit_cost_second = compute_unit_cost_hour / HOUR

    compute_units_required = _get_nb_compute_units(content)
    compute_unit_multiplier = _get_compute_unit_multiplier(content)

    compute_unit_price = (
        Decimal(compute_units_required)
        * Decimal(compute_unit_multiplier)
        * Decimal(compute_unit_cost_second)
    )

    additional_storage_flow_price = get_additional_storage_flow_price(content, session)
    price = compute_unit_price + additional_storage_flow_price
    return Decimal(price)


def get_additional_storage_flow_price(
    content: ExecutableContent, session: DbSession
) -> Decimal:
    # TODO: Use PAYMENT_PRICING_AGGREGATE when possible
    nb_compute_units = _get_nb_compute_units(content)

    is_on_demand = isinstance(content, ProgramContent) and not content.on.persistent
    included_storage_per_compute_unit = (
        STORAGE_INCLUDED_PER_COMPUTE_UNIT_ON_DEMAND
        if is_on_demand
        else STORAGE_INCLUDED_PER_COMPUTE_UNIT_PERSISTENT
    )

    total_volume_size = get_volume_size(session, content)
    additional_storage = max(
        Decimal(total_volume_size)
        - (Decimal(included_storage_per_compute_unit) * Decimal(nb_compute_units)),
        Decimal(0),
    )
    price = (additional_storage / MiB) * EXTRA_STORAGE_PRICE_PER_SECOND
    return price
