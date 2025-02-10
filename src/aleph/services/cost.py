import logging
import math
from decimal import Decimal
from functools import reduce
from typing import List, Optional, Tuple, TypeAlias, Union

from aleph_message.models import (
    ExecutableContent,
    InstanceContent,
    PaymentType,
    ProgramContent,
    StoreContent,
)
from aleph_message.models.execution.environment import (
    HostRequirements,
    InstanceEnvironment,
)
from aleph_message.models.execution.volume import ImmutableVolume

from aleph.db.accessors.aggregates import get_aggregate_by_key
from aleph.db.accessors.cost import get_message_costs
from aleph.db.accessors.files import get_file, get_file_tag, get_message_file_pin
from aleph.db.models import FileTagDb, MessageFilePinDb, StoredFileDb
from aleph.db.models.account_costs import AccountCostsDb
from aleph.db.models.aggregates import AggregateDb
from aleph.toolkit.constants import (
    HOUR,
    PRICE_AGGREGATE_KEY,
    PRICE_AGGREGATE_OWNER,
    SETTINGS_AGGREGATE_KEY,
    SETTINGS_AGGREGATE_OWNER,
    MiB,
)
from aleph.toolkit.costs import format_cost
from aleph.types.cost import (
    CostType,
    ProductComputeUnit,
    ProductPriceType,
    ProductPricing,
    RefVolume,
    SizedVolume,
)
from aleph.types.db_session import DbSession
from aleph.types.files import FileTag
from aleph.types.settings import Settings

CostComputableContent: TypeAlias = InstanceContent | ProgramContent | StoreContent

logger = logging.getLogger(__name__)


# TODO: Cache aggregate for 5 min
def _get_settings_aggregate(session: DbSession) -> AggregateDb:
    aggregate = get_aggregate_by_key(
        session=session, owner=SETTINGS_AGGREGATE_OWNER, key=SETTINGS_AGGREGATE_KEY
    )

    if not aggregate:
        raise Exception()

    return aggregate


def _get_settings(session: DbSession) -> Settings:
    aggregate = _get_settings_aggregate(session)

    return Settings.from_aggregate(aggregate)


def get_payment_type(content: CostComputableContent) -> PaymentType:
    return (
        PaymentType.superfluid
        if (
            hasattr(content, "payment")
            and content.payment
            and content.payment.is_stream
        )
        else PaymentType.hold
    )

CostComputableContent: TypeAlias = InstanceContent | ProgramContent | StoreContent


def get_payment_type(content: CostComputableContent) -> PaymentType:
    return (
        PaymentType.superfluid
        if (
            hasattr(content, "payment")
            and content.payment
            and content.payment.is_stream
        )
        else PaymentType.hold
    )

type CostComputableContent = ExecutableContent | StoreContent


def get_payment_type(content: CostComputableContent) -> PaymentType:
    return (
        PaymentType.superfluid
        if (
            hasattr(content, "payment")
            and content.payment
            and content.payment.is_stream
        )
        else PaymentType.hold
    )


def _is_on_demand(content: ExecutableContent) -> bool:
    return isinstance(content, ProgramContent) and not content.on.persistent


def _is_confidential_vm(
    content: ExecutableContent, is_on_demand: Optional[bool]
) -> bool:
    is_on_demand = is_on_demand or _is_on_demand(content=content)

    return (
        not is_on_demand
        and isinstance(getattr(content, "environment", None), InstanceEnvironment)
        and getattr(content.environment, "trusted_execution", False)
    )


def _is_gpu_vm(content: ExecutableContent) -> bool:

    return isinstance(
        getattr(content, "requirements", None), HostRequirements
    ) and getattr(content.requirements, "gpu", False)


def _get_product_instance_type(
    content: CostComputableContent, settings: Settings, price_aggregate: AggregateDb
) -> ProductPriceType:
    if _is_confidential_vm(content, False):
        return ProductPriceType.INSTANCE_CONFIDENTIAL

    if not _is_gpu_vm(content):
        return ProductPriceType.INSTANCE

    # TODO: Improve the way to compare tiers
    premium_gpu_price = ProductPricing.from_aggregate(
        ProductPriceType.INSTANCE_GPU_PREMIUM, price_aggregate
    )
    standard_gpu_price = ProductPricing.from_aggregate(
        ProductPriceType.INSTANCE_GPU_STANDARD, price_aggregate
    )

    # TODO: Allow to get more than one GPU
    selected_gpu = content.requirements.gpu[0]

    gpu_model = next(
        compatible_gpu.model
        for compatible_gpu in settings.compatible_gpus
        if compatible_gpu.device_id == selected_gpu.device_id
    )

    if gpu_model in [tier.model for tier in premium_gpu_price.tiers]:
        return ProductPriceType.INSTANCE_GPU_PREMIUM
    else:
        if gpu_model in [tier.model for tier in standard_gpu_price.tiers]:
            return ProductPriceType.INSTANCE_GPU_STANDARD
        else:
            raise ValueError(f"GPU model {gpu_model} not supported")


def _get_product_price_type(
    content: CostComputableContent, settings: Settings, price_aggregate: AggregateDb
) -> ProductPriceType:
    if isinstance(content, StoreContent):
        return ProductPriceType.STORAGE

    is_on_demand = _is_on_demand(content)

    return (
        ProductPriceType.PROGRAM
        if is_on_demand
        else _get_product_instance_type(content, settings, price_aggregate)
    )


# TODO: Cache aggregate for 5 min
def _get_price_aggregate(session: DbSession) -> AggregateDb:
    aggregate = get_aggregate_by_key(
        session=session, owner=PRICE_AGGREGATE_OWNER, key=PRICE_AGGREGATE_KEY
    )

    if not aggregate:
        raise Exception()

    return aggregate


def _get_product_price(
    session: DbSession, content: CostComputableContent, settings: Settings
) -> ProductPricing:
    price_aggregate = _get_price_aggregate(session)
    price_type = _get_product_price_type(content, settings, price_aggregate)

    return ProductPricing.from_aggregate(price_type, price_aggregate)


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


def _get_nb_compute_units(
    content: ExecutableContent, product_compute_unit: Optional[ProductComputeUnit]
) -> int:
    default_compute_unit = ProductComputeUnit(
        vcpus=1,
        memory_mib=2048,
        disk_mib=2048,
    )
    if not product_compute_unit:
        product_compute_unit = default_compute_unit

    cpu = content.resources.vcpus
    memory = math.ceil(content.resources.memory / product_compute_unit.memory_mib)
    nb_compute_units = cpu if cpu >= memory else memory
    return nb_compute_units


# TODO: Include this in the aggregate
def _get_compute_unit_multiplier(content: ExecutableContent) -> int:
    compute_unit_multiplier = 1
    if (
        isinstance(content, ProgramContent)
        and not content.on.persistent
        and content.environment.internet
    ):
        compute_unit_multiplier += 1
    return compute_unit_multiplier


def _get_volumes_costs(
    session: DbSession,
    volumes: List[RefVolume | SizedVolume],
    payment_type: PaymentType,
    price_per_mib: Decimal,
    price_per_mib_second: Decimal,
    owner: str,
    item_hash: str,
) -> List[AccountCostsDb]:
    costs: List[AccountCostsDb] = []

    for volume in volumes:
        if isinstance(volume, SizedVolume):
            storage_mib = Decimal(volume.size_mib)

            cost_hold = format_cost(storage_mib * price_per_mib)
            cost_stream = format_cost(
                storage_mib * price_per_mib_second,
            )

            costs.append(
                AccountCostsDb(
                    owner=owner,
                    item_hash=item_hash,
                    type=volume.cost_type,
                    name=volume.name,
                    payment_type=payment_type,
                    cost_hold=cost_hold,
                    cost_stream=cost_stream,
                )
            )

        elif isinstance(volume, RefVolume):
            file = _get_file_from_ref(
                session=session, ref=volume.ref, use_latest=volume.use_latest
            )

            if file is None:
                # NOTE: There are legacy volumes with missing references
                # skip cost calculation for them instead of raising an error
                continue

            storage_mib = Decimal(file.size / MiB)

            cost_hold = format_cost(storage_mib * price_per_mib)
            cost_stream = format_cost(
                storage_mib * price_per_mib_second,
            )

            costs.append(
                AccountCostsDb(
                    owner=owner,
                    item_hash=item_hash,
                    type=volume.cost_type,
                    name=volume.name,
                    ref=volume.ref,
                    payment_type=payment_type,
                    cost_hold=cost_hold,
                    cost_stream=cost_stream,
                )
            )

    return costs


def _get_execution_volumes_costs(
    session: DbSession,
    content: ExecutableContent,
    pricing: ProductPricing,
    payment_type: PaymentType,
    item_hash: str,
) -> List[AccountCostsDb]:
    volumes: List[RefVolume | SizedVolume] = []

    if isinstance(content, InstanceContent):
        volumes.append(
            SizedVolume(
                CostType.EXECUTION_INSTANCE_VOLUME_ROOTFS,
                content.rootfs.size_mib,
                content.rootfs.parent.ref,
            )
        )

    elif isinstance(content, ProgramContent):
        volumes += [
            RefVolume(
                CostType.EXECUTION_PROGRAM_VOLUME_CODE,
                content.code.ref,
                content.code.use_latest,
            ),
            RefVolume(
                CostType.EXECUTION_PROGRAM_VOLUME_RUNTIME,
                content.runtime.ref,
                content.runtime.use_latest,
            ),
        ]

        if content.data:
            volumes.append(
                RefVolume(
                    CostType.EXECUTION_PROGRAM_VOLUME_DATA,
                    content.data.ref,
                    content.data.use_latest,
                ),
            )

    for i, volume in enumerate(content.volumes):
        # NOTE: There are legacy volumes with no "mount" property set
        # or with same values for different volumes causing unique key constraint errors
        name_prefix = f"#{i}"

        if isinstance(volume, ImmutableVolume):
            name = (
                f"{name_prefix}:{volume.mount or CostType.EXECUTION_VOLUME_INMUTABLE}"
            )

            volumes.append(
                RefVolume(
                    CostType.EXECUTION_VOLUME_INMUTABLE,
                    volume.ref,
                    volume.use_latest,
                    name,
                ),
            )
        else:
            name = (
                f"{name_prefix}:{volume.mount or CostType.EXECUTION_VOLUME_PERSISTENT}"
            )

            volumes.append(
                SizedVolume(
                    CostType.EXECUTION_VOLUME_PERSISTENT,
                    volume.size_mib,
                    None,
                    name,
                ),
            )

    price_per_mib = pricing.price.storage.holding
    price_per_mib_second = pricing.price.storage.payg / HOUR

    return _get_volumes_costs(
        session,
        volumes,
        payment_type,
        price_per_mib,
        price_per_mib_second,
        content.address,
        item_hash,
    )


def _get_additional_storage_price(
    session: DbSession,
    content: ExecutableContent,
    pricing: ProductPricing,
    payment_type: PaymentType,
    item_hash: str,
) -> List[AccountCostsDb]:
    # EXECUTION VOLUMES COSTS
    costs = _get_execution_volumes_costs(
        session, content, pricing, payment_type, item_hash
    )

    # EXECUTION STORAGE DISCOUNT
    nb_compute_units = _get_nb_compute_units(content, pricing.compute_unit)
    execution_volume_discount_mib = (
        pricing.compute_unit.disk_mib if pricing.compute_unit else 0
    ) * nb_compute_units

    price_per_mib = pricing.price.storage.holding
    price_per_mib_second = pricing.price.storage.payg / HOUR

    max_discount_hold = execution_volume_discount_mib * price_per_mib
    max_discount_stream = execution_volume_discount_mib * price_per_mib_second

    discount_holding = min(
        Decimal(reduce(lambda x, y: x + Decimal(y.cost_hold), costs, Decimal(0))),
        max_discount_hold,
    )
    discount_stream = min(
        Decimal(reduce(lambda x, y: x + Decimal(y.cost_stream), costs, Decimal(0))),
        max_discount_stream,
    )

    cost_hold = format_cost(-discount_holding)
    cost_stream = format_cost(-discount_stream)

    costs.append(
        AccountCostsDb(
            owner=content.address,
            item_hash=item_hash,
            type=CostType.EXECUTION_VOLUME_DISCOUNT,
            name=CostType.EXECUTION_VOLUME_DISCOUNT,
            payment_type=payment_type,
            cost_hold=cost_hold,
            cost_stream=cost_stream,
        )
    )

    return costs


def _calculate_executable_costs(
    session: DbSession,
    content: ExecutableContent,
    pricing: ProductPricing,
    item_hash: str,
) -> List[AccountCostsDb]:
    payment_type = get_payment_type(content)

    if not pricing.compute_unit:
        logger.warning(
            "compute_unit not defined for type '{}' in pricing aggregate".format(
                pricing.type.value
            )
        )

    if not pricing.price.compute_unit:
        raise ValueError(
            "compute_unit price not defined for type '{}' in pricing aggregate".format(
                pricing.type.value
            )
        )

    # EXECUTION COST
    compute_units_required = _get_nb_compute_units(content, pricing.compute_unit)
    compute_unit_multiplier = _get_compute_unit_multiplier(content)

    compute_unit_cost = pricing.price.compute_unit.holding
    compute_unit_cost_second = pricing.price.compute_unit.payg / HOUR

    if not pricing.price.compute_unit:
        raise ValueError(
            "compute_unit not defined for type '{}' in pricing aggregate".format(
                pricing.type.value
            )
        )

    compute_unit_cost = pricing.price.compute_unit.holding
    compute_unit_cost_second = pricing.price.compute_unit.payg / HOUR

    if not pricing.price.compute_unit:
        raise ValueError(
            "compute_unit not defined for type '{}' in pricing aggregate".format(
                pricing.type.value
            )
        )

    compute_unit_cost = pricing.price.compute_unit.holding
    compute_unit_cost_second = pricing.price.compute_unit.payg / HOUR

    compute_unit_price = (
        compute_units_required * compute_unit_multiplier * compute_unit_cost
    )
    compute_unit_price_stream = (
        compute_units_required * compute_unit_multiplier * compute_unit_cost_second
    )

    cost_hold = format_cost(compute_unit_price)
    cost_stream = format_cost(compute_unit_price_stream)

    execution_cost = AccountCostsDb(
        owner=content.address,
        item_hash=item_hash,
        type=CostType.EXECUTION,
        name=pricing.type,
        payment_type=payment_type,
        cost_hold=cost_hold,
        cost_stream=cost_stream,
    )

    costs: List[AccountCostsDb] = [execution_cost]
    costs += _get_additional_storage_price(
        session, content, pricing, payment_type, item_hash
    )

    return costs


def _calculate_storage_costs(
    session: DbSession,
    content: StoreContent,
    pricing: ProductPricing,
    item_hash: str,
) -> List[AccountCostsDb]:
    payment_type = get_payment_type(content)

    file = get_file(session=session, file_hash=content.item_hash)
    if file is None:
        raise RuntimeError(f"Could not find file {item_hash}.")

    price_per_mib = pricing.price.storage.holding
    price_per_mib_second = pricing.price.storage.payg / HOUR

    storage_mib = Decimal(file.size / MiB)

    cost_hold = format_cost(storage_mib * price_per_mib)
    cost_stream = format_cost(
        storage_mib * price_per_mib_second,
    )
    return [
        AccountCostsDb(
            owner=content.address,
            item_hash=item_hash,
            type=CostType.STORAGE,
            name=CostType.STORAGE,
            payment_type=payment_type,
            cost_hold=cost_hold,
            cost_stream=cost_stream,
        )
    ]


def get_detailed_costs(
    session: DbSession,
    content: CostComputableContent,
    item_hash: str,
    pricing: Optional[ProductPricing] = None,
    settings: Optional[Settings] = None,
) -> List[AccountCostsDb]:
    settings = settings or _get_settings(session)
    pricing = pricing or _get_product_price(session, content, settings)

    if isinstance(content, StoreContent):
        return _calculate_storage_costs(session, content, pricing, item_hash)
    else:
        return _calculate_executable_costs(session, content, pricing, item_hash)


def get_total_and_detailed_costs(
    session: DbSession,
    content: CostComputableContent,
    item_hash: str,
) -> Tuple[Decimal, List[AccountCostsDb]]:
    payment_type = get_payment_type(content)

    costs = get_detailed_costs(session, content, item_hash)
    cost = format_cost(
        reduce(lambda x, y: x + y.cost_stream, costs, Decimal(0))
        if payment_type == PaymentType.superfluid
        else reduce(lambda x, y: x + y.cost_hold, costs, Decimal(0))
    )

    return Decimal(cost), list(costs)


def get_total_and_detailed_costs_from_db(
    session: DbSession,
    content: ExecutableContent,
    item_hash: str,
) -> Tuple[Decimal, List[AccountCostsDb]]:
    payment_type = get_payment_type(content)

    costs = get_message_costs(session, item_hash)
    cost = format_cost(
        reduce(lambda x, y: x + y.cost_stream, costs, Decimal(0))
        if payment_type == PaymentType.superfluid
        else reduce(lambda x, y: x + y.cost_hold, costs, Decimal(0))
    )

    return Decimal(cost), list(costs)
