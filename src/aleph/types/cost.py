from decimal import Decimal
from enum import Enum
from typing import Iterable, List, Optional, Union

from aleph.db.models import AggregateDb
from aleph.toolkit.constants import ProductPriceType


class ProductPriceOptions:
    holding: Decimal
    payg: Decimal
    credit: Decimal

    def __init__(
        self,
        holding: Optional[str | Decimal],
        payg: Optional[str | Decimal] = Decimal(0),
        credit: Optional[str | Decimal] = Decimal(0),
    ):
        self.holding = Decimal(holding or 0)
        self.payg = Decimal(payg or 0)
        self.credit = Decimal(credit or 0)


class ProductComputeUnit:
    vcpus: int
    disk_mib: int
    memory_mib: int

    def __init__(self, vcpus: int, disk_mib: int, memory_mib: int):
        self.vcpus = vcpus
        self.disk_mib = disk_mib
        self.memory_mib = memory_mib


class ProductPrice:
    storage: ProductPriceOptions
    compute_unit: Optional[ProductPriceOptions]

    def __init__(
        self,
        storage: ProductPriceOptions,
        compute_unit: Optional[ProductPriceOptions] = None,
    ):
        self.storage = storage
        self.compute_unit = compute_unit


class ProductTier:
    id: str
    compute_units: int
    model: Optional[str] = None
    vram: Optional[int] = None

    def __init__(
        self,
        id: str,
        compute_units: int,
        model: Optional[str] = None,
        vram: Optional[int] = None,
    ):
        self.id = id
        self.compute_units = compute_units
        self.model = model
        self.vram = vram


# Product types that borrow another product's numbers when the price
# aggregate does not define them yet. Only the lookup key changes; the
# resulting ProductPricing keeps the requested type.
PRICE_TYPE_FALLBACKS = {
    ProductPriceType.VPROGRAM: ProductPriceType.INSTANCE_CONFIDENTIAL,
}


def resolve_price_type_key(
    price_type: ProductPriceType, available: Iterable[Union[ProductPriceType, str]]
) -> ProductPriceType:
    """Return the aggregate key to read for `price_type`.

    Aggregate keys can be enum members or plain strings depending on the
    source (DEFAULT_PRICE_AGGREGATE vs. a DB aggregate), so compare on the
    string value.
    """
    keys = {str(getattr(k, "value", k)) for k in available}
    if price_type.value in keys:
        return price_type
    fallback = PRICE_TYPE_FALLBACKS.get(price_type)
    if fallback is not None and fallback.value in keys:
        return fallback
    return price_type


class ProductPricing:
    type: ProductPriceType
    price: ProductPrice
    compute_unit: Optional[ProductComputeUnit]
    tiers: Optional[List[ProductTier]]

    def __init__(
        self,
        price_type: ProductPriceType,
        price: ProductPrice,
        compute_unit: Optional[ProductComputeUnit] = None,
        tiers: Optional[List[ProductTier]] = None,
    ):
        self.type = price_type
        self.price = price
        self.compute_unit = compute_unit
        self.tiers = tiers

    def with_type(self, price_type: ProductPriceType) -> "ProductPricing":
        """Return a copy of this pricing with `type` replaced.

        Used when a pricing model lookup fell back to another product's
        entry (see `resolve_price_type_key`): the numbers (price,
        compute_unit, tiers) are kept as-is, only the reported type changes
        so callers still see the product they actually asked about.
        """
        return ProductPricing(
            price_type=price_type,
            price=self.price,
            compute_unit=self.compute_unit,
            tiers=self.tiers,
        )

    @staticmethod
    def from_aggregate(
        price_type: ProductPriceType, aggregate: Union[AggregateDb, dict]
    ):
        source = aggregate.content if isinstance(aggregate, AggregateDb) else aggregate
        key = resolve_price_type_key(price_type, source.keys())
        content = source[key.value]

        price = content["price"]
        compute_unit = content.get("compute_unit", None)
        tiers = content.get("tiers", None)

        product_tiers = []
        if tiers:
            product_tiers = [
                ProductTier(
                    id=tier["id"],
                    compute_units=tier["compute_units"],
                    model=tier.get("model", None),
                    vram=tier.get("vram", None),
                )
                for tier in tiers
            ]

        pricing = ProductPricing(
            price_type=price_type,
            price=ProductPrice(
                storage=ProductPriceOptions(
                    price["storage"].get("holding", None),
                    price["storage"].get("payg", None),
                    price["storage"].get("credit", None),
                ),
                compute_unit=(
                    ProductPriceOptions(
                        price["compute_unit"].get("holding", None),
                        price["compute_unit"].get("payg", None),
                        price["compute_unit"].get("credit", None),
                    )
                    if compute_unit
                    else None
                ),
            ),
            compute_unit=(
                ProductComputeUnit(
                    compute_unit["vcpus"],
                    compute_unit["disk_mib"],
                    compute_unit["memory_mib"],
                )
                if tiers
                else None
            ),
            tiers=product_tiers,
        )

        return pricing


class CostType(str, Enum):
    EXECUTION = "EXECUTION"
    EXECUTION_VOLUME_PERSISTENT = "EXECUTION_VOLUME_PERSISTENT"
    EXECUTION_VOLUME_INMUTABLE = "EXECUTION_VOLUME_INMUTABLE"
    EXECUTION_VOLUME_DISCOUNT = "EXECUTION_VOLUME_DISCOUNT"
    EXECUTION_INSTANCE_VOLUME_ROOTFS = "EXECUTION_INSTANCE_VOLUME_ROOTFS"
    EXECUTION_PROGRAM_VOLUME_CODE = "EXECUTION_PROGRAM_VOLUME_CODE"
    EXECUTION_PROGRAM_VOLUME_RUNTIME = "EXECUTION_PROGRAM_VOLUME_RUNTIME"
    EXECUTION_PROGRAM_VOLUME_DATA = "EXECUTION_PROGRAM_VOLUME_DATA"
    STORAGE = "STORAGE"


class VolumeCost:
    def __init__(self, cost_type: CostType, name: Optional[str] = None):
        self.cost_type = cost_type
        self.name = name or cost_type


class SizedVolume(VolumeCost):
    def __init__(
        self,
        cost_type: CostType,
        size_mib: Decimal,
        ref: Optional[str] = None,
        *args,
    ):
        super().__init__(cost_type, *args)
        self.size_mib = size_mib
        self.ref = ref


class RefVolume(VolumeCost):
    def __init__(self, cost_type: CostType, ref: str, use_latest: bool, *args):
        super().__init__(cost_type, *args)
        self.ref = ref
        self.use_latest = use_latest
