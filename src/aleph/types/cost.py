from decimal import Decimal
from enum import Enum
from typing import List, Optional

from pydantic import ConstrainedInt

from aleph.db.models.aggregates import AggregateDb


class ProductPriceType(str, Enum):
    STORAGE = "storage"
    WEB3_HOSTING = "web3_hosting"
    PROGRAM = "program"
    PROGRAM_PERSISTENT = "program_persistent"
    INSTANCE = "instance"
    INSTANCE_GPU_PREMIUM = "instance_gpu_premium"
    INSTANCE_CONFIDENTIAL = "instance_confidential"
    INSTANCE_GPU_STANDARD = "instance_gpu_standard"


class ProductPriceOptions:
    holding: Decimal
    payg: Decimal

    def __init__(
        self,
        holding: Optional[str | Decimal],
        payg: Optional[str | Decimal] = Decimal(0),
    ):
        self.holding = Decimal(holding or 0)
        self.payg = Decimal(payg or 0)


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

    @staticmethod
    def from_aggregate(price_type: ProductPriceType, aggregate: AggregateDb):
        content = aggregate.content[price_type.value]

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
                ProductPriceOptions(
                    price["storage"]["holding"],
                    price["storage"].get("payg", None),
                ),
                (
                    ProductPriceOptions(
                        price["compute_unit"]["holding"],
                        price["compute_unit"]["payg"],
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
        size_mib: ConstrainedInt,
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
