from typing import List, Optional

from aleph_message.models import PaymentType
from pydantic import BaseModel, ConfigDict, Field, field_validator

from aleph.schemas.messages_query_params import DEFAULT_PAGE
from aleph.toolkit.costs import format_cost_str


class GetCostsQueryParams(BaseModel):
    """Query parameters for the /api/v0/costs endpoint."""

    address: Optional[str] = Field(default=None, description="Filter by owner address")
    item_hash: Optional[str] = Field(
        default=None, description="Filter by specific resource item_hash"
    )
    payment_type: Optional[PaymentType] = Field(
        default=None, description="Filter by payment type (hold, superfluid, credit)"
    )
    include_details: int = Field(
        default=0,
        ge=0,
        le=2,
        description="Detail level: 0=summary only, 1=include resource list, 2=include resource list with cost breakdown per component",
    )
    pagination: int = Field(
        default=100,
        ge=10,
        le=1000,
        description="Number of resources per page (10-1000).",
    )
    page: int = Field(
        default=DEFAULT_PAGE, ge=1, description="Offset in pages. Starts at 1."
    )


class CostsSummaryResponse(BaseModel):
    """Summary of aggregated costs."""

    total_consumed_credits: int = Field(
        description="Total credits consumed by matching resources"
    )
    total_cost_hold: str = Field(description="Total hold cost for matching resources")
    total_cost_stream: str = Field(
        description="Total streaming cost (per second) for matching resources"
    )
    total_cost_credit: str = Field(
        description="Total credit cost for matching resources"
    )
    resource_count: int = Field(description="Number of matching resources")


class CostsFiltersResponse(BaseModel):
    """Applied filters in the response."""

    address: Optional[str] = None
    item_hash: Optional[str] = None
    payment_type: Optional[str] = None


class CostComponentDetail(BaseModel):
    """Detailed cost breakdown for a single component (execution, storage, etc.)."""

    model_config = ConfigDict(from_attributes=True)

    type: str = Field(description="Cost component type (EXECUTION, STORAGE, etc.)")
    name: str = Field(description="Cost component name")
    cost_hold: str = Field(description="Hold cost for this component")
    cost_stream: str = Field(description="Streaming cost for this component")
    cost_credit: str = Field(description="Credit cost for this component")
    size_mib: Optional[float] = Field(
        default=None,
        description="Storage size in MiB (populated for volume/storage-related components: STORAGE, EXECUTION_INSTANCE_VOLUME_ROOTFS, EXECUTION_PROGRAM_VOLUME_CODE, EXECUTION_PROGRAM_VOLUME_RUNTIME, EXECUTION_PROGRAM_VOLUME_DATA, EXECUTION_VOLUME_PERSISTENT, EXECUTION_VOLUME_INMUTABLE)",
    )

    @field_validator("cost_hold", "cost_stream", "cost_credit", mode="before")
    def check_format_price(cls, v):
        return format_cost_str(v)


class ResourceCostItem(BaseModel):
    """Cost information for a single resource."""

    model_config = ConfigDict(from_attributes=True)

    item_hash: str
    owner: str
    payment_type: str
    consumed_credits: int = Field(description="Total credits consumed by this resource")
    cost_hold: str = Field(description="Hold cost for this resource")
    cost_stream: str = Field(
        description="Streaming cost (per second) for this resource"
    )
    cost_credit: str = Field(description="Credit cost for this resource")
    detail: Optional[List[CostComponentDetail]] = Field(
        default=None,
        description="Detailed cost breakdown per component (if include_details=2)",
    )

    @field_validator("cost_hold", "cost_stream", "cost_credit", mode="before")
    def check_format_price(cls, v):
        return format_cost_str(v)


class GetCostsResponse(BaseModel):
    """Response for the /api/v0/costs endpoint."""

    summary: CostsSummaryResponse
    filters: CostsFiltersResponse
    resources: Optional[List[ResourceCostItem]] = Field(
        default=None,
        description="List of resources with costs (if include_details >= 1)",
    )
    pagination_page: Optional[int] = None
    pagination_total: Optional[int] = None
    pagination_per_page: Optional[int] = None
    pagination_item: Optional[str] = None


class EstimatedCostDetailResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    type: str
    name: str
    cost_hold: str
    cost_stream: str
    cost_credit: str
    size_mib: Optional[float] = Field(
        default=None,
        description="Storage size in MiB (populated for volume/storage-related components: STORAGE, EXECUTION_INSTANCE_VOLUME_ROOTFS, EXECUTION_PROGRAM_VOLUME_CODE, EXECUTION_PROGRAM_VOLUME_RUNTIME, EXECUTION_PROGRAM_VOLUME_DATA, EXECUTION_VOLUME_PERSISTENT, EXECUTION_VOLUME_INMUTABLE)",
    )

    @field_validator("cost_hold", "cost_stream", "cost_credit", mode="before")
    def check_format_price(cls, v):
        return format_cost_str(v)


class EstimatedCostsResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    required_tokens: float
    payment_type: str
    cost: str
    detail: List[EstimatedCostDetailResponse]
    charged_address: str
