"""Query and response models of the /api/v0/credits endpoints (spec 10)."""

from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from aleph.schemas.messages_query_params import DEFAULT_PAGE

DEFAULT_CREDITS_PAGINATION = 100
MAX_CREDITS_PAGINATION = 1000
# One month of hourly periods: the widest /credits/revenue window.
MAX_REVENUE_PERIODS = 744
QUOTE_TOKENS = ("usdc", "eth", "aleph")


class _Paginated(BaseModel):
    page: int = Field(
        default=DEFAULT_PAGE, ge=1, description="Offset in pages. Starts at 1."
    )
    pagination: int = Field(
        default=DEFAULT_CREDITS_PAGINATION,
        ge=1,
        le=MAX_CREDITS_PAGINATION,
        description="Items per page (1 to 1000).",
    )


class CreditRevenueQueryParams(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: int = Field(
        alias="from",
        ge=0,
        description="First period (unix seconds, hour-aligned), inclusive.",
    )
    to: int = Field(
        ge=0, description="End of the range (unix seconds, hour-aligned), exclusive."
    )


class CreditExclusionsQueryParams(_Paginated):
    period: Optional[int] = Field(
        default=None, ge=0, description="Period start (unix seconds, hour-aligned)."
    )
    reason: Optional[str] = Field(default=None, description="excluded_reason value.")
    address: Optional[str] = Field(default=None, description="Ledger address.")


class CreditQuoteQueryParams(BaseModel):
    token: str = Field(description="usdc, eth or aleph (case-insensitive).")
    amount: int = Field(
        ge=0,
        lt=10**60,
        description="Raw token amount (smallest unit: 1e-6 USDC, wei).",
    )

    @field_validator("token", mode="before")
    def lower_token(cls, value):
        token = str(value).lower()
        if token not in QUOTE_TOKENS:
            raise ValueError(f"token must be one of {', '.join(QUOTE_TOKENS)}")
        return token


class CreditTopupsQueryParams(_Paginated):
    address: str = Field(description="Sender address of the top-ups.")


class CreditKindTotal(BaseModel):
    rows: int
    amount: int


class CreditAttestationRef(BaseModel):
    item_hash: str
    sender: Optional[str]


class CreditPeriodResponse(BaseModel):
    period: int
    # closed: folded and current; dirty: folded, waiting for a refold from
    # dirty_from_period; open: not folded yet.
    status: str
    period_hash: Optional[str]
    checkpoint_hash: Optional[str]
    attestation: Optional[CreditAttestationRef]
    totals: Dict[str, CreditKindTotal]


class CreditRevenueNode(BaseModel):
    execution: int
    hold_equivalent: int


class CreditRevenuePeriod(BaseModel):
    period: int
    credit_price_aleph: Optional[str]
    nodes: Dict[str, CreditRevenueNode]
    storage: int
    execution_total: int


class CreditRevenueResponse(BaseModel):
    periods: List[CreditRevenuePeriod]


class CreditExclusionItem(BaseModel):
    period: int
    address: Optional[str]
    kind: str
    business_key: str
    amount: int
    resource: Optional[str]
    node_id: Optional[str]
    seconds: Optional[int]
    rate: Optional[str]
    source_ref: Optional[str]
    excluded_reason: str


class CreditExclusionsResponse(BaseModel):
    exclusions: List[CreditExclusionItem]
    pagination_page: int
    pagination_per_page: int
    pagination_total: int
    pagination_item: str = "exclusions"


class CreditQuoteResponse(BaseModel):
    credits: int
    # Credits per raw unit as an exact fraction; strings, as they exceed 2**53.
    quote_num: str
    quote_den: str
    block_number: int


class CreditTopupItem(BaseModel):
    chain_id: int
    tx_hash: str
    log_index: int
    block_number: int
    block_time: int
    from_address: str
    token: str
    raw_amount: str
    quote_num: Optional[str]
    quote_den: Optional[str]
    credits: Optional[int]
    status: str
    ledger_period: Optional[int]


class CreditTopupsResponse(BaseModel):
    address: str
    topups: List[CreditTopupItem]
    pagination_page: int
    pagination_per_page: int
    pagination_total: int
    pagination_item: str = "topups"
