from enum import Enum
from typing import Dict

from pydantic import BaseModel, Field

from aleph.schemas.messages_query_params import DEFAULT_MESSAGES_PER_PAGE, DEFAULT_PAGE
from aleph.types.sort_order import SortOrder


class SortBy(str, Enum):
    """Message types supported by Aleph"""

    post = "POST"
    aggregate = "AGGREGATE"
    store = "STORE"
    program = "PROGRAM"
    instance = "INSTANCE"
    forget = "FORGET"
    messages = "MESSAGES"


class AddressesQueryParams(BaseModel):
    address_contains: str | None = Field(
        default=None,
        alias="addressContains",
        description=(
            "Case-insensitive substring filter for addresses. "
            "Example: addressContains=abc → matches any address containing 'abc'."
        ),
    )

    # Sorts Results
    sort_by: SortBy = Field(
        default=SortBy.messages,
        alias="sortBy",
        description=(
            "Key used to sort the returned addresses. Available values:\n"
            "- 'post': sort by the number of post\n"
            "- 'aggregate': sort by aggregate activity\n"
            "- 'store': sort by store activity\n"
            "- 'forget': sort by forget activity\n"
            "- 'program': sort by program activity\n"
            "- 'instance': sort by instance activity\n"
            "- 'messages': sort by the number of messages"
        ),
    )
    sort_order: SortOrder = Field(
        default=SortOrder.DESCENDING,
        alias="sortOrder",
        description=(
            "Order to apply to the sorting:\n"
            "- 1 (ASCENDING): smallest values first\n"
            "- -1 (DESCENDING): largest values first"
        ),
    )
    # Minimum count on any of the sort by  elements
    filters: Dict[SortBy, int] | None = Field(
        default=None,
        description="Minimum values required for each sort category. Example: { 'POST': 3 }",
    )

    # Pagination
    pagination: int = Field(
        default=DEFAULT_MESSAGES_PER_PAGE,
        ge=0,
        description="Maximum number of address to return. Specifying 0 removes this limit.",
    )
    page: int = Field(
        default=DEFAULT_PAGE, ge=1, description="Offset in pages. Starts at 1."
    )
