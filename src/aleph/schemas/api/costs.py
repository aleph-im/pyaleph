from typing import List

from pydantic import BaseModel, ConfigDict, field_validator

from aleph.toolkit.costs import format_cost_str


class EstimatedCostDetailResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    type: str
    name: str
    cost_hold: str
    cost_stream: str
    cost_credit: str

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
