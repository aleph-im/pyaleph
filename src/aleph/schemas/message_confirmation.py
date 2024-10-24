from aleph_message.models import Chain
from pydantic.v1 import BaseModel, Field


class MessageConfirmation(BaseModel):
    chain: Chain = Field(
        ..., description="Chain from which the confirmation was fetched."
    )
    height: int = Field(
        ..., description="Block in which the confirmation was published."
    )
    hash: str = Field(
        ...,
        description="Hash of the transaction/block in which the confirmation was published.",
    )
    time: float = Field(
        ...,
        description="Transaction timestamp, in Unix time (number of seconds since epoch).",
    )
    publisher: str = Field(..., description="Publisher of the confirmation on chain.")
