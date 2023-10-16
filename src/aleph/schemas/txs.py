from typing import Any, Mapping, Self

from aleph_message.models import Chain
from pydantic import BaseModel

from aleph.schemas.chains.tx_context import TxContext
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.chain_sync import ChainSyncProtocol
import datetime as dt


class PendingTx(BaseModel):
    hash: str
    chain: Chain
    height: int
    datetime: dt.datetime
    publisher: str
    protocol: ChainSyncProtocol
    protocol_version: int
    content: Any

    @classmethod
    def from_sync_tx_context(
        cls, tx_context: TxContext, tx_data: Mapping[str, Any]
    ) -> Self:
        """
        Builds a chain tx object from the tx context and data.
        Only applicable to message sync transactions.

        :param tx_context: Transaction metadata.
        :param tx_data: Data included in the transaction.
        """

        return cls(
            hash=tx_context.hash,
            chain=Chain(tx_context.chain),
            height=tx_context.height,
            datetime=timestamp_to_datetime(tx_context.time),
            publisher=tx_context.publisher,
            protocol=tx_data["protocol"],
            protocol_version=tx_data["version"],
            content=tx_data["content"],
        )
