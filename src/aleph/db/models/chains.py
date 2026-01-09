"""
Chain-related tables. CCNs sync with chains in one of two ways:
1. by fetching data from an indexer
2. by indexing the chain directly.
"""

import datetime as dt
from typing import Any, Dict, Mapping, Union

from aleph_message.models import Chain
from sqlalchemy import TIMESTAMP, Boolean, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy_utils.types.choice import ChoiceType

from aleph.schemas.chains.tx_context import TxContext
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.chain_sync import ChainEventType, ChainSyncProtocol

from ...toolkit.range import Range
from .base import Base


class ChainSyncStatusDb(Base):
    """
    Keeps track of chains indexed by the CCN.
    """

    __tablename__ = "chains_sync_status"

    chain: Mapped[Chain] = mapped_column(ChoiceType(Chain), primary_key=True)
    type: Mapped[ChainEventType] = mapped_column(
        ChoiceType(ChainEventType), primary_key=True
    )
    height: Mapped[int] = mapped_column(Integer, nullable=False)
    last_update: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )


class IndexerSyncStatusDb(Base):
    """
    Keeps track of the sync status with indexers.

    Several rows can appear for one chain. The indexers work in chunks, meaning that
    some ranges may be missing.
    """

    # TODO: use multiranges with SQLAlchemy 2.0
    #       https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#range-and-multirange-types

    __tablename__ = "indexer_sync_status"

    chain: Mapped[Chain] = mapped_column(ChoiceType(Chain), primary_key=True)
    event_type: Mapped[ChainEventType] = mapped_column(
        ChoiceType(ChainEventType), primary_key=True
    )
    start_block_datetime: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True
    )
    end_block_datetime: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    start_included: Mapped[bool] = mapped_column(Boolean, nullable=False)
    end_included: Mapped[bool] = mapped_column(Boolean, nullable=False)
    last_updated: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )

    def to_range(self) -> Range[dt.datetime]:
        return Range(
            self.start_block_datetime,
            self.end_block_datetime,
            lower_inc=self.start_included,
            upper_inc=self.end_included,
        )


class ChainTxDb(Base):
    __tablename__ = "chain_txs"

    hash: Mapped[str] = mapped_column(String, primary_key=True)
    chain: Mapped[Chain] = mapped_column(ChoiceType(Chain), nullable=False)
    height: Mapped[int] = mapped_column(Integer, nullable=False)
    datetime: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    publisher: Mapped[str] = mapped_column(String, nullable=False)
    protocol: Mapped[ChainSyncProtocol] = mapped_column(
        ChoiceType(ChainSyncProtocol), nullable=False
    )
    protocol_version: Mapped[int] = mapped_column(Integer, nullable=False)
    content: Mapped[Any] = mapped_column(JSONB, nullable=False)

    # TODO: this method is only used in tests, make it a helper
    @classmethod
    def from_dict(
        cls,
        tx_dict: Dict[str, Any],
        protocol: ChainSyncProtocol = ChainSyncProtocol.ON_CHAIN_SYNC,
        protocol_version: int = 1,
        content: Union[str, Dict] = "",
    ) -> "ChainTxDb":
        return cls(
            hash=tx_dict["hash"],
            chain=Chain(tx_dict["chain"]),
            height=tx_dict["height"],
            datetime=timestamp_to_datetime(tx_dict["time"]),
            publisher=tx_dict["publisher"],
            protocol=protocol,
            protocol_version=protocol_version,
            content=content,
        )

    @classmethod
    def from_sync_tx_context(
        cls, tx_context: TxContext, tx_data: Mapping[str, Any]
    ) -> "ChainTxDb":
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
