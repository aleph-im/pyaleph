"""
Chain-related tables. CCNs sync with chains in one of two ways:
1. by fetching data from an indexer
2. by indexing the chain directly.
"""

import datetime as dt
from typing import Dict, Any, Union, Mapping

from aleph_message.models import Chain
from sqlalchemy import Column, Integer, String, TIMESTAMP, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy_utils.types.choice import ChoiceType

from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.chain_sync import ChainSyncProtocol, ChainEventType
from .base import Base
from aleph.schemas.chains.tx_context import TxContext
from ...toolkit.range import Range


class ChainSyncStatusDb(Base):
    """
    Keeps track of chains indexed by the CCN.
    """

    __tablename__ = "chains_sync_status"

    chain: Chain = Column(ChoiceType(Chain), primary_key=True)
    type: ChainEventType = Column(ChoiceType(ChainEventType), primary_key=True)
    height: int = Column(Integer, nullable=False)
    last_update: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)


class IndexerSyncStatusDb(Base):
    """
    Keeps track of the sync status with indexers.

    Several rows can appear for one chain. The indexers work in chunks, meaning that
    some ranges may be missing.
    """

    # TODO: use multiranges with SQLAlchemy 2.0
    #       https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#range-and-multirange-types

    __tablename__ = "indexer_sync_status"

    chain: Chain = Column(ChoiceType(Chain), primary_key=True)
    event_type: ChainEventType = Column(ChoiceType(ChainEventType), primary_key=True)
    start_block_datetime: dt.datetime = Column(
        TIMESTAMP(timezone=True), primary_key=True
    )
    end_block_datetime: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)
    start_included: bool = Column(Boolean, nullable=False)
    end_included: bool = Column(Boolean, nullable=False)
    last_updated: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)

    def to_range(self) -> Range[dt.datetime]:
        return Range(
            self.start_block_datetime,
            self.end_block_datetime,
            lower_inc=self.start_included,
            upper_inc=self.end_included,
        )


class ChainTxDb(Base):
    __tablename__ = "chain_txs"

    hash: str = Column(String, primary_key=True)
    chain: Chain = Column(ChoiceType(Chain), nullable=False)
    height: int = Column(Integer, nullable=False)
    datetime: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)
    publisher: str = Column(String, nullable=False)
    protocol: ChainSyncProtocol = Column(ChoiceType(ChainSyncProtocol), nullable=False)
    protocol_version = Column(Integer, nullable=False)
    content: Any = Column(JSONB, nullable=False)

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
