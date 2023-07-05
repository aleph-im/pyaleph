import datetime as dt
from enum import Enum
from typing import Optional, List, Dict, Any

from sqlalchemy import (
    Column,
    String,
    TIMESTAMP,
    Boolean,
    ARRAY,
    Index,
    BigInteger,
    ForeignKey,
    Table,
)
from sqlalchemy.orm import relationship

from aleph.db.models import Base
from aleph.types.channel import Channel


class PermissionType(str, Enum):
    AGGREGATE = "aggregate"
    DELEGATE = "delegate"
    POST = "post"
    STORE = "store"
    EXECUTABLE = "executable"


delegations_table = Table(
    "permission_delegations",
    Base.metadata,
    Column("parent", ForeignKey("permissions.id")),
    Column("child", ForeignKey("permissions.id")),
)


class BasePermissionDb(Base):
    __tablename__ = "permissions"

    id: int = Column(BigInteger, primary_key=True)
    owner: str = Column(String, nullable=False)
    granted_by: str = Column(String, nullable=False, index=True)
    address: str = Column(String, nullable=False)
    type: str = Column(String, nullable=False)
    valid_from: Optional[dt.datetime] = Column(TIMESTAMP(timezone=True), nullable=False)
    valid_until: Optional[dt.datetime] = Column(TIMESTAMP(timezone=True), nullable=True)
    channel: Optional[Channel] = Column(String, nullable=True)

    __mapper_args__: Dict[str, Any] = {
        "polymorphic_on": type,
    }
    children: List["BasePermissionDb"] = relationship(
        "BasePermissionDb",
        secondary=delegations_table,
        primaryjoin=id == delegations_table.c.parent,
        secondaryjoin=id == delegations_table.c.child,
    )

    __table_args__ = (Index("ix_owner_address", owner, address),)

    def is_equivalent_to(self, other: "BasePermissionDb") -> bool:
        """
        Returns whether the permission `other` is equal to this one, ignoring validity ranges.
        """
        return (
                self.type == other.type
                and self.owner == other.owner
                and self.granted_by == other.granted_by
                and self.address == other.address
                and self.channel == other.channel
                and self.valid_until == other.valid_until
        )

    def is_subset(self, other: "BasePermissionDb") -> bool:
        return (
            self.type == other.type
            and self.owner == other.owner
            and self.granted_by == other.granted_by
            and self.address == other.address
            and (self.channel == other.channel or other.channel is None)
        )

    def __hash__(self):
        return hash(
            (
                self.owner,
                self.type,
                self.address,
                self.granted_by,
                self.valid_from,
                self.valid_until,
                self.channel,
            )
        )


class DelegationPermissionDb(BasePermissionDb):
    __mapper_args__ = {
        "polymorphic_identity": PermissionType.DELEGATE.value,
    }


class CrudPermissionDb(BasePermissionDb):
    create: bool = Column(Boolean, nullable=False)
    update: bool = Column(Boolean, nullable=False)
    delete: bool = Column(Boolean, nullable=False)
    refs: Optional[List[str]] = Column(ARRAY(String), nullable=True)
    addresses: Optional[List[str]] = Column(ARRAY(String), nullable=True)

    def __eq__(self, other: BasePermissionDb) -> bool:
        return (
            super().__eq__(other)
            and isinstance(other, CrudPermissionDb)
            and self.create == other.create
            and self.update == other.update
            and self.delete == other.delete
            and tuple(self.refs or []) == tuple(other.refs or [])
            and tuple(self.addresses or []) == tuple(other.addresses or [])
        )

    def is_subset(self, other: BasePermissionDb) -> bool:
        return (
            super().is_subset(other)
            and isinstance(other, CrudPermissionDb)
            and (other.create or self.create == other.create)
            and (other.update or self.update == other.update)
            and (other.delete or self.delete == other.delete)
            and (other.refs is None or (set(self.refs or []) & set(other.refs)))
            and (
                other.addresses is None
                or (set(self.addresses or []) & set(other.addresses))
            )
        )


class AggregatePermissionDb(CrudPermissionDb):
    __mapper_args__ = {
        "polymorphic_identity": PermissionType.AGGREGATE.value,
    }


class PostPermissionDb(CrudPermissionDb):
    post_types: Optional[List[str]] = Column(ARRAY(String), nullable=True)

    __mapper_args__ = {
        "polymorphic_identity": PermissionType.POST.value,
    }

    def __eq__(self, other: BasePermissionDb) -> bool:
        return (
            super().__eq__(other)
            and isinstance(other, PostPermissionDb)
            and tuple(self.post_types or []) == tuple(other.post_types or [])
        )

    def is_subset(self, other: BasePermissionDb) -> bool:
        return super().is_subset(other) and (
            other.post_types is None
            or (set(self.post_types or []) & set(other.post_types))
        )


class StorePermissionDb(CrudPermissionDb):
    __mapper_args__ = {
        "polymorphic_identity": PermissionType.STORE.value,
    }


class ExecutablePermissionDb(CrudPermissionDb):
    __mapper_args__ = {
        "polymorphic_identity": PermissionType.EXECUTABLE.value,
    }
