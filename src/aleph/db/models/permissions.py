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
    valid_from: Optional[dt.datetime] = Column(TIMESTAMP(timezone=True), nullable=True)
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

    def extends(self, other: "BasePermissionDb") -> bool:
        if not isinstance(other, self.__class__):
            return False

        return (
            self.type == other.type
            and self.owner == other.owner
            and self.granted_by == other.granted_by
            and self.address == other.address
            and self.channel == other.channel
            and other.valid_from <= self.valid_until
        )

    def is_reduced_subset(self, other: "BasePermissionDb") -> bool:
        return (
            self.type == other.type
            and self.owner == other.owner
            and self.granted_by == other.granted_by
            and self.address == other.address
            and self.channel == other.channel
            and
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

    def extends(self, other: BasePermissionDb) -> bool:
        return (
            super().extends(other)
            and isinstance(other, CrudPermissionDb)
            and self.create == other.create
            and self.update == other.update
            and self.delete == other.delete
            and tuple(self.refs) == tuple(other.refs)
            and tuple(self.addresses) == tuple(other.addresses)
        )

    def is_reduced_subset(self, other: BasePermissionDb) -> bool:
        return


class AggregatePermissionDb(CrudPermissionDb):
    __mapper_args__ = {
        "polymorphic_identity": PermissionType.AGGREGATE.value,
    }


class PostPermissionDb(CrudPermissionDb):
    post_types: Optional[List[str]] = Column(ARRAY(String), nullable=True)

    __mapper_args__ = {
        "polymorphic_identity": PermissionType.POST.value,
    }

    def extends(self, other: BasePermissionDb) -> bool:
        return (
            super().extends(other)
            and isinstance(other, PostPermissionDb)
            and tuple(self.post_types) == tuple(other.post_types)
        )


class StorePermissionDb(CrudPermissionDb):
    __mapper_args__ = {
        "polymorphic_identity": PermissionType.STORE.value,
    }


class ExecutablePermissionDb(CrudPermissionDb):
    __mapper_args__ = {
        "polymorphic_identity": PermissionType.EXECUTABLE.value,
    }
