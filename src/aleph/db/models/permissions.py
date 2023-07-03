import datetime as dt
from enum import Enum
from typing import Optional, List, Dict, Any

from sqlalchemy import Column, String, TIMESTAMP, Boolean, ARRAY, Integer, Index

from aleph.db.models import Base
from aleph.types.channel import Channel


class PermissionType(str, Enum):
    AGGREGATE = "aggregate"
    DELEGATE = "delegate"
    POST = "post"
    STORE = "store"
    EXECUTABLE = "executable"


class BasePermissionDb(Base):
    __tablename__ = "permissions"

    id: int = Column(Integer, primary_key=True)
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
    __table_args__ = (Index("ix_owner_address", owner, address),)


class DelegationPermissionDb(BasePermissionDb):
    __mapper_args__ = {
        "polymorphic_identity": PermissionType.DELEGATE.value,
    }


class CrudPermissionDb(BasePermissionDb):
    create: bool = Column(Boolean, nullable=False)
    update: bool = Column(Boolean, nullable=False)
    delete: bool = Column(Boolean, nullable=False)
    ref: Optional[str] = Column(String, nullable=True)
    addresses: Optional[List[str]] = Column(ARRAY(String), nullable=True)


class AggregatePermissionDb(CrudPermissionDb):
    key: Optional[str] = Column(String, nullable=True)

    __mapper_args__ = {
        "polymorphic_identity": PermissionType.AGGREGATE.value,
    }


class PostPermissionDb(CrudPermissionDb):
    post_type: Optional[str] = Column(String, nullable=True)

    __mapper_args__ = {
        "polymorphic_identity": PermissionType.POST.value,
    }


class StorePermissionDb(CrudPermissionDb):
    __mapper_args__ = {
        "polymorphic_identity": PermissionType.STORE.value,
    }


class ExecutablePermissionDb(CrudPermissionDb):

    __mapper_args__ = {
        "polymorphic_identity": PermissionType.EXECUTABLE.value,
    }
