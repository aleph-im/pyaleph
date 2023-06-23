import datetime as dt
from typing import Optional, List, Literal

from aleph_message.models import BaseContent
from pydantic import BaseModel, Field

from aleph.types.channel import Channel


class BasePermission(BaseModel):
    type: str
    address: str
    valid_from: Optional[dt.datetime] = None
    valid_until: Optional[dt.datetime] = None
    channel: Optional[Channel] = None


class DelegationPermission(BasePermission):
    """
    Permission to delegate permissions to other addresses. An address with delegation permission
    can delegate any subset of the permissions given by the original address to a third address.
    """

    type: Literal["delegation"]


class CrudPermission(BasePermission):
    """
    Generic CReate, Update and Delete permissions.
    """

    create: bool = False
    update: bool = False
    delete: bool = False
    ref: Optional[str] = Field(
        ...,
        description="Restricts CRUD operations to objects with the specified ref.",
    )
    addresses: Optional[List[str]] = Field(
        default=None,
        description="Restricts update and delete operations to objects created by any of the specified addresses.",
    )


class AggregatePermission(CrudPermission):
    type: Literal["aggregate"]
    key: Optional[str] = Field(
        ..., description="Restricts aggregate operations to a specific aggregate key."
    )


class PostPermission(CrudPermission):
    type: Literal["post"]
    post_type: Optional[str] = Field(
        ..., description="Restricts post operations to a specific post type."
    )


class VmPermission(CrudPermission):
    type: Literal["vm"]


class PermissionContent(BaseContent):
    permissions: List[BasePermission]
