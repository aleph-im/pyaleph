from typing import Optional, List, Literal

from aleph_message.models import BaseContent
from pydantic import BaseModel
import datetime as dt

from aleph.types.channel import Channel


class BasePermission(BaseModel):
    type: str
    address: str
    valid_from: Optional[dt.datetime]
    valid_until: Optional[dt.datetime]
    channel: Optional[Channel]


class DelegationPermission(BasePermission):
    type: Literal["delegation"]


class DeletePermission(BaseModel):
    # The key can only delete objects it created.
    only_created: bool = True


class CrudPermission(BasePermission):
    """
    Generic Create, Update and Delete permissions.

    Read permissions do not apply for aleph.im, but `Cud` sounds weird.
    """

    create: bool = False
    update: bool = False
    delete: Optional[DeletePermission] = None
    ref: Optional[str]


class AggregatePermission(CrudPermission):
    type: Literal["aggregate"]
    key: Optional[str]


class PostPermission(CrudPermission):
    type: Literal["post"]
    post_type: Optional[str]


class VmPermission(CrudPermission):
    type: Literal["vm"]


class PermissionContent(BaseContent):
    permissions: List[BasePermission]
