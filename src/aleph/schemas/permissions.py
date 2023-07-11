from typing import Optional, List, Literal, Dict, Annotated, Union

from aleph_message.models import BaseContent
from pydantic import BaseModel, Field

from aleph.types.channel import Channel


class BasePermission(BaseModel):
    # Discriminator field for the different permission types.
    type: str
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
    refs: Optional[List[str]] = Field(
        default=None,
        description="Restricts CRUD operations to objects with the specified ref.",
    )
    addresses: Optional[List[str]] = Field(
        default=None,
        description="Restricts update and delete operations to objects created by any of the specified addresses.",
    )


class AggregatePermission(CrudPermission):
    type: Literal["aggregate"]
    keys: Optional[List[str]] = Field(
        default=None,
        description="Restricts aggregate operations to a specific aggregate key.",
    )


class PostPermission(CrudPermission):
    type: Literal["post"]
    post_types: Optional[List[str]] = Field(
        default=None, description="Restricts post operations to a specific post type."
    )


class StorePermission(CrudPermission):
    type: Literal["store"]


class ExecutablePermission(CrudPermission):
    type: Literal["executable"]


Permission = Annotated[
    Union[
        DelegationPermission,
        AggregatePermission,
        PostPermission,
        StorePermission,
        ExecutablePermission,
    ],
    Field(discriminator="type"),
]


class PermissionContent(BaseContent):
    permissions: Dict[str, List[Permission]]
