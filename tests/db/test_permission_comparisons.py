import copy

from aleph.db.models import BasePermissionDb, PermissionType, PostPermissionDb
import pytest
import datetime as dt


POST_PERMISSION = PostPermissionDb(
    owner="0xdeadbeef",
    granted_by="0xdeadbeef",
    address="0xbadbabe",
    type=PermissionType.POST.value,
    valid_from=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
    valid_until="infinity",
    post_type=None,
)


@pytest.mark.parametrize(
    "permission_1,permission_2", [(POST_PERMISSION, POST_PERMISSION)]
)
def test_permissions_equal(
    permission_1: BasePermissionDb, permission_2: BasePermissionDb
):
    assert permission_1 == permission_2
