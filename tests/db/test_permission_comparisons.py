import copy

from aleph.db.models import BasePermissionDb, PermissionType, PostPermissionDb
import pytest
import datetime as dt


@pytest.fixture
def post_permission() -> PostPermissionDb:
    return PostPermissionDb(
        owner="0xdeadbeef",
        granted_by="0xdeadbeef",
        address="0xbadbabe",
        type=PermissionType.POST.value,
        valid_from=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
        valid_until=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
        create=False,
        update=True,
        delete=True,
        refs=[
            "8fb44c62d7d8c19bd46bdb823e6f148f9b922214a255aca4957f9455c32a5bd3",
            "186c2a082fa6bc5385515159d10fbb30426ece7e33dab004a1960c66adefdb1d",
        ],
        post_types=["my-posts", "my-db"],
        channel="TEST",
    )


@pytest.fixture
def post_permission_delete_everything() -> PostPermissionDb:
    return PostPermissionDb(
        owner="0xdeadbeef",
        granted_by="0xdeadbeef",
        address="0xbadbabe",
        type=PermissionType.POST.value,
        valid_from=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
        valid_until=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
        create=False,
        update=False,
        delete=True,
        post_types=None,
        channel=None,
    )


@pytest.fixture
def post_permission_do_everything() -> PostPermissionDb:
    return PostPermissionDb(
        owner="0xdeadbeef",
        granted_by="0xdeadbeef",
        address="0xbadbabe",
        type=PermissionType.POST.value,
        valid_from=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
        valid_until=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
        create=True,
        update=True,
        delete=True,
        post_types=None,
        channel=None,
    )


@pytest.mark.parametrize(
    "fixture_name", ["post_permission", "post_permission_do_everything"]
)
def test_permissions_extends(fixture_name: str, request):
    old_permission: BasePermissionDb = request.getfixturevalue(fixture_name)
    new_permission = copy.deepcopy(old_permission)

    new_permission.valid_from = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
    new_permission.valid_until = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
    assert new_permission.extends(old_permission)

    new_permission.valid_from = dt.datetime(2027, 1, 1, tzinfo=dt.timezone.utc)
    new_permission.valid_until = dt.datetime(2028, 1, 1, tzinfo=dt.timezone.utc)
    assert not new_permission.extends(old_permission)


@pytest.mark.parametrize(
    "fixture_name",
    [
        "post_permission",
        "post_permission_do_everything",
        "post_permission_delete_everything",
    ],
)
def test_permission_is_subset_self(fixture_name: str, request):
    permission: BasePermissionDb = request.getfixturevalue(fixture_name)
    assert permission.is_subset(permission)


def test_permission_is_subset_post(
    post_permission: PostPermissionDb,
    post_permission_do_everything: PostPermissionDb,
    post_permission_delete_everything: PostPermissionDb,
):
    assert post_permission.is_subset(post_permission_do_everything)
    assert not post_permission_do_everything.is_subset(post_permission)

    assert post_permission_delete_everything.is_subset(post_permission_do_everything)
    assert not post_permission_do_everything.is_subset(
        post_permission_delete_everything
    )
    assert not post_permission_delete_everything.is_subset(post_permission)
    assert not post_permission.is_subset(post_permission_delete_everything)
