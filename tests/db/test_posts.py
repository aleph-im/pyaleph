import datetime as dt
from typing import Optional, Any, Dict

import pytest
import pytz
from aleph_message.models import ItemHash, ItemType, MessageType, Chain
from more_itertools import one

from aleph.db.accessors.posts import (
    get_post,
    MergedPost,
    get_matching_posts,
    count_matching_posts,
    refresh_latest_amend,
    get_original_post,
    delete_post,
    get_matching_posts_legacy,
    MergedPostV0,
)
from aleph.db.models import MessageDb
from aleph.db.models.posts import PostDb
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory
from aleph.types.sort_order import SortOrder
import json


def message_fields_from_post(post: PostDb) -> Dict[str, Any]:
    item_content = json.dumps(post.content)

    return {
        "item_hash": post.item_hash,
        "type": MessageType.post,
        "sender": post.owner,
        "item_type": ItemType.inline,
        "item_content": item_content,
        "content": post.content,
        "time": post.creation_datetime,
        "size": len(item_content),
    }


@pytest.fixture
def original_post() -> PostDb:
    item_hash = ItemHash(
        "285e8bce91cdcf595b711687906f35e53a61198da9df56b4f86e5686151ee41d"
    )
    post = PostDb(
        item_hash=item_hash,
        owner="0xabadbabe",
        type="my-post-type",
        ref=None,
        amends=None,
        channel=Channel("MY-CHANNEL"),
        content={"body": "Hello, world!"},
        creation_datetime=pytz.utc.localize(dt.datetime(2022, 11, 11, 11, 11, 11)),
    )
    return post


@pytest.fixture
def original_message(original_post: PostDb) -> MessageDb:
    return MessageDb(
        **message_fields_from_post(original_post),
        chain=Chain.ETH,
        signature="sig-original"
    )


@pytest.fixture
def first_amend_post(original_post: PostDb):
    item_hash = "cf315f88a3ab02a49df43114463ded42c266aa32c0168ee2bf231fda9e930ffb"
    amend = PostDb(
        item_hash=item_hash,
        owner="0xabadbabe",
        type="amend",
        ref=original_post.item_hash,
        amends=original_post.item_hash,
        channel=original_post.channel,
        content={"body": "Goodbye blue sky"},
        creation_datetime=pytz.utc.localize(dt.datetime(2022, 12, 6)),
    )
    return amend


@pytest.fixture
def first_amend_message(first_amend_post: PostDb) -> MessageDb:
    return MessageDb(
        **message_fields_from_post(first_amend_post),
        chain=Chain.ETH,
        signature="sig-first-amend"
    )


@pytest.fixture
def second_amend_post(original_post: PostDb):
    item_hash = "5189605b437e0a8808acb318f0fd3e73b3682c44a3f93e2d4d58c35c03caf2a2"
    amend = PostDb(
        item_hash=item_hash,
        owner="0xabadbabe",
        type="amend",
        ref=original_post.item_hash,
        amends=original_post.item_hash,
        channel=original_post.channel,
        content={"body": "Gutentag!"},
        creation_datetime=pytz.utc.localize(dt.datetime(2022, 12, 25)),
    )
    return amend


@pytest.fixture
def second_amend_message(second_amend_post: PostDb) -> MessageDb:
    return MessageDb(
        **message_fields_from_post(second_amend_post),
        chain=Chain.ETH,
        signature="sig-second-amend"
    )


@pytest.fixture
def post_from_second_user() -> PostDb:
    item_hash = ItemHash(
        "4fc575ac98f3c69e543792758f62fc0c4d3a1b422281a7425e545d979012c065"
    )
    post = PostDb(
        item_hash=item_hash,
        owner="0xdeadbabe",
        type="great-posts",
        ref=None,
        amends=None,
        channel=Channel("ALEPH-POSTS"),
        content={"body": "You're my favorite customer"},
        creation_datetime=pytz.utc.localize(dt.datetime(2022, 10, 12)),
    )
    return post


@pytest.fixture
def message_from_second_user(post_from_second_user: PostDb) -> MessageDb:
    return MessageDb(
        **message_fields_from_post(post_from_second_user),
        chain=Chain.ETH,
        signature="sig-post-from-second-user"
    )


def assert_posts_equal(
    merged_post: MergedPost, original: PostDb, last_amend: Optional[PostDb] = None
):
    expected_item_hash = last_amend.item_hash if last_amend else original.item_hash
    expected_content = last_amend.content if last_amend else original.content
    expected_last_updated = (
        last_amend.creation_datetime if last_amend else original.creation_datetime
    )

    assert merged_post.item_hash == expected_item_hash
    assert merged_post.original_item_hash == original.item_hash
    assert merged_post.owner == original.owner
    assert merged_post.ref == original.ref
    assert merged_post.channel == original.channel
    assert merged_post.content == expected_content
    assert merged_post.last_updated == expected_last_updated
    assert merged_post.created == original.creation_datetime


def assert_posts_v0_equal(
    merged_post: MergedPostV0,
    original: PostDb,
    original_message: MessageDb,
    last_amend: Optional[PostDb] = None,
    amend_message: Optional[MessageDb] = None,
):
    expected_item_hash = last_amend.item_hash if last_amend else original.item_hash
    expected_content = last_amend.content if last_amend else original.content
    expected_ref = last_amend.ref if last_amend else original.ref
    expected_last_updated = (
        last_amend.creation_datetime if last_amend else original.creation_datetime
    )

    assert merged_post.item_hash == expected_item_hash
    assert merged_post.original_item_hash == original.item_hash
    assert merged_post.owner == original.owner
    assert merged_post.ref == expected_ref
    assert merged_post.channel == original.channel
    assert merged_post.content == expected_content
    assert merged_post.time == expected_last_updated.timestamp()

    assert merged_post.original_signature == original_message.signature

    if amend_message:
        assert merged_post.signature == amend_message.signature
    else:
        assert merged_post.signature == original_message.signature


@pytest.mark.asyncio
async def test_get_post_no_amend(
    original_post: PostDb, session_factory: DbSessionFactory
):
    """
    Checks that getting a post without amends works.
    """
    with session_factory() as session:
        session.add(original_post)
        session.commit()

    with session_factory() as session:
        post = get_post(session=session, item_hash=original_post.item_hash)
        assert post
        assert_posts_equal(merged_post=post, original=original_post)


@pytest.mark.asyncio
async def test_get_post_with_one_amend(
    original_post: PostDb, first_amend_post: PostDb, session_factory: DbSessionFactory
):
    """
    Checks that getting an amended post will return the amend and not the original.
    """
    with session_factory() as session:
        session.add(original_post)
        session.add(first_amend_post)
        original_post.latest_amend = first_amend_post.item_hash
        session.commit()

    with session_factory() as session:
        post = get_post(session=session, item_hash=original_post.item_hash)
        assert post
        assert_posts_equal(
            merged_post=post, original=original_post, last_amend=first_amend_post
        )

        # Check that the query will not return a result when addressing the amend hash
        amend_post = get_post(session=session, item_hash=first_amend_post.item_hash)
        assert amend_post is None


@pytest.mark.asyncio
async def test_get_post_with_two_amends(
    original_post: PostDb,
    first_amend_post: PostDb,
    second_amend_post,
    session_factory: DbSessionFactory,
):
    """
    Checks that getting a post amended twice will return the latest amend.
    """
    with session_factory() as session:
        session.add(original_post)
        session.add(first_amend_post)
        session.add(second_amend_post)
        original_post.latest_amend = second_amend_post.item_hash
        session.commit()

    with session_factory() as session:
        post = get_post(session=session, item_hash=original_post.item_hash)
        assert post
        assert_posts_equal(
            merged_post=post, original=original_post, last_amend=second_amend_post
        )


@pytest.mark.asyncio
async def test_get_matching_posts(
    original_post: PostDb,
    first_amend_post: PostDb,
    post_from_second_user: PostDb,
    session_factory: DbSessionFactory,
):
    """
    Tests that the list getter works.
    """

    with session_factory() as session:
        session.add(original_post)
        session.add(first_amend_post)
        original_post.latest_amend = first_amend_post.item_hash
        session.add(post_from_second_user)
        session.commit()

    with session_factory() as session:
        # Get all posts, no filter
        matching_posts = get_matching_posts(session=session)
        assert len(matching_posts) == 2
        nb_posts = count_matching_posts(session=session)
        assert nb_posts == 2

        # Get by hash
        matching_hash_posts = get_matching_posts(
            session=session, hashes=[original_post.item_hash]
        )
        assert matching_hash_posts
        assert_posts_equal(
            merged_post=one(matching_hash_posts),
            original=original_post,
            last_amend=first_amend_post,
        )
        nb_matching_hash_posts = count_matching_posts(
            session=session, hashes=[original_post.item_hash]
        )
        assert nb_matching_hash_posts == 1

        # Get by owner address
        matching_address_posts = get_matching_posts(
            session=session, addresses=[post_from_second_user.owner]
        )
        assert matching_address_posts
        assert_posts_equal(
            merged_post=one(matching_address_posts), original=post_from_second_user
        )
        nb_matching_address_posts = count_matching_posts(
            session=session, addresses=[post_from_second_user.owner]
        )
        assert nb_matching_address_posts == 1

        # Get by channel
        matching_channel_posts = get_matching_posts(
            session=session, channels=[post_from_second_user.channel]
        )
        assert matching_channel_posts
        assert_posts_equal(
            merged_post=one(matching_channel_posts), original=post_from_second_user
        )
        nb_matching_channel_posts = count_matching_posts(
            session=session, channels=[post_from_second_user.channel]
        )
        assert nb_matching_channel_posts == 1


@pytest.mark.asyncio
async def test_get_matching_posts_legacy(
    original_post: PostDb,
    original_message: MessageDb,
    first_amend_post: PostDb,
    first_amend_message: MessageDb,
    post_from_second_user: PostDb,
    message_from_second_user: MessageDb,
    session_factory: DbSessionFactory,
):
    """
    Tests that the list getter works with the legacy format. Same test logic as the test above.
    """

    with session_factory() as session:
        session.add_all(
            [original_message, first_amend_message, message_from_second_user]
        )
        session.add(original_post)
        session.add(first_amend_post)
        original_post.latest_amend = first_amend_post.item_hash
        session.add(post_from_second_user)
        session.commit()

    with session_factory() as session:
        # Get all posts, no filter
        matching_posts = get_matching_posts_legacy(session=session)
        assert len(matching_posts) == 2
        nb_posts = count_matching_posts(session=session)
        assert nb_posts == 2

        # Get by hash
        matching_hash_posts = get_matching_posts_legacy(
            session=session, hashes=[original_post.item_hash]
        )
        assert matching_hash_posts
        assert_posts_v0_equal(
            merged_post=one(matching_hash_posts),
            original=original_post,
            original_message=original_message,
            last_amend=first_amend_post,
            amend_message=first_amend_message,
        )
        nb_matching_hash_posts = count_matching_posts(
            session=session, hashes=[original_post.item_hash]
        )
        assert nb_matching_hash_posts == 1

        # Get by owner address
        matching_address_posts = get_matching_posts_legacy(
            session=session, addresses=[post_from_second_user.owner]
        )
        assert matching_address_posts
        assert_posts_v0_equal(
            merged_post=one(matching_address_posts),
            original=post_from_second_user,
            original_message=message_from_second_user,
        )
        nb_matching_address_posts = count_matching_posts(
            session=session, addresses=[post_from_second_user.owner]
        )
        assert nb_matching_address_posts == 1

        # Get by channel
        matching_channel_posts = get_matching_posts_legacy(
            session=session, channels=[post_from_second_user.channel]
        )
        assert matching_channel_posts
        assert_posts_v0_equal(
            merged_post=one(matching_channel_posts),
            original=post_from_second_user,
            original_message=message_from_second_user,
        )
        nb_matching_channel_posts = count_matching_posts(
            session=session, channels=[post_from_second_user.channel]
        )
        assert nb_matching_channel_posts == 1


@pytest.mark.asyncio
async def test_get_matching_posts_time_filters(
    original_post: PostDb,
    first_amend_post: PostDb,
    post_from_second_user: PostDb,
    session_factory: DbSessionFactory,
):
    """
    Tests that the time filters for the list getter work.
    """

    with session_factory() as session:
        session.add(original_post)
        session.add(first_amend_post)
        original_post.latest_amend = first_amend_post.item_hash
        session.add(post_from_second_user)
        session.commit()

    with session_factory() as session:
        start_datetime = first_amend_post.creation_datetime
        end_datetime = start_datetime + dt.timedelta(days=1)
        # Sanity check, the amend is supposed to be the latest entry
        assert start_datetime > post_from_second_user.creation_datetime
        matching_posts = get_matching_posts(
            session=session, start_date=start_datetime, end_date=end_datetime
        )
        assert matching_posts
        assert_posts_equal(
            merged_post=one(matching_posts),
            original=original_post,
            last_amend=first_amend_post,
        )


@pytest.mark.asyncio
async def test_get_matching_posts_sort_order(
    original_post: PostDb,
    first_amend_post: PostDb,
    post_from_second_user: PostDb,
    session_factory: DbSessionFactory,
):
    """
    Tests that the sort order specifier for the list getter work.
    """

    with session_factory() as session:
        session.add(original_post)
        session.add(first_amend_post)
        original_post.latest_amend = first_amend_post.item_hash
        session.add(post_from_second_user)
        session.commit()

    with session_factory() as session:
        # Ascending order first
        asc_posts = get_matching_posts(session=session, sort_order=SortOrder.ASCENDING)
        assert asc_posts
        assert_posts_equal(merged_post=asc_posts[0], original=post_from_second_user)
        assert_posts_equal(
            merged_post=asc_posts[1],
            original=original_post,
            last_amend=first_amend_post,
        )

        # Descending order first
        asc_posts = get_matching_posts(session=session, sort_order=SortOrder.DESCENDING)
        assert asc_posts
        assert_posts_equal(
            merged_post=asc_posts[0],
            original=original_post,
            last_amend=first_amend_post,
        )
        assert_posts_equal(merged_post=asc_posts[1], original=post_from_second_user)


@pytest.mark.asyncio
async def test_get_matching_posts_no_data(
    session_factory: DbSessionFactory,
):
    """
    Tests that the list getter works when a node starts syncing.
    """

    with session_factory() as session:
        posts = list(get_matching_posts(session=session))
    assert posts == []


@pytest.mark.asyncio
async def test_refresh_latest_amend(
    session_factory: DbSessionFactory,
    original_post: PostDb,
    first_amend_post: PostDb,
    second_amend_post: PostDb,
):
    with session_factory() as session:
        session.add(original_post)
        session.add(first_amend_post)
        session.add(second_amend_post)
        session.commit()

    with session_factory() as session:
        refresh_latest_amend(session, original_post.item_hash)
        session.commit()

        original_post_db = get_original_post(session, item_hash=original_post.item_hash)
        assert original_post_db
        assert original_post_db.latest_amend == second_amend_post.item_hash

    # Now, delete the second post and check that refreshing the latest amend works
    with session_factory() as session:
        delete_post(session, item_hash=second_amend_post.item_hash)
        refresh_latest_amend(session=session, item_hash=original_post.item_hash)
        session.commit()

        original_post_db = get_original_post(session, item_hash=original_post.item_hash)
        assert original_post_db
        assert original_post_db.latest_amend == first_amend_post.item_hash

    # Delete the last amend, check that latest_amend is now null
    with session_factory() as session:
        delete_post(session, item_hash=first_amend_post.item_hash)
        refresh_latest_amend(session=session, item_hash=original_post.item_hash)
        session.commit()

        original_post_db = get_original_post(session, item_hash=original_post.item_hash)
        assert original_post_db
        assert original_post_db.latest_amend is None
