from typing import Dict, Iterable, Sequence, Tuple

import aiohttp
import pytest

from aleph.db.models import MessageDb
from aleph.db.models.posts import PostDb
import datetime as dt

from aleph.types.db_session import DbSessionFactory

POSTS_URI = "/api/v1/posts.json"


def assert_posts_equal(posts: Iterable[Dict], expected_posts: Iterable[PostDb]):
    posts_by_hash = {post["item_hash"]: post for post in posts}

    for expected_post in expected_posts:
        post = posts_by_hash[expected_post.item_hash]
        assert "_id" not in post

        assert post["channel"] == expected_post.channel
        assert post["address"] == expected_post.owner
        assert post["original_type"] == expected_post.type
        assert post["ref"] == expected_post.ref

        # TODO: figure out what to do with this corner case
        # if "content" not in expected_message["content"]:
        #     # TODO: there is a problem with the spec of posts: they can be specified
        #     #       without an internal "content" field, which does not break the
        #     #       endpoint but returns the content of message["content"] instead.
        #     #       We skip the issue for now.
        #     continue

        assert post["content"] == expected_post.content


async def get_posts(api_client, **params) -> aiohttp.ClientResponse:
    return await api_client.get(POSTS_URI, params=params)


async def get_posts_expect_success(api_client, **params):
    response = await get_posts(api_client, **params)
    assert response.status == 200, await response.text()
    data = await response.json()
    return data["posts"]


@pytest.mark.asyncio
async def test_get_posts(ccn_api_client, fixture_posts: Sequence[PostDb]):

    # The POST messages in the fixtures file do not amend one another, so we should have
    # 1 POST = 1 message.
    posts = await get_posts_expect_success(ccn_api_client)
    assert_posts_equal(posts, fixture_posts)


@pytest.fixture
def post_with_refs_and_tags() -> Tuple[MessageDb, PostDb]:
    message = MessageDb(
        item_hash="1234",
        sender="0xdeadbeef",
        type="POST",
        chain="ETH",
        signature=None,
        item_type="storage",
        item_content=None,
        content={},
        time=dt.datetime(2023, 5, 1, tzinfo=dt.timezone.utc),
        channel=None,
        size=254,
    )

    post = PostDb(
        item_hash=message.item_hash,
        owner=message.sender,
        type=None,
        ref="custom-ref",
        amends=None,
        channel=None,
        content={"swap": "this"},
        creation_datetime=message.time,
        latest_amend=None,
    )

    return message, post


@pytest.fixture
def amended_post_with_refs_and_tags(post_with_refs_and_tags: Tuple[MessageDb, PostDb]):
    original_message, original_post = post_with_refs_and_tags

    amend_message = MessageDb(
        item_hash="5678",
        sender="0xdeadbeef",
        type="POST",
        chain="ETH",
        signature=None,
        item_type="storage",
        item_content=None,
        content={},
        time=dt.datetime(2023, 5, 2, tzinfo=dt.timezone.utc),
        channel=None,
        size=277,
    )

    amend_post = PostDb(
        item_hash=amend_message.item_hash,
        owner=original_message.sender,
        type="amend",
        ref=original_message.item_hash,
        amends=original_message.item_hash,
        channel=None,
        content={"don't": "swap"},
        creation_datetime=amend_message.time,
        latest_amend=None,
    )

    return amend_message, amend_post


@pytest.mark.asyncio
async def test_get_posts_refs(
    ccn_api_client,
    session_factory: DbSessionFactory,
    fixture_posts: Sequence[PostDb],
    post_with_refs_and_tags: Tuple[MessageDb, PostDb],
):
    message_db, post_db = post_with_refs_and_tags

    with session_factory() as session:
        session.add_all(fixture_posts)
        session.add(message_db)
        session.add(post_db)
        session.commit()

    # Match the ref
    response = await ccn_api_client.get(
        "/api/v0/posts.json", params={"refs": f"{post_db.ref}"}
    )
    assert response.status == 200
    response_json = await response.json()
    assert len(response_json["posts"]) == 1
    assert response_json["pagination_total"] == 1

    post = response_json["posts"][0]
    assert post["item_hash"] == post_db.item_hash
    assert post["original_item_hash"] == post_db.item_hash
    assert post["ref"] == post_db.ref
    assert post["content"] == post_db.content

    # Unknown ref
    response = await ccn_api_client.get(
        "/api/v0/posts.json", params={"refs": "not-a-ref"}
    )
    assert response.status == 200
    response_json = await response.json()
    assert len(response_json["posts"]) == 0
    assert response_json["pagination_total"] == 0

    # Search for several refs
    response = await ccn_api_client.get(
        "/api/v0/posts.json", params={"refs": f"{post_db.ref},not-a-ref"}
    )
    assert response.status == 200
    response_json = await response.json()
    assert len(response_json["posts"]) == 1
    assert response_json["pagination_total"] == 1

    post = response_json["posts"][0]
    assert post["item_hash"] == post_db.item_hash
    assert post["original_item_hash"] == post_db.item_hash
    assert post["ref"] == post_db.ref
    assert post["content"] == post_db.content


@pytest.mark.asyncio
async def test_get_amended_posts_refs(
    ccn_api_client,
    session_factory: DbSessionFactory,
    fixture_posts: Sequence[PostDb],
    post_with_refs_and_tags: Tuple[MessageDb, PostDb],
    amended_post_with_refs_and_tags: Tuple[MessageDb, PostDb],
):
    original_message_db, original_post_db = post_with_refs_and_tags
    amend_message_db, amend_post_db = amended_post_with_refs_and_tags

    original_post_db.latest_amend = amend_post_db.item_hash

    with session_factory() as session:
        session.add_all(fixture_posts)
        session.add(original_message_db)
        session.add(original_post_db)
        session.add(amend_message_db)
        session.add(amend_post_db)
        session.commit()

    # Match the ref
    response = await ccn_api_client.get(
        "/api/v0/posts.json", params={"refs": f"{original_post_db.ref}"}
    )
    assert response.status == 200
    response_json = await response.json()
    assert len(response_json["posts"]) == 1
    assert response_json["pagination_total"] == 1

    post = response_json["posts"][0]
    assert post["item_hash"] == amend_post_db.item_hash
    assert post["original_item_hash"] == original_post_db.item_hash
    assert post["ref"] == original_post_db.ref
    assert post["content"] == amend_post_db.content

    # Unknown ref
    response = await ccn_api_client.get(
        "/api/v0/posts.json", params={"refs": "not-a-ref"}
    )
    assert response.status == 200
    response_json = await response.json()
    assert len(response_json["posts"]) == 0
    assert response_json["pagination_total"] == 0

    # Search for several refs
    response = await ccn_api_client.get(
        "/api/v0/posts.json", params={"refs": f"{original_post_db.ref},not-a-ref"}
    )
    assert response.status == 200
    response_json = await response.json()
    assert len(response_json["posts"]) == 1
    assert response_json["pagination_total"] == 1

    post = response_json["posts"][0]
    assert post["item_hash"] == amend_post_db.item_hash
    assert post["original_item_hash"] == original_post_db.item_hash
    assert post["ref"] == original_post_db.ref
    assert post["content"] == amend_post_db.content
