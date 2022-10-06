from typing import Dict, Iterable

import aiohttp
import pytest
from aleph_message.models import MessageType

from .utils import get_messages_by_keys

POSTS_URI = "/api/v0/posts.json"


def assert_posts_equal(posts: Iterable[Dict], expected_messages: Iterable[Dict]):
    posts_by_hash = {post["item_hash"]: post for post in posts}

    for expected_message in expected_messages:
        post = posts_by_hash[expected_message["item_hash"]]
        assert "_id" not in post

        assert post["chain"] == expected_message["chain"]
        assert post["channel"] == expected_message["channel"]
        assert post["sender"] == expected_message["sender"]
        assert post["signature"] == expected_message["signature"]

        if expected_message.get("forgotten_by", []):
            assert post["content"] is None
            continue

        if "content" not in expected_message["content"]:
            # TODO: there is a problem with the spec of posts: they can be specified
            #       without an internal "content" field, which does not break the
            #       endpoint but returns the content of message["content"] instead.
            #       We skip the issue for now.
            continue

        assert post["content"] == expected_message["content"]["content"]


async def get_posts(api_client, **params) -> aiohttp.ClientResponse:
    return await api_client.get(POSTS_URI, params=params)


async def get_posts_expect_success(api_client, **params):
    response = await get_posts(api_client, **params)
    assert response.status == 200, await response.text()
    data = await response.json()
    return data["posts"]


@pytest.mark.asyncio
async def test_get_posts(fixture_messages, ccn_api_client):
    # The POST messages in the fixtures file do not amend one another, so we should have
    # 1 POST = 1 message.
    post_messages = get_messages_by_keys(
        fixture_messages,
        type=MessageType.post,
    )
    posts = await get_posts_expect_success(ccn_api_client)

    assert_posts_equal(posts, post_messages)
