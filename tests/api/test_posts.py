from typing import Dict, Iterable, Sequence

import aiohttp
import pytest

from aleph.db.models.posts import PostDb

POSTS_URI = "/api/v0/posts.json"


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
