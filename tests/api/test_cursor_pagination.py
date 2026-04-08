import pytest

POSTS_V0_URI = "/api/v0/posts.json"
POSTS_V1_URI = "/api/v1/posts.json"


@pytest.mark.asyncio
async def test_posts_cursor_pagination(ccn_api_client, fixture_posts):
    """Cursor-based pagination walks through all posts without duplicates."""
    all_hashes = []
    cursor = ""

    for _ in range(10):  # safety limit
        response = await ccn_api_client.get(
            POSTS_V1_URI, params={"pagination": "2", "cursor": cursor}
        )
        assert response.status == 200, await response.text()
        data = await response.json()

        posts = data["posts"]
        all_hashes.extend(p["item_hash"] for p in posts)

        cursor = data.get("next_cursor")
        if cursor is None:
            break

    # No duplicates
    assert len(all_hashes) == len(set(all_hashes))
    # Got all posts
    assert len(all_hashes) == len(fixture_posts)


@pytest.mark.asyncio
async def test_posts_cursor_invalid_returns_422(ccn_api_client, fixture_posts):
    response = await ccn_api_client.get(
        POSTS_V1_URI, params={"cursor": "not-a-valid-cursor!!!"}
    )
    assert response.status == 422


@pytest.mark.asyncio
async def test_posts_cursor_pagination_zero_returns_422(ccn_api_client, fixture_posts):
    response = await ccn_api_client.get(
        POSTS_V1_URI, params={"cursor": "dummy", "pagination": "0"}
    )
    assert response.status == 422


@pytest.mark.asyncio
async def test_posts_v0_cursor_pagination(ccn_api_client, fixture_posts):
    """Cursor-based pagination works on v0 posts endpoint."""
    all_hashes = []
    cursor = ""

    for _ in range(10):
        response = await ccn_api_client.get(
            POSTS_V0_URI, params={"pagination": "2", "cursor": cursor}
        )
        assert response.status == 200, await response.text()
        data = await response.json()

        posts = data["posts"]
        all_hashes.extend(p["item_hash"] for p in posts)

        cursor = data.get("next_cursor")
        if cursor is None:
            break

    assert len(all_hashes) == len(set(all_hashes))
    assert len(all_hashes) == len(fixture_posts)


@pytest.mark.asyncio
async def test_files_cursor_invalid_returns_422(ccn_api_client):
    response = await ccn_api_client.get(
        "/api/v0/addresses/0xtest/files",
        params={"cursor": "not-valid!!!"},
    )
    assert response.status == 422


@pytest.mark.asyncio
async def test_credit_history_cursor_invalid_returns_422(ccn_api_client):
    response = await ccn_api_client.get(
        "/api/v0/addresses/0xtest/credit_history",
        params={"cursor": "bad-cursor"},
    )
    assert response.status == 422


@pytest.mark.asyncio
async def test_balances_cursor_invalid_returns_422(ccn_api_client):
    response = await ccn_api_client.get("/api/v0/balances", params={"cursor": "bad"})
    assert response.status == 422


@pytest.mark.asyncio
async def test_credit_balances_cursor_invalid_returns_422(ccn_api_client):
    response = await ccn_api_client.get(
        "/api/v0/credit_balances", params={"cursor": "bad"}
    )
    assert response.status == 422


AGGREGATES_URI = "/api/v0/aggregates.json"


@pytest.mark.asyncio
async def test_aggregates_cursor_pagination(ccn_api_client, fixture_aggregate_messages):
    """Cursor-based pagination walks through all aggregates."""
    all_keys = []
    cursor = ""
    params = {"pagination": "2"}

    for _ in range(10):
        params["cursor"] = cursor
        response = await ccn_api_client.get(AGGREGATES_URI, params=params)
        assert response.status == 200, await response.text()
        data = await response.json()

        aggregates = data["aggregates"]
        all_keys.extend((a["address"], a["key"]) for a in aggregates)

        cursor = data.get("next_cursor")
        if cursor is None:
            break

    assert len(all_keys) == len(set(all_keys))
    assert len(all_keys) > 0


ADDRESSES_STATS_URI = "/api/v1/addresses/stats.json"


@pytest.mark.asyncio
async def test_address_stats_cursor_pagination(
    ccn_api_client, fixture_address_stats_messages
):
    """Cursor pagination walks through all address stats."""
    all_addresses = []
    cursor = ""
    params = {"pagination": "1"}

    for _ in range(20):
        params["cursor"] = cursor
        response = await ccn_api_client.get(ADDRESSES_STATS_URI, params=params)
        assert response.status == 200, await response.text()
        data = await response.json()

        addresses = list(data["data"].keys())
        all_addresses.extend(addresses)

        cursor = data.get("next_cursor")
        if cursor is None:
            break

    assert len(all_addresses) == len(set(all_addresses))
    assert len(all_addresses) > 0


@pytest.mark.asyncio
async def test_address_stats_cursor_invalid_returns_422(ccn_api_client):
    response = await ccn_api_client.get(ADDRESSES_STATS_URI, params={"cursor": "bad"})
    assert response.status == 422
