import datetime as dt
import itertools
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union, Tuple

import aiohttp
import pytest

from .utils import get_messages_by_keys

MESSAGES_URI = "/api/v0/messages.json"
MESSAGES_PAGE_URI = "/api/v0/messages/page/{page}.json"


def check_message_fields(messages: Iterable[Dict]):
    """
    Basic checks on fields. For example, check that we do not expose internal data
    like MongoDB object IDs.
    """
    for message in messages:
        assert "_id" not in message


def assert_messages_equal(messages: Iterable[Dict], expected_messages: Iterable[Dict]):
    messages_by_hash = {msg["item_hash"]: msg for msg in messages}

    for expected_message in expected_messages:
        message = messages_by_hash[expected_message["item_hash"]]

        assert message["chain"] == expected_message["chain"]
        assert message["channel"] == expected_message["channel"]
        for key, value in expected_message["content"].items():
            assert message["content"][key] == value
        assert message["item_content"] == expected_message["item_content"]
        assert message["sender"] == expected_message["sender"]
        assert message["signature"] == expected_message["signature"]


@pytest.mark.asyncio
async def test_get_messages(fixture_messages: Sequence[Dict[str, Any]], ccn_api_client):
    response = await ccn_api_client.get(MESSAGES_URI)
    assert response.status == 200, await response.text()

    data = await response.json()

    messages = data["messages"]
    assert len(messages) == len(fixture_messages)
    check_message_fields(messages)
    assert_messages_equal(messages, fixture_messages)

    assert data["pagination_total"] == len(messages)
    assert data["pagination_page"] == 1


@pytest.mark.asyncio
async def test_get_messages_filter_by_channel(fixture_messages, ccn_api_client):
    async def fetch_messages_by_channel(channel: str) -> Dict:
        response = await ccn_api_client.get(MESSAGES_URI, params={"channels": channel})
        assert response.status == 200, await response.text()
        return await response.json()

    data = await fetch_messages_by_channel("unit-tests")
    messages = data["messages"]

    unit_test_messages = get_messages_by_keys(fixture_messages, channel="unit-tests")

    assert len(messages) == len(unit_test_messages)
    assert_messages_equal(messages, unit_test_messages)

    data = await fetch_messages_by_channel("aggregates-tests")
    messages = data["messages"]

    aggregates_test_messages = get_messages_by_keys(
        fixture_messages, channel="aggregates-tests"
    )
    assert_messages_equal(messages, aggregates_test_messages)

    # Multiple channels
    data = await fetch_messages_by_channel("aggregates-tests,unit-tests")
    messages = data["messages"]

    assert_messages_equal(
        messages, itertools.chain(unit_test_messages, aggregates_test_messages)
    )

    # Nonexistent channel
    data = await fetch_messages_by_channel("none-pizza-with-left-beef")
    assert data["messages"] == []


async def fetch_messages_by_chain(api_client, chain: str) -> aiohttp.ClientResponse:
    response = await api_client.get(MESSAGES_URI, params={"chains": chain})
    return response


@pytest.mark.asyncio
async def test_get_messages_filter_by_chain(fixture_messages, ccn_api_client):
    response = await fetch_messages_by_chain(api_client=ccn_api_client, chain="ETH")
    assert response.status == 200, await response.text()

    eth_data = await response.json()
    eth_messages = eth_data["messages"]
    assert_messages_equal(
        eth_messages, get_messages_by_keys(fixture_messages, chain="ETH")
    )


@pytest.mark.asyncio
async def test_get_messages_filter_invalid_chain(fixture_messages, ccn_api_client):
    response = await fetch_messages_by_chain(api_client=ccn_api_client, chain="2CHAINZ")
    assert response.status == 422, await response.text()


async def fetch_messages_by_content_hash(
    api_client, item_hash: str
) -> aiohttp.ClientResponse:
    response = await api_client.get(MESSAGES_URI, params={"contentHashes": item_hash})
    return response


@pytest.mark.asyncio
async def test_get_messages_filter_by_content_hash(fixture_messages, ccn_api_client):
    content_hash = "5ccdd7bccfbc5955e2e40166dd0cdea0b093154fd87bc2bea57e7c768cde2f21"
    response = await fetch_messages_by_content_hash(ccn_api_client, content_hash)
    assert response.status == 200, await response.text()
    data = await response.json()

    messages = data["messages"]
    assert_messages_equal(
        messages,
        get_messages_by_keys(
            fixture_messages,
            item_hash="2953f0b52beb79fc0ed1bc455346fdcb530611605e16c636778a0d673d7184af",
        ),
    )


@pytest.mark.asyncio
async def test_get_messages_multiple_hashes(fixture_messages, ccn_api_client):
    hashes = [
        "2953f0b52beb79fc0ed1bc455346fdcb530611605e16c636778a0d673d7184af",
        "bc411ae2ba89289458d0168714457e7c9394a29ca83159240585591f4f46444a",
    ]
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"hashes": ",".join(hashes)}
    )
    assert response.status == 200, await response.text()
    messages = (await response.json())["messages"]
    assert len(messages) == 2


@pytest.mark.asyncio
async def test_get_messages_filter_by_invalid_content_hash(
    fixture_messages, ccn_api_client
):
    response = await fetch_messages_by_content_hash(ccn_api_client, "1234")
    assert response.status == 422, await response.text()


async def fetch_messages_filter_time(
    api_client,
    start: Optional[float] = None,
    end: Optional[float] = None,
    sort_by: str = "tx-time",
    sort_order: int = -1,
) -> aiohttp.ClientResponse:

    params: Dict[str, Union[float, int, str]] = {
        "sort_by": sort_by,
        "sort_order": sort_order,
    }
    if start:
        params["startDate"] = start
    if end:
        params["endDate"] = end

    return await api_client.get(MESSAGES_URI, params=params)


async def fetch_messages_filter_time_expect_success(
    api_client,
    start: Optional[float] = None,
    end: Optional[float] = None,
    sort_by: str = "tx-time",
    sort_order: int = -1,
) -> List[Dict]:
    response = await fetch_messages_filter_time(
        api_client, start=start, end=end, sort_order=sort_order
    )
    print(await response.text())
    assert response.status == 200, await response.text()
    data = await response.json()
    return data["messages"]


@pytest.mark.asyncio
async def test_time_filters(fixture_messages, ccn_api_client):
    # Start and end time specified, should return all messages
    start_time, end_time = 1648215900, 1652126600
    messages = await fetch_messages_filter_time_expect_success(
        ccn_api_client, start=start_time, end=end_time
    )
    assert_messages_equal(
        messages=messages,
        expected_messages=filter(
            lambda msg: start_time <= msg["time"] < end_time,
            fixture_messages,
        ),
    )

    # Only a start time
    messages = await fetch_messages_filter_time_expect_success(
        ccn_api_client, start=start_time
    )
    assert_messages_equal(
        messages=messages,
        expected_messages=filter(
            lambda msg: msg["time"] >= start_time,
            fixture_messages,
        ),
    )

    # Only an end time
    messages = await fetch_messages_filter_time_expect_success(
        ccn_api_client, end=end_time
    )
    assert_messages_equal(
        messages=messages,
        expected_messages=filter(
            lambda msg: msg["time"] < end_time,
            fixture_messages,
        ),
    )

    # Change the default order (ascending instead of descending)
    messages = await fetch_messages_filter_time_expect_success(
        ccn_api_client, start=start_time, end=end_time, sort_order=1
    )
    assert_messages_equal(
        messages=messages,
        expected_messages=filter(
            lambda msg: start_time <= msg["time"] < end_time,
            fixture_messages,
        ),
    )

    # End time lower than start time
    response = await fetch_messages_filter_time(
        ccn_api_client, start=end_time, end=start_time
    )
    assert response.status == 422

    # Negative value
    response = await fetch_messages_filter_time(ccn_api_client, start=-8000)
    assert response.status == 422

    response = await fetch_messages_filter_time(ccn_api_client, end=-700)
    assert response.status == 422

    # Non-string value
    response = await fetch_messages_filter_time(ccn_api_client, start="yes")
    assert response.status == 422


async def fetch_messages_with_pagination(
    api_client, page: int = 1, pagination: int = 20, sort_order: int = -1
):
    return await api_client.get(
        MESSAGES_URI,
        params={"page": page, "pagination": pagination, "sort_order": sort_order},
    )


async def fetch_messages_with_pagination_expect_success(
    api_client, page: int = 1, pagination: int = 20, sort_order: int = -1
):
    response = await fetch_messages_with_pagination(
        api_client, page, pagination, sort_order
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    return data["messages"]


@pytest.mark.asyncio()
async def test_pagination(fixture_messages, ccn_api_client):
    sorted_messages_by_time = sorted(fixture_messages, key=lambda msg: msg["time"])

    # More messages than available
    messages = await fetch_messages_with_pagination_expect_success(
        ccn_api_client, page=1, pagination=len(fixture_messages) + 1
    )
    assert_messages_equal(messages=messages, expected_messages=fixture_messages)

    # All the messages
    messages = await fetch_messages_with_pagination_expect_success(
        ccn_api_client, page=1, pagination=0
    )
    assert_messages_equal(messages=messages, expected_messages=fixture_messages)

    # Only some messages
    messages = await fetch_messages_with_pagination_expect_success(
        ccn_api_client, page=1, pagination=4
    )
    assert_messages_equal(messages, sorted_messages_by_time[-4:])

    # Second page
    messages = await fetch_messages_with_pagination_expect_success(
        ccn_api_client, page=2, pagination=4
    )
    assert_messages_equal(messages, sorted_messages_by_time[-8:-4])

    # Only one message
    messages = await fetch_messages_with_pagination_expect_success(
        ccn_api_client, page=1, pagination=1
    )
    assert_messages_equal(messages, sorted_messages_by_time[-1:])

    # Some messages, reverse sort order
    messages = await fetch_messages_with_pagination_expect_success(
        ccn_api_client, page=1, pagination=3, sort_order=1
    )
    assert_messages_equal(messages, sorted_messages_by_time[:3])

    # Several pages too far
    messages = await fetch_messages_with_pagination_expect_success(
        ccn_api_client, page=1000, pagination=4, sort_order=1
    )
    assert messages == []

    # With the /page/{page} endpoint
    response = await ccn_api_client.get(
        MESSAGES_PAGE_URI.format(page=2), params={"pagination": 4}
    )
    assert response.status == 200, await response.text()
    messages = (await response.json())["messages"]
    assert_messages_equal(messages, sorted_messages_by_time[-8:-4])

    # Page 0
    response = await fetch_messages_with_pagination(ccn_api_client, page=0)
    assert response.status == 422

    # Negative page
    response = await fetch_messages_with_pagination(ccn_api_client, page=-3)
    assert response.status == 422

    # Negative pagination
    response = await fetch_messages_with_pagination(ccn_api_client, pagination=-10)
    assert response.status == 422


@pytest.mark.asyncio()
@pytest.mark.parametrize("sort_order", [-1, 1])
async def test_sort_by_tx_time(fixture_messages, ccn_api_client, sort_order: int):
    def get_confirmed_time(msg: Dict) -> Tuple[float, float]:
        try:
            return msg["confirmations"][0]["time"], msg["time"]
        except KeyError:
            # Return a high timestamp to make unconfirmed messages last
            return dt.datetime(3000, 1, 1).timestamp(), msg["time"]

    sorted_messages_by_time = sorted(fixture_messages, key=get_confirmed_time)

    # Descending order
    messages = await fetch_messages_filter_time_expect_success(
        ccn_api_client, sort_by="tx-time", sort_order=sort_order
    )
    assert_messages_equal(messages=messages, expected_messages=sorted_messages_by_time)

    if sort_order == -1:
        # Note: list() is used for mypy
        sorted_messages_by_time = list(reversed(sorted_messages_by_time))

    assert [msg["item_hash"] for msg in messages] == [
        msg["item_hash"] for msg in sorted_messages_by_time
    ]
