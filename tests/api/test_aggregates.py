import itertools
from typing import Dict, Iterable, List

import aiohttp
import pytest

AGGREGATES_URI = "/api/v0/aggregates/{address}.json"

# Another address with three aggregates
ADDRESS_1 = "0x720F319A9c3226dCDd7D8C49163D79EDa1084E98"
# Another address with one aggregate
ADDRESS_2 = "0xaC033C1cA5C49Eff98A1D9a56BeDBC4840010BA4"

EXPECTED_AGGREGATES = {
    ADDRESS_1: {
        "test_key": {"a": 1, "b": 2},
        "test_target": {"a": 1, "b": 2},
        "test_reference": {"a": 1, "b": 2, "c": 3, "d": 4},
    },
    ADDRESS_2: {"test_reference": {"c": 3, "d": 4}},
}


def make_uri(address: str) -> str:
    return AGGREGATES_URI.format(address=address)


def assert_aggregates_equal(expected: List[Dict], actual: Dict[str, Dict]):
    for expected_aggregate in expected:
        aggregate = actual[expected_aggregate["content"]["key"]]
        assert "_id" not in aggregate

        assert aggregate == expected_aggregate["content"]["content"]


def merge_aggregates(messages: Iterable[Dict]) -> List[Dict]:
    def merge_content(_messages: List[Dict]) -> Dict:
        original = _messages[0]
        for update in _messages[1:]:
            original["content"]["content"].update(update["content"]["content"])
        return original

    aggregates = []

    for key, group in itertools.groupby(
            sorted(messages, key=lambda msg: msg["content"]["key"]),
            lambda msg: msg["content"]["key"],
    ):
        sorted_messages = sorted(group, key=lambda msg: msg["time"])
        aggregates.append(merge_content(sorted_messages))

    return aggregates


async def get_aggregates(api_client, address: str, **params) -> aiohttp.ClientResponse:
    return await api_client.get(make_uri(address), params=params)


async def get_aggregates_expect_success(api_client, address: str, **params):
    response = await get_aggregates(api_client, address, **params)
    assert response.status == 200, await response.text()
    return await response.json()


@pytest.fixture()
def fixture_aggregates(fixture_aggregate_messages):
    return merge_aggregates(fixture_aggregate_messages)


@pytest.mark.asyncio
async def test_get_aggregates_no_update(ccn_api_client, fixture_aggregates):
    """
    Tests receiving an aggregate from an address which posted one aggregate and never
    updated it.
    """

    address = ADDRESS_2
    aggregates = await get_aggregates_expect_success(ccn_api_client, address)

    assert aggregates["address"] == address
    assert aggregates["data"] == EXPECTED_AGGREGATES[address]


@pytest.mark.asyncio
async def test_get_aggregates(ccn_api_client, fixture_aggregates: List[Dict]):
    """
    A more complex case with 3 aggregates, one of which was updated.
    """

    address = ADDRESS_1
    aggregates = await get_aggregates_expect_success(ccn_api_client, address)

    assert address == aggregates["address"]
    assert aggregates["data"]["test_key"] == {"a": 1, "b": 2}
    assert aggregates["data"]["test_target"] == {"a": 1, "b": 2}
    assert aggregates["data"]["test_reference"] == {"a": 1, "b": 2, "c": 3, "d": 4}

    assert_aggregates_equal(fixture_aggregates, aggregates["data"])


@pytest.mark.asyncio
async def test_get_aggregates_filter_by_key(ccn_api_client, fixture_aggregates: List[Dict]):
    """
    Tests the 'keys' query parameter.
    """

    address, key = ADDRESS_1, "test_target"
    aggregates = await get_aggregates_expect_success(ccn_api_client, address=address, keys=key)
    assert aggregates["address"] == address
    assert aggregates["data"][key] == EXPECTED_AGGREGATES[address][key]

    # Multiple keys
    address, keys = ADDRESS_1, ["test_target", "test_reference"]
    aggregates = await get_aggregates_expect_success(ccn_api_client, address=address, keys=",".join(keys))
    assert aggregates["address"] == address
    for key in keys:
        assert aggregates["data"][key] == EXPECTED_AGGREGATES[address][key], f"Key {key} does not match"


@pytest.mark.asyncio
async def test_get_aggregates_limit(ccn_api_client, fixture_aggregates: List[Dict]):
    """
    Tests the 'limit' query parameter.
    """

    address, key = ADDRESS_1, "test_reference"
    aggregates = await get_aggregates_expect_success(ccn_api_client, address=address, keys=key, limit=1)
    assert aggregates["address"] == address
    assert aggregates["data"][key] == {"c": 3, "d": 4}


@pytest.mark.asyncio
async def test_get_aggregates_invalid_address(ccn_api_client, fixture_aggregates: List[Dict]):
    """
    Pass an unknown address.
    """

    invalid_address = "unknown"

    response = await get_aggregates(ccn_api_client, invalid_address)
    assert response.status == 404
