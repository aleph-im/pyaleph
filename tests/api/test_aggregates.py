from typing import Sequence

import aiohttp
import pytest

from aleph.db.models import MessageDb

AGGREGATES_URI = "/api/v0/aggregates/{address}.json"

# An address with three aggregates
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


async def get_aggregates(
    api_client, address: str, with_info=False, **params
) -> aiohttp.ClientResponse:
    params["with_info"] = str(with_info)
    return await api_client.get(make_uri(address), params=params)


async def get_aggregates_expect_success(api_client, address: str, **params):
    response = await get_aggregates(api_client, address, True, **params)
    assert response.status == 200, await response.text()
    return await response.json()


@pytest.mark.asyncio
async def test_get_aggregates_no_update(
    ccn_api_client, fixture_aggregate_messages: Sequence[MessageDb]
):
    """
    Tests receiving an aggregate from an address which posted one aggregate and never
    updated it.
    """
    assert fixture_aggregate_messages  # To avoid unused parameter warnings

    address = ADDRESS_2
    aggregates = await get_aggregates_expect_success(ccn_api_client, address)

    assert aggregates["address"] == address
    assert aggregates["data"] == EXPECTED_AGGREGATES[address]


@pytest.mark.asyncio
async def test_get_aggregates(
    ccn_api_client, fixture_aggregate_messages: Sequence[MessageDb]
):
    """
    A more complex case with 3 aggregates, one of which was updated.
    """
    assert fixture_aggregate_messages  # To avoid unused parameter warnings

    address = ADDRESS_1
    aggregates = await get_aggregates_expect_success(ccn_api_client, address)

    assert address == aggregates["address"]
    assert aggregates["data"]["test_key"] == {"a": 1, "b": 2}
    assert aggregates["data"]["test_target"] == {"a": 1, "b": 2}
    assert aggregates["data"]["test_reference"] == {"a": 1, "b": 2, "c": 3, "d": 4}
    assert aggregates["info"]["test_reference"] is not None


@pytest.mark.asyncio
async def test_get_aggregates_filter_by_key(
    ccn_api_client, fixture_aggregate_messages: Sequence[MessageDb]
):
    """
    Tests the 'keys' query parameter.
    """

    assert fixture_aggregate_messages  # To avoid unused parameter warnings

    address, key = ADDRESS_1, "test_target"
    aggregates = await get_aggregates_expect_success(
        ccn_api_client, address=address, keys=key
    )
    assert aggregates["address"] == address
    assert aggregates["data"][key] == EXPECTED_AGGREGATES[address][key]

    # Multiple keys
    address, keys = ADDRESS_1, ["test_target", "test_reference"]
    aggregates = await get_aggregates_expect_success(
        ccn_api_client, address=address, keys=",".join(keys)
    )
    assert aggregates["address"] == address
    for key in keys:
        assert (
            aggregates["data"][key] == EXPECTED_AGGREGATES[address][key]
        ), f"Key {key} does not match"


@pytest.mark.skip(
    "This test does not make any sense anymore, we do not want to limit aggregates?"
)
@pytest.mark.asyncio
async def test_get_aggregates_limit(
    ccn_api_client, fixture_aggregate_messages: Sequence[MessageDb]
):
    """
    Tests the 'limit' query parameter.
    """
    assert fixture_aggregate_messages  # To avoid unused parameter warnings

    address, key = ADDRESS_1, "test_reference"
    aggregates = await get_aggregates_expect_success(
        ccn_api_client, address=address, keys=key, limit=1
    )
    assert aggregates["address"] == address
    assert aggregates["data"][key] == {"c": 3, "d": 4}


@pytest.mark.asyncio
async def test_get_aggregates_invalid_address(
    ccn_api_client, fixture_aggregate_messages: Sequence[MessageDb]
):
    """
    Pass an unknown address.
    """
    assert fixture_aggregate_messages  # To avoid unused parameter warnings

    invalid_address = "unknown"

    response = await get_aggregates(ccn_api_client, invalid_address)
    assert response.status == 404


@pytest.mark.asyncio
async def test_get_aggregates_invalid_params(
    ccn_api_client, fixture_aggregate_messages: Sequence[MessageDb]
):
    """
    Tests that passing invalid parameters returns a 422 error.
    """
    assert fixture_aggregate_messages  # To avoid unused parameter warnings

    # A string as limit
    response = await get_aggregates(ccn_api_client, ADDRESS_1, limit="abc")
    assert response.status == 422
    assert response.content_type == "application/json"

    errors = await response.json()
    assert len(errors) == 1
    assert errors[0]["loc"] == ["limit"], errors
