import datetime as dt

import pytest

from aleph.db.models import AggregateDb, AggregateElementDb

GRANTED_URI = "/api/v0/authorizations/granted/{address}.json"
RECEIVED_URI = "/api/v0/authorizations/received/{address}.json"


@pytest.fixture
def security_aggregates(session_factory):
    """Insert security aggregates for API testing.

    AggregateDb has a FK to AggregateElementDb, so we must create
    matching element rows first.
    """
    with session_factory() as session:
        # Create aggregate elements (required by FK constraint)
        session.add(
            AggregateElementDb(
                item_hash="hash_a",
                key="security",
                owner="0xOwnerA",
                content={"authorizations": []},
                creation_datetime=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            )
        )
        session.add(
            AggregateElementDb(
                item_hash="hash_b",
                key="security",
                owner="0xOwnerB",
                content={"authorizations": []},
                creation_datetime=dt.datetime(2024, 1, 2, tzinfo=dt.timezone.utc),
            )
        )
        session.flush()

        session.add(
            AggregateDb(
                key="security",
                owner="0xOwnerA",
                content={
                    "authorizations": [
                        {
                            "address": "0xGranteeB",
                            "types": ["POST"],
                            "channels": ["chan1"],
                            "chain": "ETH",
                        },
                        {
                            "address": "0xGranteeC",
                            "types": ["STORE"],
                        },
                    ]
                },
                creation_datetime=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
                last_revision_hash="hash_a",
                dirty=False,
            )
        )
        session.add(
            AggregateDb(
                key="security",
                owner="0xOwnerB",
                content={
                    "authorizations": [
                        {
                            "address": "0xGranteeB",
                            "types": ["POST", "STORE"],
                            "chain": "SOL",
                        },
                    ]
                },
                creation_datetime=dt.datetime(2024, 1, 2, tzinfo=dt.timezone.utc),
                last_revision_hash="hash_b",
                dirty=False,
            )
        )
        session.commit()


# --- Granted endpoint tests ---


@pytest.mark.asyncio
async def test_granted_basic(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(GRANTED_URI.format(address="0xOwnerA"))
    assert response.status == 200
    data = await response.json()
    assert data["address"] == "0xOwnerA"
    assert "0xGranteeB" in data["authorizations"]
    assert "0xGranteeC" in data["authorizations"]
    assert data["pagination_total"] == 2


@pytest.mark.asyncio
async def test_granted_no_aggregate(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(GRANTED_URI.format(address="0xNobody"))
    assert response.status == 200
    data = await response.json()
    assert data["authorizations"] == {}
    assert data["pagination_total"] == 0


@pytest.mark.asyncio
async def test_granted_filter_grantee(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(
        GRANTED_URI.format(address="0xOwnerA"),
        params={"grantee": "0xGranteeB"},
    )
    assert response.status == 200
    data = await response.json()
    assert "0xGranteeB" in data["authorizations"]
    assert "0xGranteeC" not in data["authorizations"]


@pytest.mark.asyncio
async def test_granted_pagination(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(
        GRANTED_URI.format(address="0xOwnerA"),
        params={"pagination": "1", "page": "1"},
    )
    assert response.status == 200
    data = await response.json()
    assert len(data["authorizations"]) == 1
    assert data["pagination_total"] == 2
    assert data["pagination_per_page"] == 1
    assert data["pagination_page"] == 1


# --- Received endpoint tests ---


@pytest.mark.asyncio
async def test_received_basic(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(RECEIVED_URI.format(address="0xGranteeB"))
    assert response.status == 200
    data = await response.json()
    assert data["address"] == "0xGranteeB"
    # Both 0xOwnerA and 0xOwnerB granted to 0xGranteeB
    assert "0xOwnerA" in data["authorizations"]
    assert "0xOwnerB" in data["authorizations"]
    assert data["pagination_total"] == 2


@pytest.mark.asyncio
async def test_received_no_grants(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(RECEIVED_URI.format(address="0xNobody"))
    assert response.status == 200
    data = await response.json()
    assert data["authorizations"] == {}
    assert data["pagination_total"] == 0


@pytest.mark.asyncio
async def test_received_filter_granter(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(
        RECEIVED_URI.format(address="0xGranteeB"),
        params={"granter": "0xOwnerA"},
    )
    assert response.status == 200
    data = await response.json()
    assert "0xOwnerA" in data["authorizations"]
    assert "0xOwnerB" not in data["authorizations"]


@pytest.mark.asyncio
async def test_received_filter_chains(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(
        RECEIVED_URI.format(address="0xGranteeB"),
        params={"chains": "ETH"},
    )
    assert response.status == 200
    data = await response.json()
    # 0xOwnerA has ETH chain, 0xOwnerB has SOL chain
    assert "0xOwnerA" in data["authorizations"]
    assert "0xOwnerB" not in data["authorizations"]


@pytest.mark.asyncio
async def test_received_pagination(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(
        RECEIVED_URI.format(address="0xGranteeB"),
        params={"pagination": "1", "page": "1"},
    )
    assert response.status == 200
    data = await response.json()
    assert len(data["authorizations"]) == 1
    assert data["pagination_total"] == 2


@pytest.mark.asyncio
async def test_received_filter_types(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(
        RECEIVED_URI.format(address="0xGranteeB"),
        params={"types": "STORE"},
    )
    assert response.status == 200
    data = await response.json()
    # 0xOwnerB has STORE in types, 0xOwnerA only has POST
    assert "0xOwnerB" in data["authorizations"]
    assert "0xOwnerA" not in data["authorizations"]


@pytest.mark.asyncio
async def test_invalid_pagination(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(
        GRANTED_URI.format(address="0xOwnerA"),
        params={"pagination": "0"},
    )
    assert response.status == 422
