import datetime as dt

import pytest

from aleph.db.accessors.authorizations import (
    filter_authorizations,
    get_granted_authorizations,
    get_received_authorizations,
    paginate_authorizations,
)
from aleph.db.models import AggregateDb, AggregateElementDb


@pytest.fixture
def security_aggregates(session_factory):
    """Insert security aggregates for testing.

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
        session.add(
            AggregateElementDb(
                item_hash="hash_empty",
                key="security",
                owner="0xOwnerEmpty",
                content={},
                creation_datetime=dt.datetime(2024, 1, 3, tzinfo=dt.timezone.utc),
            )
        )
        session.flush()

        # Owner A grants to B and C
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
        # Owner B grants to A
        session.add(
            AggregateDb(
                key="security",
                owner="0xOwnerB",
                content={
                    "authorizations": [
                        {
                            "address": "0xOwnerA",
                            "types": ["POST", "STORE"],
                        },
                    ]
                },
                creation_datetime=dt.datetime(2024, 1, 2, tzinfo=dt.timezone.utc),
                last_revision_hash="hash_b",
                dirty=False,
            )
        )
        # Owner with no authorizations key
        session.add(
            AggregateDb(
                key="security",
                owner="0xOwnerEmpty",
                content={},
                creation_datetime=dt.datetime(2024, 1, 3, tzinfo=dt.timezone.utc),
                last_revision_hash="hash_empty",
                dirty=False,
            )
        )
        session.commit()


# --- Forward lookup tests ---


def test_get_granted_authorizations(session_factory, security_aggregates):
    """get_granted_authorizations returns the raw aggregate content."""
    with session_factory() as session:
        result = get_granted_authorizations(session=session, owner="0xOwnerA")

    assert result is not None
    assert "authorizations" in result
    auths = result["authorizations"]
    assert len(auths) == 2
    # Raw content still includes the address field
    assert auths[0]["address"] == "0xGranteeB"
    assert auths[1]["address"] == "0xGranteeC"


def test_get_granted_authorizations_no_aggregate(session_factory, security_aggregates):
    with session_factory() as session:
        result = get_granted_authorizations(session=session, owner="0xNobody")

    assert result is None


# --- Reverse lookup tests ---


def test_get_received_authorizations(session_factory, security_aggregates):
    with session_factory() as session:
        results = get_received_authorizations(session=session, address="0xOwnerA")

    # 0xOwnerB granted permissions to 0xOwnerA
    assert len(results) == 1
    owner, auths = results[0]
    assert owner == "0xOwnerB"
    assert len(auths) == 1
    assert auths[0]["types"] == ["POST", "STORE"]
    # 'address' field is stripped (redundant with the lookup key)
    assert "address" not in auths[0]


def test_get_received_authorizations_multiple_granters(
    session_factory, security_aggregates
):
    with session_factory() as session:
        results = get_received_authorizations(session=session, address="0xGranteeB")

    # Only 0xOwnerA granted to 0xGranteeB
    assert len(results) == 1
    owner, auths = results[0]
    assert owner == "0xOwnerA"
    assert len(auths) == 1
    assert auths[0]["types"] == ["POST"]
    assert "address" not in auths[0]


def test_get_received_authorizations_none(session_factory, security_aggregates):
    with session_factory() as session:
        results = get_received_authorizations(session=session, address="0xNobody")

    assert results == []


# --- Filter tests ---


@pytest.fixture
def sample_authorizations():
    """Grouped authorization data for testing filters.

    The 'address' field is already stripped at this stage (done by
    _build_grouped_from_content / get_received_authorizations).
    """
    return {
        "0xGranterA": [
            {
                "types": ["POST"],
                "channels": ["chan1", "chan2"],
                "chain": "ETH",
                "post_types": ["amend"],
                "aggregate_keys": [],
            },
            {
                "types": ["STORE"],
                "channels": ["chan3"],
                "chain": "SOL",
            },
        ],
        "0xGranterB": [
            {
                "types": ["POST", "STORE"],
            },
        ],
        "0xGranterC": [
            {
                "types": ["AGGREGATE"],
                "aggregate_keys": ["key1"],
            },
        ],
    }


def test_filter_by_types(sample_authorizations):
    result = filter_authorizations(sample_authorizations, types=["POST"])
    # 0xGranterA has a POST entry, 0xGranterB has POST+STORE, 0xGranterC excluded
    assert "0xGranterA" in result
    assert "0xGranterB" in result
    assert "0xGranterC" not in result
    # Only the POST entry from 0xGranterA, not the STORE one
    assert len(result["0xGranterA"]) == 1
    assert ["POST"] in [e.get("types") for e in result["0xGranterA"]]


def test_filter_by_channels(sample_authorizations):
    result = filter_authorizations(sample_authorizations, channels=["chan1"])
    assert "0xGranterA" in result
    assert len(result["0xGranterA"]) == 1
    assert "chan1" in result["0xGranterA"][0]["channels"]
    # 0xGranterB has no channels field -> it matches any channel (unrestricted)
    assert "0xGranterB" in result


def test_filter_by_chains(sample_authorizations):
    result = filter_authorizations(sample_authorizations, chains=["ETH"])
    assert "0xGranterA" in result
    assert len(result["0xGranterA"]) == 1
    assert result["0xGranterA"][0]["chain"] == "ETH"
    # 0xGranterB has no chain -> unrestricted, matches
    assert "0xGranterB" in result


def test_filter_by_post_types(sample_authorizations):
    result = filter_authorizations(sample_authorizations, post_types=["amend"])
    assert "0xGranterA" in result
    # Both entries match: entry 1 has post_types=["amend"], entry 2 has no post_types (unrestricted)
    assert len(result["0xGranterA"]) == 2
    # 0xGranterB has no post_types -> unrestricted
    assert "0xGranterB" in result


def test_filter_by_aggregate_keys(sample_authorizations):
    result = filter_authorizations(sample_authorizations, aggregate_keys=["key1"])
    assert "0xGranterC" in result
    # 0xGranterB has no aggregate_keys -> unrestricted
    assert "0xGranterB" in result
    # 0xGranterA first entry has empty aggregate_keys -> unrestricted
    assert "0xGranterA" in result


def test_filter_no_filters(sample_authorizations):
    result = filter_authorizations(sample_authorizations)
    assert result == sample_authorizations


def test_filter_combined(sample_authorizations):
    result = filter_authorizations(
        sample_authorizations, types=["POST"], chains=["ETH"]
    )
    # Only 0xGranterA has POST+ETH, 0xGranterB has POST but no chain (unrestricted)
    assert "0xGranterA" in result
    assert len(result["0xGranterA"]) == 1
    assert "0xGranterB" in result
    assert "0xGranterC" not in result


# --- Pagination tests ---


def test_paginate_first_page():
    data = {f"0xAddr{i}": [{"types": ["POST"]}] for i in range(5)}
    result, total = paginate_authorizations(data, page=1, pagination=2)
    assert total == 5
    assert len(result) == 2


def test_paginate_second_page():
    data = {f"0xAddr{i}": [{"types": ["POST"]}] for i in range(5)}
    result, total = paginate_authorizations(data, page=2, pagination=2)
    assert total == 5
    assert len(result) == 2


def test_paginate_last_page():
    data = {f"0xAddr{i}": [{"types": ["POST"]}] for i in range(5)}
    result, total = paginate_authorizations(data, page=3, pagination=2)
    assert total == 5
    assert len(result) == 1


def test_paginate_empty():
    result, total = paginate_authorizations({}, page=1, pagination=20)
    assert total == 0
    assert result == {}


def test_paginate_beyond_range():
    data = {"0xAddr0": [{"types": ["POST"]}]}
    result, total = paginate_authorizations(data, page=5, pagination=20)
    assert total == 1
    assert result == {}
