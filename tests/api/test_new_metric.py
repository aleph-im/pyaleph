from typing import Any, Dict, Sequence

import pytest
import pytest_asyncio

from aleph.types.db_session import DbSessionFactory

from .conftest import _load_fixtures


def _generate_uri(node_type: str, node_id: str) -> str:
    return f"/api/v0/{node_type}/{node_id}/metrics"


@pytest_asyncio.fixture
async def fixture_metrics_messages(
    session_factory: DbSessionFactory,
) -> Sequence[Dict[str, Any]]:
    return await _load_fixtures(session_factory, "test-metric.json")


@pytest.mark.asyncio
async def test_node_core_metrics(fixture_metrics_messages, ccn_api_client):
    uri = _generate_uri(
        "core", "b8b9104da69c54e58531212234fa31f49ef4c668a39a0bf6793322407857b821"
    )
    response = await ccn_api_client.get(uri)
    test_data = await response.json()

    assert response.status == 200
    assert (
        test_data["metrics"]["item_hash"][0]
        == "56c82c6d3b28b76456594b4b57154b6826a6d5fb97d355d0428e5ca7d08193b9"
    )
    assert (
        test_data["metrics"]["item_hash"][1]
        == "172bab8f624fff1be70a19fecd45ff51fa4f833a34074451c7d79ece19bf37f0"
    )


@pytest.mark.asyncio
async def test_node_core_metrics_sort(fixture_metrics_messages, ccn_api_client):
    uri = _generate_uri(
        "core", "2e7cd67ff8f556b0b3fb8a2ef8ab0e8e1466cfa279dd7b9bfbc8aba92e0c5672"
    )
    response = await ccn_api_client.get(uri, params={"sort": "DESC"})
    test_data = await response.json()

    assert response.status == 200
    assert (
        test_data["metrics"]["item_hash"][1]
        == "56c82c6d3b28b76456594b4b57154b6826a6d5fb97d355d0428e5ca7d08193b9"
    )
    assert (
        test_data["metrics"]["item_hash"][0]
        == "172bab8f624fff1be70a19fecd45ff51fa4f833a34074451c7d79ece19bf37f0"
    )


@pytest.mark.asyncio
async def test_node_core_metrics_end_date(fixture_metrics_messages, ccn_api_client):
    uri = _generate_uri(
        "core", "b8b9104da69c54e58531212234fa31f49ef4c668a39a0bf6793322407857b821"
    )
    response = await ccn_api_client.get(uri, params={"end_date": 1701261023})
    test_data = await response.json()

    assert response.status == 200
    assert (
        test_data["metrics"]["item_hash"][0]
        == "56c82c6d3b28b76456594b4b57154b6826a6d5fb97d355d0428e5ca7d08193b9"
    )


@pytest.mark.asyncio
async def test_node_core_metrics_start_date(fixture_metrics_messages, ccn_api_client):
    uri = _generate_uri(
        "core", "b8b9104da69c54e58531212234fa31f49ef4c668a39a0bf6793322407857b821"
    )
    response = await ccn_api_client.get(uri, params={"start_date": 1701261023})
    test_data = await response.json()

    assert response.status == 200
    assert (
        test_data["metrics"]["item_hash"][0]
        == "172bab8f624fff1be70a19fecd45ff51fa4f833a34074451c7d79ece19bf37f0"
    )


@pytest.mark.asyncio
async def test_node_core_not_exist(fixture_metrics_messages, ccn_api_client):
    uri = _generate_uri("core", "This_is_a_node_id")
    response = await ccn_api_client.get(uri)

    assert response.status == 404


@pytest.mark.asyncio
async def test_node_compute_metric(fixture_metrics_messages, ccn_api_client):
    uri = _generate_uri(
        "compute", "d491f38ec66fe23a9c9ad398a04fd4dcb44a115b948ef612db844caea85cd59a"
    )
    response = await ccn_api_client.get(uri)
    test_data = await response.json()

    assert response.status == 200
    assert (
        test_data["metrics"]["item_hash"][0]
        == "56c82c6d3b28b76456594b4b57154b6826a6d5fb97d355d0428e5ca7d08193b9"
    )

@pytest.mark.asyncio
async def test_node_compute_metric_not_exist(fixture_metrics_messages, ccn_api_client):
    uri = _generate_uri(
        "compute", "This_is_a_node_id"
    )
    response = await ccn_api_client.get(uri)

    assert response.status == 404