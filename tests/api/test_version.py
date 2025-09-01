import pytest

from aleph.version import __version__


@pytest.mark.asyncio
async def test_get_version(ccn_api_client):
    response = await ccn_api_client.get("/api/v0/version")
    assert response.status == 200, await response.text()

    data = await response.json()
    assert data["version"] == __version__
