import pytest

from aleph import __version__
from aleph.web import app


@pytest.mark.asyncio
async def test_get_version(aiohttp_client):
    client = await aiohttp_client(app)

    response = await client.get("/api/v0/version")
    assert response.status == 200, await response.text()

    data = await response.json()
    assert data["version"] == __version__
