import pytest

from aleph.web import app
from aleph import get_git_version


@pytest.mark.asyncio
async def test_get_version(aiohttp_client):
    client = await aiohttp_client(app)

    response = await client.get("/api/v0/version")
    assert response.status == 200, await response.text()

    data = await response.json()
    assert data["version"] == get_git_version()
