import pytest
import pytest_asyncio

from aleph.services.p2p import http as p2p_http


@pytest_asyncio.fixture(autouse=True)
async def _reset_p2p_sessions():
    """Ensure tests start and end with an empty SESSIONS dict."""
    await p2p_http.close_sessions()
    yield
    await p2p_http.close_sessions()


@pytest.mark.asyncio
async def test_close_sessions_clears_cache_and_closes_sessions():
    # Force a session into the module-level cache by calling the request helper
    # against a deliberately unreachable URI (the helper swallows errors).
    await p2p_http.api_get_request("http://127.0.0.1:1", "ignored", timeout=2)
    assert 2 in p2p_http.SESSIONS
    session = p2p_http.SESSIONS[2]
    assert not session.closed

    await p2p_http.close_sessions()

    assert p2p_http.SESSIONS == {}
    assert session.closed


@pytest.mark.asyncio
async def test_close_sessions_is_idempotent():
    await p2p_http.close_sessions()
    await p2p_http.close_sessions()
