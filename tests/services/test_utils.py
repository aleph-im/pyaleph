import pytest

from aleph.services.utils import (
    get_IP,
    get_ip4_from_service,
    get_ip4_from_socket,
    is_valid_ip4,
)


def test_is_valid_ip4():
    assert is_valid_ip4("1.2.3.4")
    assert is_valid_ip4("123.456.789.123")
    assert not is_valid_ip4("")
    assert not is_valid_ip4("Hello !")
    assert not is_valid_ip4("a.b.c.d")


@pytest.mark.asyncio
async def test_get_ip4_from_service():
    ip4 = await get_ip4_from_service()
    assert is_valid_ip4(ip4)


def test_get_ip4_from_socket():
    ip4 = get_ip4_from_socket()
    assert is_valid_ip4(ip4)


@pytest.mark.asyncio
async def test_get_IP():
    ip4 = await get_IP()
    assert is_valid_ip4(ip4)
