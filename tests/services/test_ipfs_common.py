from unittest.mock import patch

from configmanager import Config

from aleph.config import get_defaults
from aleph.services.ipfs.common import (
    make_ipfs_client,
    make_ipfs_p2p_client,
    make_ipfs_pinning_client,
)


def _make_config() -> Config:
    config = Config(schema=get_defaults())
    return config


def test_make_ipfs_client_does_not_pass_read_timeout():
    """aioipfs 0.7.1 silently ignores read_timeout: AsyncIPFS stores it in
    self._read_timeout but builds its session via get_session() with no
    arguments, so the value never reaches aiohttp. Passing it would advertise
    a configurable pin timeout that does not exist, so we must not pass it."""
    with patch("aleph.services.ipfs.common.aioipfs.AsyncIPFS") as mock_cls:
        make_ipfs_client("localhost", 5001)

    _, kwargs = mock_cls.call_args
    assert "read_timeout" not in kwargs


def test_pinning_client_falls_back_to_main_config():
    """With no separate pinning daemon configured, the pinning client is built
    against the main IPFS host/port. It takes no timeout: the effective
    timeouts are aioipfs's session defaults."""
    config = _make_config()

    with patch("aleph.services.ipfs.common.aioipfs.AsyncIPFS") as mock_cls:
        make_ipfs_pinning_client(config)

    _, kwargs = mock_cls.call_args
    assert kwargs["host"] == config.ipfs.host.value
    assert kwargs["port"] == int(config.ipfs.port.value)
    assert "read_timeout" not in kwargs


def test_p2p_client_uses_main_config():
    config = _make_config()

    with patch("aleph.services.ipfs.common.aioipfs.AsyncIPFS") as mock_cls:
        make_ipfs_p2p_client(config)

    _, kwargs = mock_cls.call_args
    assert kwargs["host"] == config.ipfs.host.value
    assert kwargs["port"] == int(config.ipfs.port.value)
    assert "read_timeout" not in kwargs
