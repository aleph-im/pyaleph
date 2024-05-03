from dataclasses import dataclass
from typing import Optional
from unittest.mock import Mock, AsyncMock

import pytest
import requests
from aiohttp import ClientResponseError

from aleph.web.controllers.metrics import (
    BuildInfo,
    Metrics,
    fetch_reference_total_messages,
    format_dataclass_for_prometheus,
    format_dict_for_prometheus, fetch_eth_height,
)
from aioresponses import aioresponses



def test_format_dict_for_prometheus():
    assert (
        format_dict_for_prometheus(
            {
                "a": 1,
                "b": 2.2,
                "c": "3",
            }
        )
        == '{a=1,b=2.2,c="3"}'
    )


def test_format_dataclass_for_prometheus() -> None:

    @dataclass
    class Simple:
        a: int
        b: float
        c: str

    assert format_dataclass_for_prometheus(Simple(1, 2.2, "3")) == 'a 1\nb 2.2\nc "3"'

    @dataclass
    class Tagged:
        d: Simple
        e: str

    assert (
        format_dataclass_for_prometheus(Tagged(Simple(1, 2.2, "3"), "e"))
        == 'd{a=1,b=2.2,c="3"} 1\ne "e"'
    )


def test_metrics():
    metrics = Metrics(
        pyaleph_build_info=BuildInfo(
            python_version="3.8.0",
            version="v999",
        ),
        pyaleph_status_peers_total=0,
        pyaleph_status_sync_messages_total=123,
        pyaleph_status_sync_permanent_files_total=1999,
        pyaleph_status_sync_pending_messages_total=456,
        pyaleph_status_sync_pending_txs_total=0,
        pyaleph_status_chain_eth_last_committed_height=0,
        pyaleph_processing_pending_messages_aggregate_tasks=0,
        pyaleph_processing_pending_messages_forget_tasks=0,
        pyaleph_processing_pending_messages_post_tasks=0,
        pyaleph_processing_pending_messages_program_tasks=0,
        pyaleph_processing_pending_messages_store_tasks=0,
    )

    assert format_dataclass_for_prometheus(metrics) == (
        'pyaleph_build_info{python_version="3.8.0",version="v999"} 1\n'
        "pyaleph_status_peers_total 0\n"
        "pyaleph_status_sync_messages_total 123\n"
        "pyaleph_status_sync_permanent_files_total 1999\n"
        "pyaleph_status_sync_pending_messages_total 456\n"
        "pyaleph_status_sync_pending_txs_total 0\n"
        "pyaleph_status_chain_eth_last_committed_height 0\n"
        "pyaleph_processing_pending_messages_aggregate_tasks 0\n"
        "pyaleph_processing_pending_messages_forget_tasks 0\n"
        "pyaleph_processing_pending_messages_post_tasks 0\n"
        "pyaleph_processing_pending_messages_program_tasks 0\n"
        "pyaleph_processing_pending_messages_store_tasks 0"
    )


def mock_aiocache_cached(ttl: Optional[int] = None):
    """A mock for `aiocache.cached` that does not cache the return value."""
    def decorator(func):
        return func

    return decorator


@pytest.mark.asyncio
async def test_fetch_reference_total_messages_success(mocker):
    # Define a test configuration with a mock URL
    config = Mock()
    config.aleph.reference_node_url.value = "https://reference-node.example.com"

    # Mock the aiocache.cached decorator to avoid caching the return value
    mocker.patch("aleph.web.controllers.metrics.cached", mock_aiocache_cached)

    # Mock the get_config function to return the test configuration
    mocker.patch("aleph.web.controllers.metrics.get_config", return_value=config)

    # Use aioresponses to mock the aiohttp request
    with aioresponses() as mocked:
        mocked.get("https://reference-node.example.com/metrics.json",
                   payload={"pyaleph_status_sync_messages_total": "10"}, status=200)

        # Test the fetch_reference_total_messages function
        result = await fetch_reference_total_messages()

        mocked.assert_called_once_with("https://reference-node.example.com/metrics.json",
                                       raise_for_status=True)
        assert result == 10


@pytest.mark.asyncio
async def test_fetch_reference_total_messages_failure(mocker):
    # Define a test configuration with a mock URL
    config = Mock()
    config.aleph.reference_node_url.value = "https://reference-node.example.com"

    # Mock the aiocache.cached decorator to avoid caching the return value
    mocker.patch("aleph.web.controllers.metrics.cached", mock_aiocache_cached)

    # Mock the get_config function to return the test configuration
    mocker.patch("aleph.web.controllers.metrics.get_config", return_value=config)

    # Use aioresponses to mock the aiohttp request
    with aioresponses() as mocked:
        mocked.get("https://reference-node.example.com/metrics.json",
                   status=400)

        # Test the fetch_reference_total_messages function
        result = await fetch_reference_total_messages()

        mocked.assert_called_once_with("https://reference-node.example.com/metrics.json",
                                       raise_for_status=True)
        assert result is None


@pytest.mark.asyncio
async def test_fetch_eth_height_success(mocker):
    # Define a test configuration with Ethereum enabled and a mock API URL
    config = Mock()
    config.ethereum.enabled.value = True
    config.ethereum.api_url.value = "https://ethereum-rpc.example.com"

    # Mock the get_config function to return the test configuration
    mocker.patch('aleph.web.controllers.metrics.get_config', return_value=config)

    # Mock the Web3 class and its eth.getBlock method
    web3_mock = mocker.patch('aleph.web.controllers.metrics.Web3', autospec=True)
    web3_instance = web3_mock.return_value
    web3_instance.eth.getBlock.return_value = 100

    # Test the fetch_eth_height function
    result = await fetch_eth_height()
    assert result == 100


@pytest.mark.asyncio
async def test_fetch_eth_height_failure(mocker):
    # Define a test configuration with Ethereum enabled and a mock API URL
    config = Mock()
    config.ethereum.enabled.value = True
    config.ethereum.api_url.value = "https://ethereum-rpc.example.com"

    # Mock the get_config function to return the test configuration
    mocker.patch('aleph.web.controllers.metrics.get_config', return_value=config)

    # Mock the Web3 class and its eth.getBlock method to raise an exception
    web3_mock = mocker.patch('aleph.web.controllers.metrics.Web3', autospec=True)
    web3_instance = web3_mock.return_value
    web3_instance.eth.getBlock.side_effect = Exception

    # Test the fetch_eth_height function
    result = await fetch_eth_height()
    assert result is None


# @pytest.mark.asyncio
# async def test_fetch_eth_height_tim
