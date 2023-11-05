import pytest
from configmanager import Config
from web3 import Web3

from aleph.chains.ethereum import get_contract


@pytest.fixture
def web3():
    return Web3()


@pytest.mark.asyncio
async def test_get_contract(mock_config: Config, web3: Web3):
    contract = await get_contract(config=mock_config, web3=web3)
    # The type hint provided by the web3 library is clearly wrong. This is a simple check
    # to ensure that we get a proper web3 object. Improve as needed.
    assert contract.w3 == web3
