"""
Integration tests for EVMVerifier against a real Ethereum mainnet RPC.

These tests are marked `network` and excluded from the default pytest run.
Enable them explicitly:

    hatch run testing:test tests/chains/test_evm_integration.py -m network -v

Or override addopts:

    hatch run testing:test tests/chains/test_evm_integration.py -m network -v \
        --override-ini="addopts="

Uses a public mainnet RPC by default. Override with ALEPH_TEST_ETH_RPC if you
have your own node.
"""

import os

import pytest

from aleph.chains.evm import EVMVerifier
from aleph.schemas.pending_messages import BasePendingMessage, parse_message

ETH_MAINNET_RPC = os.environ.get(
    "ALEPH_TEST_ETH_RPC", "https://ethereum-rpc.publicnode.com"
)


# Real Aleph message signed by a Privy/Kernel counterfactual smart wallet.
# Source: the production Aleph network, originally rejected before EIP-6492
# support was added.
# item_hash: f4daf9c0dadd7aa89c37e62e24f90a032183ba3b829b2bd2cf87568a940fd0a8
REAL_ERC6492_MESSAGE = {
    "item_hash": "f4daf9c0dadd7aa89c37e62e24f90a032183ba3b829b2bd2cf87568a940fd0a8",
    "type": "POST",
    "chain": "ETH",
    "sender": "0xa9F3Cd4E416c6e911DB3DcB5CA6CD77e9F861635",
    "time": 1776949817.862,
    "item_type": "inline",
    "item_content": (
        '{"type":"ALEPH-SSH",'
        '"address":"0xa9F3Cd4E416c6e911DB3DcB5CA6CD77e9F861635",'
        '"content":{"key":"test1","label":"test1"},'
        '"time":1776949817.862}'
    ),
    "channel": "ALEPH-CLOUDSOLUTIONS",
    "signature": "0x000000000000000000000000d703aae79538628d27099b8c4f621be4ccd142d50000000000000000000000000000000000000000000000000000000000000060000000000000000000000000000000000000000000000000000000000000026000000000000000000000000000000000000000000000000000000000000001c4c5265d5d000000000000000000000000aac5d4240af87249b3f71bc8e4a2cae074a3e4190000000000000000000000000000000000000000000000000000000000000060000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001243c3b752b01845ADb2C711129d4f3966735eD98a9F09fC4cE570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000a000000000000000000000000000000000000000000000000000000000000000e000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000014fFFEfCDE25e1d00474530f1A7b90D02CEda94fD7000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005601845ADb2C711129d4f3966735eD98a9F09fC4cE57ad3840a219707e52978ad891b851ac7302c95785dd6e233f010c205018312c7b1232d3eb5e60be5a12e41d3be3a9635660eea241fc2ef92cc461abf00d44b4831b000000000000000000006492649264926492649264926492649264926492649264926492649264926492",  # noqa: E501
}


@pytest.fixture
def real_erc6492_message() -> BasePendingMessage:
    return parse_message(REAL_ERC6492_MESSAGE)


@pytest.mark.network
@pytest.mark.asyncio
async def test_erc6492_validation_against_mainnet(
    real_erc6492_message: BasePendingMessage,
):
    """End-to-end: real ERC-6492 sig + real mainnet RPC + EIP-6492 bytecode.

    This test exercises the full happy path against a live Ethereum mainnet
    node:
      1. Detects the 0x6492…6492 magic suffix.
      2. Builds the ValidateSigOffchain deploy_data (bytecode + ABI-encoded args).
      3. eth_call with no `to` field → the bytecode runs as a constructor,
         deploys UniversalSigValidator inline, simulates the Kernel factory
         deployment, and calls isValidSignature.
      4. Asserts the returned byte is 0x01 (valid).

    Verifies that the bogus-address bug is fixed and the EIP-6492
    contract-creation pattern works as specified.
    """
    verifier = EVMVerifier(rpc_url=ETH_MAINNET_RPC)
    result = await verifier.verify_signature(real_erc6492_message)
    assert result is True, (
        "Expected the real ERC-6492 signature to validate against mainnet. "
        "If this fails, either the RPC is down or the bytecode asset drifted "
        "from the EIP-6492 reference implementation."
    )
