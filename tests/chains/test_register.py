import pytest

import aleph.chains
from aleph.chains.register import VERIFIER_REGISTER, INCOMING_WORKERS, OUTGOING_WORKERS
from aleph.chains import binance, ethereum, nuls


@pytest.mark.asyncio
async def test_verifiers():
    assert len(VERIFIER_REGISTER.keys()) == 3  # 3 verifiers are included by default
    assert "BNB" in VERIFIER_REGISTER.keys()
    assert VERIFIER_REGISTER["BNB"] is binance.verify_signature
    assert "ETH" in VERIFIER_REGISTER.keys()
    assert VERIFIER_REGISTER["ETH"] is ethereum.verify_signature
    assert "NULS" in VERIFIER_REGISTER.keys()
    assert VERIFIER_REGISTER["NULS"] is nuls.verify_signature


@pytest.mark.asyncio
async def test_outgoing():
    assert len(OUTGOING_WORKERS.keys()) == 3  # 3 verifiers are included by default
    assert "BNB" in OUTGOING_WORKERS.keys()
    assert OUTGOING_WORKERS["BNB"] is binance.binance_outgoing_worker
    assert "ETH" in OUTGOING_WORKERS.keys()
    assert OUTGOING_WORKERS["ETH"] is ethereum.ethereum_outgoing_worker
    assert "NULS" in OUTGOING_WORKERS.keys()
    assert OUTGOING_WORKERS["NULS"] is nuls.nuls_outgoing_worker


@pytest.mark.asyncio
async def test_incoming():
    assert len(INCOMING_WORKERS.keys()) == 3  # 3 verifiers are included by default
    assert "BNB" in INCOMING_WORKERS.keys()
    assert INCOMING_WORKERS["BNB"] is binance.binance_incoming_worker
    assert "ETH" in INCOMING_WORKERS.keys()
    assert INCOMING_WORKERS["ETH"] is ethereum.ethereum_incoming_worker
    assert "NULS" in INCOMING_WORKERS.keys()
    assert INCOMING_WORKERS["NULS"] is nuls.nuls_incoming_worker
