import pytest

import aleph.chains
from aleph.chains import register
from aleph.chains.register import (VERIFIER_REGISTER, INCOMING_WORKERS, OUTGOING_WORKERS,
                                   register_verifier, register_incoming_worker, register_outgoing_worker)
from aleph.chains import binance, ethereum, nuls, nuls2, neo, substrate, cosmos

@pytest.mark.asyncio
async def test_register_verifier(monkeypatch):
    monkeypatch.setattr(register, 'VERIFIER_REGISTER', dict())
    curlen = len(register.VERIFIER_REGISTER.keys())
    a = object()
    register_verifier('TEST', a)
    assert len(register.VERIFIER_REGISTER.keys()) == curlen + 1
    assert register.VERIFIER_REGISTER['TEST'] is a

@pytest.mark.asyncio
async def test_register_verifier_twice(monkeypatch):
    monkeypatch.setattr(register, 'VERIFIER_REGISTER', dict())
    curlen = len(register.VERIFIER_REGISTER.keys())
    a = dict()
    a['a'] = 1
    register_verifier('TEST', a)
    b = dict()
    b['a'] = 2
    register_verifier('TEST', b)
    assert len(register.VERIFIER_REGISTER.keys()) == curlen + 1
    assert register.VERIFIER_REGISTER['TEST'] is not a
    assert register.VERIFIER_REGISTER['TEST'] is b
    assert register.VERIFIER_REGISTER['TEST']['a'] == 2

@pytest.mark.asyncio
async def test_verifiers():
    assert len(VERIFIER_REGISTER.keys()) == 7  # 6 verifiers are included by default
    assert "BNB" in VERIFIER_REGISTER.keys()
    assert VERIFIER_REGISTER["BNB"] is binance.verify_signature
    assert "ETH" in VERIFIER_REGISTER.keys()
    assert VERIFIER_REGISTER["ETH"] is ethereum.verify_signature
    assert "NULS" in VERIFIER_REGISTER.keys()
    assert VERIFIER_REGISTER["NULS"] is nuls.verify_signature
    assert "NEO" in VERIFIER_REGISTER.keys()
    assert VERIFIER_REGISTER["NEO"] is neo.verify_signature
    assert "DOT" in VERIFIER_REGISTER.keys()
    assert VERIFIER_REGISTER["DOT"] is substrate.verify_signature
    assert "CSDK" in VERIFIER_REGISTER.keys()
    assert VERIFIER_REGISTER["CSDK"] is cosmos.verify_signature


@pytest.mark.asyncio
async def test_register_outgoing_worker(monkeypatch):
    monkeypatch.setattr(register, 'OUTGOING_WORKERS', dict())
    curlen = len(register.OUTGOING_WORKERS.keys())
    a = object()
    register_outgoing_worker('TEST', a)
    assert len(register.OUTGOING_WORKERS.keys()) == curlen + 1
    assert register.OUTGOING_WORKERS['TEST'] is a


@pytest.mark.asyncio
async def test_outgoing():
    assert len(OUTGOING_WORKERS.keys()) == 3  # 3 verifiers are included by default
    assert "BNB" in OUTGOING_WORKERS.keys()
    assert OUTGOING_WORKERS["BNB"] is binance.binance_outgoing_worker
    assert "ETH" in OUTGOING_WORKERS.keys()
    assert OUTGOING_WORKERS["ETH"] is ethereum.ethereum_outgoing_worker
    assert "NULS2" in OUTGOING_WORKERS.keys()
    assert OUTGOING_WORKERS["NULS2"] is nuls2.nuls2_outgoing_worker

@pytest.mark.asyncio
async def test_register_incoming_worker(monkeypatch):
    monkeypatch.setattr(register, 'INCOMING_WORKERS', dict())
    curlen = len(register.INCOMING_WORKERS.keys())
    a = object()
    register_incoming_worker('TEST', a)
    assert len(register.INCOMING_WORKERS.keys()) == curlen + 1
    assert register.INCOMING_WORKERS['TEST'] is a

@pytest.mark.asyncio
async def test_incoming():
    assert len(INCOMING_WORKERS.keys()) == 3  # 3 verifiers are included by default
    assert "BNB" in INCOMING_WORKERS.keys()
    assert INCOMING_WORKERS["BNB"] is binance.binance_incoming_worker
    assert "ETH" in INCOMING_WORKERS.keys()
    assert INCOMING_WORKERS["ETH"] is ethereum.ethereum_incoming_worker
    assert "NULS2" in INCOMING_WORKERS.keys()
    assert INCOMING_WORKERS["NULS2"] is nuls2.nuls_incoming_worker
