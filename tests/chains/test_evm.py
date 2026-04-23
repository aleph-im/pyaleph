from unittest.mock import AsyncMock, patch

import pytest

from aleph.chains.evm import EVMVerifier
from aleph.schemas.pending_messages import BasePendingMessage, parse_message


@pytest.fixture
def evm_message() -> BasePendingMessage:
    return parse_message(
        {
            "item_hash": "f524a258d87f1771e8538fd4fd91acdcc527c3b7f138fafd6ff89a5fcf97c3b7",
            "type": "POST",
            "chain": "ETH",
            "sender": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
            "signature": "0x99efc66c781c889e1f21c680869c832141dcee90189e75e85f570b8b49e72dee0338d77c214ae55bfcb886bbd7bac6dc4dcfda4eb0d2c47ed93d51b36b7259b01c",
            "time": 1730410918.092607,
            "item_type": "inline",
            "item_content": '{"address":"0xA07B1214bAe0D5ccAA25449C3149c0aC83658874","time":1730410918.0924816,"content":{"type":"polygon","address":"0xA07B1214bAe0D5ccAA25449C3149c0aC83658874","content":{"body":"This message was posted from the typescript-SDK test suite"},"time":1689163528.372},"type":"test"}',
            "channel": "ALEPH-CLOUDSOLUTIONS",
        }
    )


@pytest.mark.asyncio
async def test_verify_evm_signature_real(evm_message: BasePendingMessage):
    verifier = EVMVerifier()
    result = await verifier.verify_signature(evm_message)
    assert result is True


@pytest.mark.asyncio
async def test_verify_evm_signature_case_insensitive(evm_message: BasePendingMessage):
    """Verify that signature verification succeeds when sender case differs from recovered address."""
    checksummed_address = "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874"
    evm_message.sender = checksummed_address.lower()

    verifier = EVMVerifier()

    with patch(
        "aleph.chains.evm.Account.recover_message", return_value=checksummed_address
    ):
        result = await verifier.verify_signature(evm_message)

    assert result is True


@pytest.mark.asyncio
async def test_verify_bad_evm_signature(evm_message: BasePendingMessage):
    verifier = EVMVerifier()
    evm_message.signature = "baba"
    result = await verifier.verify_signature(evm_message)
    assert result is False


def test_evm_verifier_accepts_rpc_url():
    """EVMVerifier can be constructed with an rpc_url without errors."""
    verifier = EVMVerifier(rpc_url="http://localhost:8545")
    assert verifier.rpc_url == "http://localhost:8545"


def test_evm_verifier_no_rpc_url():
    """EVMVerifier can still be constructed with no rpc_url."""
    verifier = EVMVerifier()
    assert verifier.rpc_url is None


@pytest.fixture
def erc1271_message() -> BasePendingMessage:
    """Message signed by a deployed Kernel smart wallet (86-byte inner sig, no ERC-6492 wrapper)."""
    return parse_message(
        {
            "item_hash": "442b2570512753ed1b41f84e8202023f19fd5d5ba31117c8319ea173a92488bd",
            "type": "POST",
            "chain": "ETH",
            "sender": "0xa9F3Cd4E416c6e911DB3DcB5CA6CD77e9F861635",
            "signature": "0x01845adb2c711129d4f3966735ed98a9f09fc4ce5794f8df9bcc3e2fa2049519666e9977ff76f9c99322db6a1f1117f3955411b2ae316b72e49bd1743a5dee905ea4f27c4e7912479995f6b99eb56c44349dabe3731c",
            "time": 1730410918.0,
            "item_type": "inline",
            "item_content": '{"address":"0xa9F3Cd4E416c6e911DB3DcB5CA6CD77e9F861635","time":1730410918.0,"content":{},"type":"test"}',
            "channel": "TEST",
        }
    )


@pytest.mark.asyncio
async def test_verify_erc1271_deployed_valid(erc1271_message: BasePendingMessage):
    """ERC-1271: valid sig from a deployed smart wallet returns True."""
    verifier = EVMVerifier(rpc_url="http://localhost:8545")

    mock_w3 = AsyncMock()
    mock_w3.eth.get_code = AsyncMock(return_value=b"\x60\x80")
    mock_w3.eth.call = AsyncMock(return_value=bytes.fromhex("1626ba7e" + "00" * 28))

    with patch.object(verifier, "_get_web3_client", return_value=mock_w3):
        result = await verifier.verify_signature(erc1271_message)

    assert result is True


@pytest.mark.asyncio
async def test_verify_erc1271_deployed_invalid(erc1271_message: BasePendingMessage):
    """ERC-1271: wrong response from isValidSignature returns False."""
    verifier = EVMVerifier(rpc_url="http://localhost:8545")

    mock_w3 = AsyncMock()
    mock_w3.eth.get_code = AsyncMock(return_value=b"\x60\x80")
    mock_w3.eth.call = AsyncMock(return_value=bytes.fromhex("deadbeef" + "00" * 28))

    with patch.object(verifier, "_get_web3_client", return_value=mock_w3):
        result = await verifier.verify_signature(erc1271_message)

    assert result is False


@pytest.mark.asyncio
async def test_verify_erc1271_no_rpc_falls_back_to_ecdsa(
    erc1271_message: BasePendingMessage,
):
    """Without rpc_url, smart wallet sigs fail gracefully (no RPC available)."""
    verifier = EVMVerifier()
    result = await verifier.verify_signature(erc1271_message)
    assert result is False


@pytest.mark.asyncio
async def test_verify_erc1271_skipped_when_no_code(
    erc1271_message: BasePendingMessage,
):
    """If sender has no deployed code, ERC-1271 path is skipped."""
    verifier = EVMVerifier(rpc_url="http://localhost:8545")

    mock_w3 = AsyncMock()
    mock_w3.eth.get_code = AsyncMock(return_value=b"")

    with patch.object(verifier, "_get_web3_client", return_value=mock_w3):
        result = await verifier.verify_signature(erc1271_message)

    assert result is False
    mock_w3.eth.call.assert_not_called()


# The real ERC-6492 signature from message 6f699b25...
ERC6492_SIG = (
    "0x000000000000000000000000d703aae79538628d27099b8c4f621be4ccd142d5"
    "0000000000000000000000000000000000000000000000000000000000000060"
    "0000000000000000000000000000000000000000000000000000000000000260"
    "00000000000000000000000000000000000000000000000000000000000001c4"
    "c5265d5d000000000000000000000000aac5d4240af87249b3f71bc8e4a2cae074a3e419"
    "0000000000000000000000000000000000000000000000000000000000000060"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "00000000000000000000000000000000000000000000000000000000000001243c3b752b"
    "01845ADb2C711129d4f3966735eD98a9F09fC4cE57"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "00000000000000000000000000000000000000000000000000000000000000000000"
    "0000a000000000000000000000000000000000000000000000000000000000000000e0"
    "0000000000000000000000000000000000000000000000000000000000000100"
    "0000000000000000000000000000000000000000000000000000000000000014"
    "fFFEfCDE25e1d00474530f1A7b90D02CEda94fD7"
    "000000000000000000000000000000000000000000000000000000000000000000"
    "000000000000000000000000000000000000000000000000000000000000000000"
    "000000000000000000000000000000000000000000000000000000000000000000"
    "000000000000000000000000000000000000000000000000000000000000000000"
    "000000000000000000000000000000000000000000000000000000000000000000"
    "000000000000000000000000000000000000000000000000000000000056"
    "01845ADb2C711129d4f3966735eD98a9F09fC4cE57"
    "94f8df9bcc3e2fa2049519666e9977ff76f9c99322db6a1f1117f3955411b2ae"
    "316b72e49bd1743a5dee905ea4f27c4e7912479995f6b99eb56c44349dabe373"
    "1c000000000000000000"
    "006492649264926492649264926492649264926492649264926492649264926492"
)


@pytest.fixture
def erc6492_message() -> BasePendingMessage:
    """Message with ERC-6492 counterfactual smart wallet signature."""
    return parse_message(
        {
            "item_hash": "316b07861dee3fcaa40d10a563fec5e8d6b4a81514b1265941bec861ce3e95ae",
            "type": "POST",
            "chain": "ETH",
            "sender": "0xa9F3Cd4E416c6e911DB3DcB5CA6CD77e9F861635",
            "signature": ERC6492_SIG,
            "time": 1745362074.0,
            "item_type": "inline",
            "item_content": '{"address":"0xa9F3Cd4E416c6e911DB3DcB5CA6CD77e9F861635","time":1745362074.0,"content":{"key":"test2","label":"2222"},"type":"ALEPH-SSH"}',
            "channel": "ALEPH-CLOUDSOLUTIONS",
        }
    )


@pytest.mark.asyncio
async def test_verify_erc6492_counterfactual_valid(
    erc6492_message: BasePendingMessage,
):
    """ERC-6492: valid sig from a counterfactual smart wallet returns True."""
    verifier = EVMVerifier(rpc_url="http://localhost:8545")

    mock_w3 = AsyncMock()
    mock_w3.eth.call = AsyncMock(return_value=(1).to_bytes(32, "big"))

    with patch.object(verifier, "_get_web3_client", return_value=mock_w3):
        result = await verifier.verify_signature(erc6492_message)

    assert result is True
    call_args = mock_w3.eth.call.call_args[0][0]
    assert call_args["to"].lower() == "0x0000000000002fd5aeb385d324b580fca7c83823"


@pytest.mark.asyncio
async def test_verify_erc6492_counterfactual_invalid(
    erc6492_message: BasePendingMessage,
):
    """ERC-6492: UniversalSigValidator returns 0 → invalid."""
    verifier = EVMVerifier(rpc_url="http://localhost:8545")

    mock_w3 = AsyncMock()
    mock_w3.eth.call = AsyncMock(return_value=(0).to_bytes(32, "big"))

    with patch.object(verifier, "_get_web3_client", return_value=mock_w3):
        result = await verifier.verify_signature(erc6492_message)

    assert result is False


@pytest.mark.asyncio
async def test_erc6492_detection_skips_ecdsa(
    erc6492_message: BasePendingMessage,
):
    """ERC-6492 signatures skip ECDSA entirely and go straight to UniversalSigValidator."""
    verifier = EVMVerifier(rpc_url="http://localhost:8545")

    mock_w3 = AsyncMock()
    mock_w3.eth.call = AsyncMock(return_value=(1).to_bytes(32, "big"))

    with patch.object(verifier, "_get_web3_client", return_value=mock_w3):
        with patch("aleph.chains.evm.Account.recover_message") as mock_recover:
            await verifier.verify_signature(erc6492_message)
            mock_recover.assert_not_called()


@pytest.mark.asyncio
async def test_verify_erc6492_no_rpc_fails(erc6492_message: BasePendingMessage):
    """Without rpc_url, ERC-6492 sigs fail (cannot call UniversalSigValidator)."""
    verifier = EVMVerifier()
    result = await verifier.verify_signature(erc6492_message)
    assert result is False
