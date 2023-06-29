import pytest
from aleph_message.models import MessageType, Chain, ItemType

from aleph.chains.solana import SolanaConnector
from aleph.schemas.pending_messages import PendingPostMessage
from aleph.toolkit.timestamp import timestamp_to_datetime


@pytest.fixture
def solana_message() -> PendingPostMessage:
    return PendingPostMessage(
        item_hash="6dc1b023dd1e64d28037f49a921412b3e04cf7b2b3d2537c6ae0ad2c239eae3f",
        type=MessageType.post,
        chain=Chain.SOL,
        sender="AzfsDdCQp8uqzR4ProJ7yyLGKNp9iXHt92rEoiAHG4Pw",
        signature='{"signature": "56hzHaJHH3bz1DtU6Wjhyn7eLsMCYSY4HcXkKrpwAdEHFdw7k95NPzubR3x7otstbr5JCffw81Qqpd9YUq8XJuFi","publicKey": "AzfsDdCQp8uqzR4ProJ7yyLGKNp9iXHt92rEoiAHG4Pw"}',
        item_type=ItemType.inline,
        item_content='{"type":"note","address":"AzfsDdCQp8uqzR4ProJ7yyLGKNp9iXHt92rEoiAHG4Pw","content":{"body":"this is a test!","title":"Test note","private":false,"notebook":null},"time":1610379947.771}',
        time=timestamp_to_datetime(1610379947.771),
    )


@pytest.mark.asyncio
async def test_solana_signature(solana_message: PendingPostMessage):
    connector = SolanaConnector()
    assert await connector.verify_signature(message=solana_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "signature",
    (
        '{"signature": "bad-signature","publicKey": "AzfsDdCQp8uqzR4ProJ7yyLGKNp9iXHt92rEoiAHG4Pw"}',
        '{"signature": "56hzHaJHH3bz1DtU6Wjhyn7eLsMCYSY4HcXkKrpwAdEHFdw7k95NPzubR3x7otstbr5JCffw81Qqpd9YUq8XJuFj","publicKey": "AzfsDdCQp8uqzR4ProJ7yyLGKNp9iXHt92rEoiAHG4Pw"}',
        '{"signature": "56hzHaJHH3bz1DtU6Wjhyn7eLsMCYSY4HcXkKrpwAdEHFdw7k95NPzubR3x7otstbr5JCffw81Qqpd9YUq8XJuFj","publicKey": "BzfsDdCQp8uqzR4ProJ7yyLGKNp9iXHt92rEoiAHG4Pw"}',
        '{{{{{',
        "56hzHaJHH3bz1DtU6Wjhyn7eLsMCYSY4HcXkKrpwAdEHFdw7k95NPzubR3x7otstbr5JCffw81Qqpd9YUq8XJuFj",
    ),
    ids=(
        "bad signature field",
        "bad signature, closer to the original",
        "bad public key",
        "invalid json",
        "just a signature",
    ),
)
async def test_solana_bad_signature(
    solana_message: PendingPostMessage, mocker, signature: str
):
    connector = SolanaConnector()
    logger_mock = mocker.patch("aleph.chains.solana.LOGGER")

    # Bad signature field
    solana_message.signature = signature
    assert not await connector.verify_signature(message=solana_message)
    assert not logger_mock.exception.called
