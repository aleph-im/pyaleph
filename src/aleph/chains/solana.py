import json
import logging

import base58
from nacl.exceptions import BadSignatureError
from nacl.signing import VerifyKey

from aleph_message.models import Chain
from aleph.types.db_session import DbSessionFactory
from aleph.types.chain_sync import ChainEventType, ChainEventType
from aleph.chains.common import get_verification_buffer
from aleph.schemas.pending_messages import BasePendingMessage

from configmanager import Config
from .connector import Verifier
from .chaindata import ChainDataService
from .indexer_reader import AlephIndexerReader

LOGGER = logging.getLogger("chains.solana")
CHAIN_NAME = "SOL"

class SolanaConnector(Verifier):
    def __init__(
        self, session_factory: DbSessionFactory, chain_data_service: ChainDataService
    ):
        self.indexer_reader = AlephIndexerReader(
            chain=Chain.SOL,
            session_factory=session_factory,
            chain_data_service=chain_data_service,
        )
        
    async def verify_signature(self, message: BasePendingMessage) -> bool:
        """Verifies a signature of a message, return True if verified, false if not"""

        if message.signature is None:
            LOGGER.warning("'%s': missing signature.", message.item_hash)
            return False

        try:
            signature = json.loads(message.signature)
            sigdata = base58.b58decode(signature["signature"])
            public_key = base58.b58decode(signature["publicKey"])
        except ValueError:
            LOGGER.warning("Solana signature deserialization error")
            return False

        if signature.get("version", 1) != 1:
            LOGGER.warning(
                "Unsupported signature version %s" % signature.get("version")
            )
            return False

        if message.sender != signature["publicKey"]:
            LOGGER.warning("Solana signature source error")
            return False

        try:
            verify_key = VerifyKey(public_key)
            verification_buffer = get_verification_buffer(message)
            verif = verify_key.verify(verification_buffer, signature=sigdata)
            result = verif == verification_buffer
        except BadSignatureError:
            result = False
        except Exception:
            LOGGER.exception("Solana Signature verification error")
            result = False

        return result
            
    async def fetcher(self, config: Config):
        await self.indexer_reader.fetcher(
            indexer_url=config.aleph.indexer_url.value,
            smart_contract_address=config.solana.sync_contract.value,
            event_type=ChainEventType.MESSAGE,
        )