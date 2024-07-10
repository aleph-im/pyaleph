from typing import Dict

from aleph_message.models import Chain

from aleph.chains.abc import Verifier
from aleph.chains.avalanche import AvalancheConnector
from aleph.chains.ethereum import EthereumVerifier
from aleph.chains.nuls import NulsConnector
from aleph.chains.nuls2 import Nuls2Verifier
from aleph.chains.solana import SolanaConnector
from aleph.chains.substrate import SubstrateConnector
from aleph.chains.tezos import TezosVerifier
from aleph.schemas.pending_messages import BasePendingMessage
from aleph.types.message_status import InvalidMessageFormat, InvalidSignature


class SignatureVerifier:
    verifiers: Dict[Chain, Verifier]

    def __init__(self):
        self.verifiers = {
            Chain.AVAX: AvalancheConnector(),
            Chain.DOT: SubstrateConnector(),
            Chain.ETH: EthereumVerifier(),
            Chain.NULS: NulsConnector(),
            Chain.NULS2: Nuls2Verifier(),
            Chain.SOL: SolanaConnector(),
            Chain.TEZOS: TezosVerifier(),
        }

    async def verify_signature(self, message: BasePendingMessage) -> None:
        try:
            verifier = self.verifiers[message.chain]
        except KeyError:
            raise InvalidMessageFormat(f"Unknown chain for validation: {message.chain}")

        try:
            if await verifier.verify_signature(message):
                return
            else:
                raise InvalidSignature("The signature of the message is invalid")

        except ValueError as e:
            raise InvalidSignature(f"Signature validation error: {str(e)}")
