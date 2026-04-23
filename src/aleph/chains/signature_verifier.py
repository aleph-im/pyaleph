from typing import Dict, Optional

from aleph_message.models import Chain

from aleph.chains.abc import Verifier
from aleph.chains.avalanche import AvalancheConnector
from aleph.chains.cosmos import CosmosConnector
from aleph.chains.ethereum import EthereumVerifier
from aleph.chains.evm import EVMVerifier
from aleph.chains.nuls import NulsConnector
from aleph.chains.nuls2 import Nuls2Verifier
from aleph.chains.solana import SolanaConnector
from aleph.chains.substrate import SubstrateConnector
from aleph.chains.tezos import TezosVerifier
from aleph.schemas.pending_messages import BasePendingMessage
from aleph.types.message_status import InvalidMessageFormat, InvalidSignature


class SignatureVerifier:
    verifiers: Dict[Chain, Verifier]

    def __init__(self, rpc_url: Optional[str] = None):
        # Smart wallet validation (ERC-1271 / ERC-6492) is chain-specific: the
        # wallet's EIP-712 domain uses `block.chainid`, so verifying a signature
        # from Base (or any other chain) against the Ethereum mainnet RPC would
        # produce false negatives. Until per-chain RPCs are configured, only
        # Ethereum mainnet gets the RPC-backed paths. Every other EVM chain
        # keeps the previous behavior: plain ECDSA only.
        evm = EVMVerifier()
        eth = EthereumVerifier(rpc_url=rpc_url)
        etherlink = EthereumVerifier()
        self.verifiers = {
            Chain.ARBITRUM: evm,
            Chain.AVAX: AvalancheConnector(),
            Chain.BASE: evm,
            Chain.BLAST: evm,
            Chain.BOB: evm,
            Chain.BSC: evm,
            Chain.CYBER: evm,
            Chain.CSDK: CosmosConnector(),
            Chain.DOT: SubstrateConnector(),
            Chain.ECLIPSE: SolanaConnector(),
            Chain.ETH: eth,
            Chain.ETHERLINK: etherlink,
            Chain.FRAXTAL: evm,
            Chain.HYPE: evm,
            Chain.INK: evm,
            Chain.LENS: evm,
            Chain.METIS: evm,
            Chain.MODE: evm,
            Chain.NEO: evm,
            Chain.NULS: NulsConnector(),
            Chain.NULS2: Nuls2Verifier(),
            Chain.LINEA: evm,
            Chain.LISK: evm,
            Chain.OPTIMISM: evm,
            Chain.POL: evm,
            Chain.SOL: SolanaConnector(),
            Chain.SONIC: evm,
            Chain.UNICHAIN: evm,
            Chain.TEZOS: TezosVerifier(),
            Chain.WORLDCHAIN: evm,
            Chain.ZORA: evm,
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
