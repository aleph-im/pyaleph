"""
On-chain credit top-up indexer.

Deposits are read straight from Ethereum: topic-filtered `eth_getLogs` on
finalized blocks, priced with `eth_call`s pinned to the transfer's block. All
credit arithmetic is integer (quotes are numerator/denominator pairs).

The ALEPH/WETH TWAP price is a fixed-point integer scaled by PRICE_SCALE
(10**36), computed with a fixed Decimal context (prec 80, ROUND_FLOOR). The
floor rounding is part of the deterministic contract: every node must produce
the same quote. Quotes stay within the NUMERIC(78,0) columns.
"""

import datetime as dt
import logging
from dataclasses import dataclass, field
from decimal import ROUND_FLOOR, Context, Decimal
from typing import Any, Dict, List, Optional, Tuple

from web3 import AsyncWeb3, Web3

from aleph.db.accessors.credit_ledger import mark_dirty, unquoted_topups, upsert_topup
from aleph.db.models.credit_ledger import CreditFoldStateDb, CreditTopupDb
from aleph.services.credits.periods import floor_hour
from aleph.types.db_session import DbSession

LOGGER = logging.getLogger(__name__)

TRANSFER_TOPIC = "0x" + Web3.keccak(
    text="Transfer(address,address,uint256)"
).hex().removeprefix("0x")
PROCESS_SELECTOR = (
    Web3.keccak(text="process(address,uint128,uint128,uint48)")
    .hex()
    .removeprefix("0x")[:8]
)
# The two admin functions of AlephPaymentProcessor.sol.
ADMIN_SIGNATURES = (
    "process(address,uint128,uint128,uint48)",
    "withdraw(address,address,uint128)",
)
EXCLUDED_SELECTORS = {
    Web3.keccak(text=sig).hex().removeprefix("0x")[:8] for sig in ADMIN_SIGNATURES
}

CHAINLINK_ABI = [
    {
        "name": "latestRoundData",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [
            {"name": "roundId", "type": "uint80"},
            {"name": "answer", "type": "int256"},
            {"name": "startedAt", "type": "uint256"},
            {"name": "updatedAt", "type": "uint256"},
            {"name": "answeredInRound", "type": "uint80"},
        ],
    }
]
POOL_ABI = [
    {
        "name": "observe",
        "type": "function",
        "stateMutability": "view",
        "inputs": [{"name": "secondsAgos", "type": "uint32[]"}],
        "outputs": [
            {"name": "tickCumulatives", "type": "int56[]"},
            {"name": "secondsPerLiquidityCumulativeX128s", "type": "uint160[]"},
        ],
    },
    {
        "name": "token0",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [{"name": "", "type": "address"}],
    },
]

TWAP_SECONDS = 1800
# Chainlink ETH/USD: 8 decimals. 1 credit = 1e-6 USD, 1 wei = 1e-18 ETH.
CREDITS_PER_WEI_DEN = 10**20
PRICE_SCALE = 10**36
TICK_CONTEXT = Context(prec=80, rounding=ROUND_FLOOR)
MAX_QUOTE = 10**77  # NUMERIC(78, 0)


@dataclass
class TopupConfig:
    chain_id: int
    start_block: int
    contract: str
    usdc: str
    aleph: str
    chainlink_eth_usd: str
    aleph_weth_pool: Optional[str]
    denylist: List[str] = field(default_factory=list)
    chunk_blocks: int = 2000
    aleph_bonus: Decimal = Decimal("0.2")

    @classmethod
    def from_config(cls, config: Any) -> "TopupConfig":
        chain = config.aleph.credits.chain
        start_block = chain.start_block.value
        if start_block is None:
            raise ValueError("aleph.credits.chain.start_block must be set")
        return cls(
            chain_id=int(config.ethereum.chain_id.value),
            start_block=int(start_block),
            contract=chain.contract.value,
            usdc=chain.usdc.value,
            aleph=chain.aleph.value,
            chainlink_eth_usd=chain.chainlink_eth_usd.value,
            aleph_weth_pool=chain.aleph_weth_pool.value,
            denylist=list(chain.denylist.value),
            chunk_blocks=int(chain.chunk_blocks.value),
            aleph_bonus=Decimal(str(config.aleph.credits.topup_bonus.aleph.value)),
        )


def _hex(value: Any) -> str:
    if isinstance(value, str):
        h = value
    else:
        h = bytes(value).hex()
    h = h.lower()
    return h if h.startswith("0x") else "0x" + h


def _pad32(address: str) -> str:
    return "0x" + address.lower().removeprefix("0x").rjust(64, "0")


def _topic_address(topic: Any) -> str:
    return Web3.to_checksum_address("0x" + _hex(topic)[-40:])


class TopupIndexer:
    def __init__(self, web3: AsyncWeb3, cfg: TopupConfig):
        self.web3 = web3
        self.cfg = cfg
        self._denylist = {a.lower() for a in cfg.denylist}
        self._token0_cache: Dict[str, str] = {}

    # -- classification -------------------------------------------------

    def _is_eth(self, log: Dict[str, Any]) -> bool:
        return str(log["address"]).lower() == self.cfg.contract.lower()

    def classify(self, log: Dict[str, Any], tx: Optional[Dict[str, Any]]) -> str:
        sender = _topic_address(log["topics"][1]).lower()
        if sender in self._denylist:
            return "excluded_denylist"
        if tx is not None and tx.get("to") is not None:
            if str(tx["to"]).lower() == self.cfg.contract.lower():
                selector = _hex(tx.get("input", "0x"))[2:10]
                if selector in EXCLUDED_SELECTORS:
                    return "excluded_process_tx"
        return "included"

    # -- quotes ---------------------------------------------------------

    async def _eth_usd_answer(self, block_number: int) -> int:
        feed = self.web3.eth.contract(
            address=Web3.to_checksum_address(self.cfg.chainlink_eth_usd),
            abi=CHAINLINK_ABI,
        )
        data = await feed.functions.latestRoundData().call(
            block_identifier=block_number
        )
        answer = int(data[1])
        if answer <= 0:
            raise ValueError("non-positive Chainlink answer")
        return answer

    async def _token0(self, pool_address: str, block_number: int) -> str:
        key = pool_address.lower()
        if key not in self._token0_cache:
            pool = self.web3.eth.contract(
                address=Web3.to_checksum_address(pool_address), abi=POOL_ABI
            )
            self._token0_cache[key] = str(
                await pool.functions.token0().call(block_identifier=block_number)
            ).lower()
        return self._token0_cache[key]

    async def _aleph_weth_price(self, block_number: int) -> Tuple[int, int]:
        """WETH raw units per ALEPH raw unit, as (num, den) with a PRICE_SCALE side."""
        if not self.cfg.aleph_weth_pool:
            raise ValueError("aleph_weth_pool is not configured")
        pool = self.web3.eth.contract(
            address=Web3.to_checksum_address(self.cfg.aleph_weth_pool), abi=POOL_ABI
        )
        cumulatives, _ = await pool.functions.observe([TWAP_SECONDS, 0]).call(
            block_identifier=block_number
        )
        token0 = await self._token0(self.cfg.aleph_weth_pool, block_number)
        # Python floors toward -inf, like the Uniswap oracle libraries.
        tick = (int(cumulatives[1]) - int(cumulatives[0])) // TWAP_SECONDS
        # token1 raw per token0 raw, scaled; floored.
        price_scaled = int(
            (
                TICK_CONTEXT.power(Decimal("1.0001"), tick) * PRICE_SCALE
            ).to_integral_value(rounding=ROUND_FLOOR)
        )
        if price_scaled <= 0:
            raise ValueError(f"tick {tick} out of the supported price range")
        # Both tokens have 18 decimals, so raw ratios equal token ratios.
        if token0 == self.cfg.aleph.lower():
            return price_scaled, PRICE_SCALE
        return PRICE_SCALE, price_scaled

    async def quote(
        self,
        token: str,
        raw_amount: int,
        block_number: int,
        apply_bonus: bool = True,
    ) -> Tuple[int, int]:
        """Credits per raw unit as (numerator, denominator), pinned to the block."""
        t = token.lower()
        if t == self.cfg.usdc.lower():
            return 1, 1
        if t in ("eth", self.cfg.contract.lower()):
            answer = await self._eth_usd_answer(block_number)
            return answer, CREDITS_PER_WEI_DEN
        if t == self.cfg.aleph.lower():
            p_num, p_den = await self._aleph_weth_price(block_number)
            answer = await self._eth_usd_answer(block_number)
            num, den = p_num * answer, p_den * CREDITS_PER_WEI_DEN
            if max(num * 6, den * 6) >= MAX_QUOTE:
                raise ValueError("ALEPH quote out of the storable range")
            if apply_bonus:
                b_num, b_den = self.cfg.aleph_bonus.as_integer_ratio()
                num, den = num * (b_den + b_num), den * b_den
            return num, den
        raise ValueError(f"unknown token {token}")

    # -- indexing -------------------------------------------------------

    async def _block_time(self, block_number: int, cache: Dict[int, dt.datetime]):
        if block_number not in cache:
            block = await self.web3.eth.get_block(block_number)
            cache[block_number] = dt.datetime.fromtimestamp(
                block["timestamp"], tz=dt.timezone.utc
            )
        return cache[block_number]

    async def _fetch_logs(self, from_block: int, to_block: int) -> List[Dict[str, Any]]:
        base = {"fromBlock": from_block, "toBlock": to_block}
        contract = Web3.to_checksum_address(self.cfg.contract)
        filters = [
            {
                **base,
                "address": Web3.to_checksum_address(token),
                "topics": [TRANSFER_TOPIC, None, _pad32(self.cfg.contract)],
            }
            for token in (self.cfg.usdc, self.cfg.aleph)
        ]
        filters.append({**base, "address": contract, "topics": [TRANSFER_TOPIC]})
        logs: List[Dict[str, Any]] = []
        for flt in filters:
            logs.extend(await self.web3.eth.get_logs(flt))  # type: ignore[arg-type]
        logs.sort(key=lambda lg: (lg["blockNumber"], lg["logIndex"]))
        return logs

    async def _build_row(
        self, log: Dict[str, Any], block_times: Dict[int, dt.datetime]
    ) -> CreditTopupDb:
        block_number = int(log["blockNumber"])
        block_time = await self._block_time(block_number, block_times)
        raw_amount = int(_hex(log["data"]), 16)
        is_eth = self._is_eth(log)
        row = CreditTopupDb(
            chain_id=self.cfg.chain_id,
            tx_hash=_hex(log["transactionHash"]),
            log_index=int(log["logIndex"]),
            block_number=block_number,
            block_time=block_time,
            from_address=_topic_address(log["topics"][1]),
            token="ETH" if is_eth else Web3.to_checksum_address(log["address"]),
            raw_amount=Decimal(raw_amount),
            quote_num=None,
            quote_den=None,
            credits=None,
            status="",
            ledger_period=None,
        )
        sender = row.from_address.lower()
        if sender in self._denylist:
            row.status = "excluded_denylist"
            return row
        # Not guarded on purpose: a lookup failure is a transient RPC error, so
        # the chunk aborts and is retried on the next run.
        tx = await self.web3.eth.get_transaction(log["transactionHash"])
        status = self.classify(log, dict(tx))
        if status != "included":
            row.status = status
            return row
        if raw_amount == 0:
            row.status = "zero"
            return row
        try:
            num, den = await self.quote(row.token, raw_amount, block_number)
        except Exception:
            LOGGER.exception("Could not quote top-up %s", row.tx_hash)
            row.status = "unquoted"
            return row
        self._apply_quote(row, raw_amount, num, den)
        return row

    @staticmethod
    def _apply_quote(row: CreditTopupDb, raw_amount: int, num: int, den: int) -> None:
        row.quote_num = Decimal(num)
        row.quote_den = Decimal(den)
        row.credits = raw_amount * num // den
        row.status = "credited" if row.credits > 0 else "zero"
        row.ledger_period = floor_hour(row.block_time) if row.credits > 0 else None

    async def index(self, session: DbSession, state: CreditFoldStateDb) -> int:
        finalized = int((await self.web3.eth.get_block("finalized"))["number"])
        cursor = (
            state.chain_cursor_block
            if state.chain_cursor_block is not None
            else self.cfg.start_block - 1
        )
        new_rows = 0
        while cursor < finalized:
            end = min(finalized, cursor + self.cfg.chunk_blocks)
            block_times: Dict[int, dt.datetime] = {}
            for log in await self._fetch_logs(cursor + 1, end):
                row = await self._build_row(log, block_times)
                if upsert_topup(session, row):
                    new_rows += 1
                    if row.status == "credited" and row.ledger_period is not None:
                        mark_dirty(session, row.ledger_period)
            state.chain_cursor_time = await self._block_time(end, block_times)
            state.chain_cursor_block = end
            session.flush()
            cursor = end
        return new_rows

    async def requote_unquoted(self, session: DbSession) -> int:
        fixed = 0
        for row in unquoted_topups(session):
            raw_amount = int(row.raw_amount)
            try:
                num, den = await self.quote(row.token, raw_amount, row.block_number)
            except Exception:
                LOGGER.warning("Top-up %s is still unquotable", row.tx_hash)
                continue
            self._apply_quote(row, raw_amount, num, den)
            if row.status == "credited":
                mark_dirty(session, floor_hour(row.block_time))
            fixed += 1
        session.flush()
        return fixed
