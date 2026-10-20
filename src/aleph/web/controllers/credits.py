"""
Read-only endpoints over the credit ledger v2 (spec 10). They read the v2
tables whatever `aleph.credits.serve_v2` says, so they also serve the shadow
run. No authentication: everything they return is derived from public inputs.
"""

import datetime as dt
import logging
from collections import defaultdict
from decimal import Decimal
from typing import Dict, Optional

from aiohttp import web
from pydantic import ValidationError

from aleph.db.accessors.credit_ledger import (
    attestation_sender,
    count_exclusions,
    count_topups_by_address,
    get_exclusions,
    get_fold_state,
    get_topups_by_address,
    global_checkpoint_hash,
    period_kind_totals,
    period_quotes,
    revenue_sums,
    selected_attestation,
)
from aleph.schemas.api.credits import (
    MAX_REVENUE_PERIODS,
    CreditAttestationRef,
    CreditExclusionItem,
    CreditExclusionsQueryParams,
    CreditExclusionsResponse,
    CreditKindTotal,
    CreditPeriodResponse,
    CreditQuoteQueryParams,
    CreditQuoteResponse,
    CreditRevenueNode,
    CreditRevenuePeriod,
    CreditRevenueQueryParams,
    CreditRevenueResponse,
    CreditTopupItem,
    CreditTopupsQueryParams,
    CreditTopupsResponse,
)
from aleph.services.credits.addresses import normalize_address
from aleph.services.credits.fold import period_hash
from aleph.services.credits.periods import PERIOD, floor_hour
from aleph.services.credits.topups import QuoteNotConfigured
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSessionFactory
from aleph.web.controllers.app_state_getters import (
    get_credit_quoter_from_request,
    get_session_factory_from_request,
)

LOGGER = logging.getLogger(__name__)


def _unix(value: dt.datetime) -> int:
    return int(value.timestamp())


def _hour(timestamp: int, name: str) -> dt.datetime:
    """The UTC datetime of an hour-aligned unix timestamp; 400 otherwise."""
    if timestamp % 3600 != 0:
        raise web.HTTPBadRequest(
            text=f"{name} must be a unix timestamp aligned on an hour"
        )
    try:
        return dt.datetime.fromtimestamp(timestamp, tz=dt.timezone.utc)
    except (OverflowError, OSError, ValueError):
        raise web.HTTPUnprocessableEntity(text=f"{name} is out of range")


def _json(model) -> web.Response:
    return web.json_response(text=model.model_dump_json())


# --- /credits/periods/{period} --------------------------------------------------


async def get_credit_period(request: web.Request) -> web.Response:
    """
    Hash, totals and attestation of one credit period, so operators can diff
    CCNs hour by hour.

    `status` is `closed` (folded), `dirty` (folded, but an input changed:
    the next fold run refolds it from `dirty_from_period`) or `open` (not
    folded yet). `period_hash` is the hash of the period's ledger rows as they
    are now (null while open); `checkpoint_hash` is the global checkpoint hash
    written at that period (null when the period is not a checkpoint period).

    ---
    summary: Get a credit ledger period
    tags:
      - Credits
    parameters:
      - name: period
        in: path
        required: true
        schema:
          type: integer
        description: Period start, unix seconds, hour-aligned
    responses:
      '200':
        description: The period
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/CreditPeriodResponse'
      '400':
        description: Period not aligned on an hour
      '422':
        description: Validation error
    """
    raw = request.match_info.get("period", "")
    try:
        timestamp = int(raw)
    except ValueError:
        raise web.HTTPUnprocessableEntity(text="period must be an integer")
    if timestamp < 0:
        raise web.HTTPUnprocessableEntity(text="period must not be negative")
    period = _hour(timestamp, "period")

    session_factory: DbSessionFactory = get_session_factory_from_request(request)
    with session_factory() as session:
        state = get_fold_state(session)
        last_closed = state.last_closed_period
        dirty_from = state.dirty_from_period
        if last_closed is None or period > last_closed:
            status = "open"
        elif dirty_from is not None and period >= dirty_from:
            status = "dirty"
        else:
            status = "closed"

        attestation = None
        current_hash = None
        if status != "open":
            current_hash = period_hash(session, period)
            item_hash = selected_attestation(session, period)
            if item_hash is not None:
                attestation = CreditAttestationRef(
                    item_hash=item_hash, sender=attestation_sender(session, item_hash)
                )
        response = CreditPeriodResponse(
            period=timestamp,
            status=status,
            period_hash=current_hash,
            checkpoint_hash=global_checkpoint_hash(session, period),
            attestation=attestation,
            totals={
                kind: CreditKindTotal(rows=rows, amount=amount)
                for kind, (rows, amount) in sorted(
                    period_kind_totals(session, period).items()
                )
            },
        )
    return _json(response)


# --- /credits/revenue ---------------------------------------------------------


async def get_credit_revenue(request: web.Request) -> web.Response:
    """
    Per-period credit revenue for node rewards (aleph-nodestatus).

    `from` (inclusive) and `to` (exclusive) are hour-aligned unix seconds
    (400 otherwise), at most 744 periods apart, and `to` is at most the
    current hour start (422 otherwise). Only closed periods are returned:
    those before the end of the fold's last closed period. A period at or
    after `dirty_from_period` (waiting for a refold) is returned as it was
    folded; its figures can still change when the refold runs. Every closed
    period of the range is listed, with zeros when it has no revenue.

    Per node: `execution` is the sum of the billed credit-paid `execution`
    rows, `hold_equivalent` the credit-equivalent of the hold-paid VMs it
    ran. `storage` is the network total of the `storage` rows,
    `execution_total` the sum of every node's `execution`. Exclusion rows
    never count. `credit_price_aleph` is the ALEPH per credit quote stored
    when the period was closed (decimal string), null if unquoted.

    ---
    summary: Get credit revenue per period
    tags:
      - Credits
    parameters:
      - name: from
        in: query
        required: true
        schema:
          type: integer
        description: First period, unix seconds, hour-aligned, inclusive
      - name: to
        in: query
        required: true
        schema:
          type: integer
        description: End of the range, unix seconds, hour-aligned, exclusive
    responses:
      '200':
        description: Revenue per closed period
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/CreditRevenueResponse'
      '400':
        description: Bounds not aligned on an hour
      '422':
        description: Validation error, range over 744 periods or in the future
    """
    try:
        params = CreditRevenueQueryParams.model_validate(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(text=e.json())
    start = _hour(params.from_, "from")
    end = _hour(params.to, "to")
    if end < start:
        raise web.HTTPUnprocessableEntity(text="to must not be before from")
    if end - start > MAX_REVENUE_PERIODS * PERIOD:
        raise web.HTTPUnprocessableEntity(
            text=f"at most {MAX_REVENUE_PERIODS} periods per request"
        )
    if end > floor_hour(utc_now()):
        raise web.HTTPUnprocessableEntity(
            text="to must not be after the current hour start"
        )

    session_factory: DbSessionFactory = get_session_factory_from_request(request)
    with session_factory() as session:
        last_closed = get_fold_state(session).last_closed_period
        if last_closed is None:
            return _json(CreditRevenueResponse(periods=[]))
        end = min(end, last_closed + PERIOD)
        sums = revenue_sums(session, start, end) if start < end else []
        quotes = period_quotes(session, start, end) if start < end else {}

    nodes: Dict[dt.datetime, Dict[str, Dict[str, int]]] = defaultdict(dict)
    storage: Dict[dt.datetime, int] = defaultdict(int)
    for row in sums:
        amount = int(row.amount)
        if row.kind == "storage":
            storage[row.period] += -amount
            continue
        if row.node_id is None:
            continue
        node = nodes[row.period].setdefault(
            row.node_id, {"execution": 0, "hold_equivalent": 0}
        )
        if row.kind == "execution":
            node["execution"] += -amount
        else:
            node["hold_equivalent"] += amount

    periods = []
    period = start
    while period < end:
        by_node = nodes.get(period, {})
        quote: Optional[Decimal] = quotes.get(period)
        periods.append(
            CreditRevenuePeriod(
                period=_unix(period),
                credit_price_aleph=None if quote is None else str(quote),
                nodes={
                    node_id: CreditRevenueNode(**values)
                    for node_id, values in sorted(by_node.items())
                },
                storage=storage.get(period, 0),
                execution_total=sum(v["execution"] for v in by_node.values()),
            )
        )
        period += PERIOD
    return _json(CreditRevenueResponse(periods=periods))


# --- /credits/exclusions ------------------------------------------------------


async def get_credit_exclusions(request: web.Request) -> web.Response:
    """
    Ledger rows the fold decided not to bill (amount 0, `excluded_reason`
    set), newest period first.

    ---
    summary: List credit ledger exclusions
    tags:
      - Credits
    parameters:
      - name: period
        in: query
        schema:
          type: integer
        description: Period start, unix seconds, hour-aligned
      - name: reason
        in: query
        schema:
          type: string
      - name: address
        in: query
        schema:
          type: string
      - name: page
        in: query
        schema:
          type: integer
          default: 1
          minimum: 1
      - name: pagination
        in: query
        schema:
          type: integer
          default: 100
          minimum: 1
          maximum: 1000
    responses:
      '200':
        description: Exclusion rows
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/CreditExclusionsResponse'
      '400':
        description: Period not aligned on an hour
      '422':
        description: Validation error
    """
    try:
        params = CreditExclusionsQueryParams.model_validate(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(text=e.json())
    period = None if params.period is None else _hour(params.period, "period")
    address = None if params.address is None else normalize_address(params.address)

    session_factory: DbSessionFactory = get_session_factory_from_request(request)
    with session_factory() as session:
        rows = get_exclusions(
            session,
            period=period,
            reason=params.reason,
            address=address,
            page=params.page,
            pagination=params.pagination,
        )
        total = count_exclusions(
            session, period=period, reason=params.reason, address=address
        )
        items = [
            CreditExclusionItem(
                period=_unix(row.period),
                address=row.address,
                kind=row.kind,
                business_key=row.business_key,
                amount=int(row.amount),
                resource=row.resource,
                node_id=row.node_id,
                seconds=row.seconds,
                rate=None if row.rate is None else str(row.rate),
                source_ref=row.source_ref,
                excluded_reason=row.excluded_reason or "",
            )
            for row in rows
        ]
    return _json(
        CreditExclusionsResponse(
            exclusions=items,
            pagination_page=params.page,
            pagination_per_page=params.pagination,
            pagination_total=total,
        )
    )


# --- /credits/quote -----------------------------------------------------------


def _token_address(quoter, token: str) -> str:
    if token == "usdc":
        return quoter.cfg.usdc
    if token == "aleph":
        return quoter.cfg.aleph
    return "eth"


async def get_credit_quote(request: web.Request) -> web.Response:
    """
    Credits a top-up of `amount` raw units of `token` would buy now: the quote
    the fold would apply (bonus included), evaluated at the latest finalized
    block. Advisory only: a top-up is quoted at its own block.

    ---
    summary: Quote a credit top-up
    tags:
      - Credits
    parameters:
      - name: token
        in: query
        required: true
        schema:
          type: string
          enum: [usdc, eth, aleph]
      - name: amount
        in: query
        required: true
        schema:
          type: string
        description: Raw token amount (1e-6 USDC, wei)
    responses:
      '200':
        description: The quote
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/CreditQuoteResponse'
      '422':
        description: Validation error
      '503':
        description: The node cannot quote (no RPC or no ALEPH pool configured)
    """
    try:
        params = CreditQuoteQueryParams.model_validate(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(text=e.json())

    quoter = get_credit_quoter_from_request(request)
    if quoter is None:
        raise web.HTTPServiceUnavailable(text="credit quotes are not available")
    try:
        block = await quoter.web3.eth.get_block("finalized")
        block_number = int(block["number"])
        num, den = await quoter.quote(
            _token_address(quoter, params.token),
            params.amount,
            block_number,
            apply_bonus=True,
        )
    except QuoteNotConfigured as error:
        raise web.HTTPServiceUnavailable(text=str(error))
    except Exception as error:
        LOGGER.warning("Credit quote for %s failed: %s", params.token, error)
        raise web.HTTPServiceUnavailable(text="the quote failed, retry later")

    return _json(
        CreditQuoteResponse(
            credits=params.amount * num // den,
            quote_num=str(num),
            quote_den=str(den),
            block_number=block_number,
        )
    )


# --- /credits/topups ----------------------------------------------------------


def _decimal_str(value: Optional[Decimal]) -> Optional[str]:
    return None if value is None else str(int(value))


async def get_credit_topups(request: web.Request) -> web.Response:
    """
    The indexed on-chain top-ups sent by an address, newest first, with their
    status (`credited`, `unquoted`, `zero`, `excluded_*`), so a user can see
    why a transfer did or did not credit.

    ---
    summary: List the top-ups of an address
    tags:
      - Credits
    parameters:
      - name: address
        in: query
        required: true
        schema:
          type: string
      - name: page
        in: query
        schema:
          type: integer
          default: 1
          minimum: 1
      - name: pagination
        in: query
        schema:
          type: integer
          default: 100
          minimum: 1
          maximum: 1000
    responses:
      '200':
        description: Top-ups of the address
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/CreditTopupsResponse'
      '422':
        description: Validation error
    """
    try:
        params = CreditTopupsQueryParams.model_validate(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(text=e.json())
    address = normalize_address(params.address)

    session_factory: DbSessionFactory = get_session_factory_from_request(request)
    with session_factory() as session:
        rows = get_topups_by_address(
            session, address, page=params.page, pagination=params.pagination
        )
        total = count_topups_by_address(session, address)
        items = [
            CreditTopupItem(
                chain_id=row.chain_id,
                tx_hash=row.tx_hash,
                log_index=row.log_index,
                block_number=row.block_number,
                block_time=_unix(row.block_time),
                from_address=row.from_address,
                token=row.token,
                raw_amount=str(int(row.raw_amount)),
                quote_num=_decimal_str(row.quote_num),
                quote_den=_decimal_str(row.quote_den),
                credits=row.credits,
                status=row.status,
                ledger_period=(
                    None if row.ledger_period is None else _unix(row.ledger_period)
                ),
            )
            for row in rows
        ]
    return _json(
        CreditTopupsResponse(
            address=address,
            topups=items,
            pagination_page=params.page,
            pagination_per_page=params.pagination,
            pagination_total=total,
        )
    )
