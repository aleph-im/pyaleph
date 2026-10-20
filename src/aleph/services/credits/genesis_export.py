"""
Genesis export (spec 5.5, 11): snapshots the legacy credit lots as of the
genesis period into the file every CCN loads at cutover
(`lifecycle.load_genesis`).

The lots are rebuilt with the legacy FIFO replay of `aleph.repair`
(`credit_history_records` + `replay_credit_lots`, the very code that rebuilds
the `credit_balances` cache), restricted to the history observed before the
period, so they equal what the legacy balance reports at that instant.
"""

import datetime as dt
import hashlib
import json
from typing import Any, Dict, Iterable, List, Tuple

from sqlalchemy import select

from aleph.db.models import AlephCreditHistoryDb
from aleph.repair import credit_history_records, replay_credit_lots
from aleph.services.credits.addresses import normalize_address
from aleph.types.db_session import DbSession

REASON_DENYLIST = "denylist"
REASON_LATE_POST = "late_post"


def _seconds(value: dt.datetime) -> int:
    return int(value.timestamp())


def export_genesis(
    session: DbSession, period: dt.datetime, denylist: Iterable[str]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    (kept, excluded) at `period` (an hour start).

    `kept` is the genesis list `[{address, amount, expiration, ref}]`, with
    normalized addresses, expiration in unix seconds (or None), lots of the
    same address and expiration merged (amounts summed, `ref` of the first lot
    by `(ref, index)`), sorted by `(address, ref, index)`.

    `excluded` lists what the snapshot drops, with a `reason`: remaining lots
    of denylisted addresses, and grants observed at or after `period` (posts
    after the legacy service stopped).
    """
    denied = {normalize_address(a.strip()) for a in denylist if a.strip()}
    addresses = session.execute(
        select(AlephCreditHistoryDb.address).distinct()
    ).scalars()

    # (address, expiration seconds) -> [amount, first (ref, index)]
    merged: Dict[Tuple[str, Any], List[Any]] = {}
    excluded: List[Dict[str, Any]] = []

    for raw_address in sorted(addresses):
        address = normalize_address(raw_address)
        records = credit_history_records(session, raw_address)
        before = [(r, ts) for r, ts in records if ts < period]
        for record, ts in records:
            if ts >= period and record.amount > 0:
                excluded.append(
                    {
                        "address": address,
                        "amount": int(record.amount),
                        "expiration": (
                            None
                            if record.expiration_date is None
                            else _seconds(record.expiration_date)
                        ),
                        "ref": record.credit_ref,
                        "index": record.credit_index,
                        "reason": REASON_LATE_POST,
                    }
                )
        for lot in replay_credit_lots(before):
            expiration = lot["expiration_date"]
            if lot["amount_remaining"] <= 0:
                continue
            if expiration is not None and expiration <= period:
                continue
            exp_seconds = None if expiration is None else _seconds(expiration)
            ref = (lot["credit_ref"], lot["credit_index"])
            if address in denied:
                excluded.append(
                    {
                        "address": address,
                        "amount": lot["amount_remaining"],
                        "expiration": exp_seconds,
                        "ref": ref[0],
                        "index": ref[1],
                        "reason": REASON_DENYLIST,
                    }
                )
                continue
            entry = merged.setdefault((address, exp_seconds), [0, ref])
            entry[0] += lot["amount_remaining"]
            entry[1] = min(entry[1], ref)

    ordered = sorted(
        (
            (address, ref, exp, amount)
            for (address, exp), (amount, ref) in merged.items()
        ),
        key=lambda item: (item[0], item[1], -1 if item[2] is None else item[2]),
    )
    kept = [
        {"address": address, "amount": amount, "expiration": exp, "ref": ref[0]}
        for address, ref, exp, amount in ordered
    ]
    excluded.sort(key=lambda e: (e["address"], e["ref"], e["index"]))
    return kept, excluded


def genesis_bytes(kept: List[Dict[str, Any]]) -> bytes:
    """Deterministic encoding of the genesis list: compact JSON, keys sorted."""
    return json.dumps(kept, sort_keys=True, separators=(",", ":")).encode("utf-8")


def genesis_hash(content: bytes) -> str:
    """
    The value for `credits.genesis.hash` once the file is uploaded as a STORE
    (storage engine): the sha256 of the file bytes, which is the item hash the
    node fetches it by.
    """
    return hashlib.sha256(content).hexdigest()
