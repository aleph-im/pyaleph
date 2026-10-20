import datetime as dt
import json
from types import SimpleNamespace

import pytest
from aleph_message.models import Chain, ItemType, MessageType
from sqlalchemy import select

from aleph.db.accessors.credit_ledger import get_fold_state
from aleph.db.models import AlephCreditHistoryDb, MessageDb
from aleph.db.models.credit_ledger import CreditLotDb
from aleph.services.credits.genesis_export import (
    export_genesis,
    genesis_bytes,
    genesis_hash,
)
from aleph.services.credits.lifecycle import load_genesis
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory

PERIOD = dt.datetime(2026, 10, 20, 12, tzinfo=dt.timezone.utc)
HOUR = dt.timedelta(hours=1)
DAY = dt.timedelta(days=1)

ALICE = "0xAbCdEf0000000000000000000000000000000001"
ALICE_LOWER = ALICE.lower()
BOB = "0xabcdef0000000000000000000000000000000002"
POOL = "0x000000000004444c5dc75cB358380D2e3dE08A90"
LATE = "late-post"
EARLY = "early-post"


def row(address, amount, ref, index, ts, expiration=None):
    return AlephCreditHistoryDb(
        address=address,
        amount=amount,
        credit_ref=ref,
        credit_index=index,
        message_timestamp=ts,
        expiration_date=expiration,
    )


def message(item_hash, confirmed_at):
    return MessageDb(
        item_hash=item_hash,
        chain=Chain.ETH,
        sender="0x0000000000000000000000000000000000000001",
        signature="0xsig",
        item_type=ItemType.inline,
        type=MessageType.post,
        content={},
        size=1,
        time=confirmed_at,
        channel=Channel("TEST"),
        first_confirmed_at=confirmed_at,
        reception_time=confirmed_at,
    )


def seed(session):
    t0 = PERIOD - 10 * DAY
    session.add_all(
        [
            message(EARLY, t0),
            message(LATE, PERIOD + HOUR),
            # Alice: checksummed and lowercase forms merge; a non-expiring lot
            # of 1000 drained by 300; a lot that expired before the period.
            row(ALICE, 1000, EARLY, 0, t0),
            row(ALICE, -300, "exp-1", 0, t0 + HOUR),
            row(ALICE_LOWER, 50, EARLY, 1, t0),
            row(ALICE, 70, EARLY, 2, t0, expiration=PERIOD + DAY),
            row(ALICE_LOWER, 20, EARLY, 3, t0, expiration=PERIOD + DAY),
            row(ALICE, 999, EARLY, 4, t0, expiration=PERIOD - HOUR),
            # Bob: an expense after the period must not drain the snapshot.
            row(BOB, 500, EARLY, 5, t0),
            row(BOB, -500, "exp-late", 0, PERIOD + HOUR),
            # Denylisted pool, and a grant confirmed after the stop.
            row(POOL, 10**9, EARLY, 6, t0),
            row(ALICE, 777, LATE, 0, t0),
        ]
    )
    session.commit()


def test_export_genesis_excludes_denylist_and_late_posts(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        seed(session)
        kept, excluded = export_genesis(session, PERIOD, [POOL.lower()])

    expiry = int((PERIOD + DAY).timestamp())
    assert kept == [
        {"address": ALICE_LOWER, "amount": 750, "expiration": None, "ref": EARLY},
        {"address": ALICE_LOWER, "amount": 90, "expiration": expiry, "ref": EARLY},
        {"address": BOB, "amount": 500, "expiration": None, "ref": EARLY},
    ]
    assert [(e["reason"], e["address"], e["amount"]) for e in excluded] == [
        ("denylist", POOL.lower(), 10**9),
        ("late_post", ALICE_LOWER, 777),
        # A debit after the period is listed too, never dropped silently.
        ("late_post", BOB, -500),
    ]


def test_export_genesis_floors_expirations_to_seconds(
    session_factory: DbSessionFactory,
):
    t0 = PERIOD - DAY
    with session_factory() as session:
        session.add_all(
            [
                message(EARLY, t0),
                # Expires within the second after the period: exported with
                # an expiration of exactly the period, it would be expired at
                # load, so it is not exported.
                row(
                    ALICE,
                    10,
                    EARLY,
                    0,
                    t0,
                    expiration=PERIOD + dt.timedelta(milliseconds=500),
                ),
                row(
                    ALICE, 20, EARLY, 1, t0, expiration=PERIOD + dt.timedelta(seconds=1)
                ),
            ]
        )
        session.commit()
        kept, excluded = export_genesis(session, PERIOD, [])

    assert kept == [
        {
            "address": ALICE_LOWER,
            "amount": 20,
            "expiration": int(PERIOD.timestamp()) + 1,
            "ref": EARLY,
        }
    ]
    assert excluded == []


def _load_script():
    import importlib.util
    from pathlib import Path

    path = Path(__file__).parent.parent / "scripts" / "credits_export_genesis.py"
    spec = importlib.util.spec_from_file_location("credits_export_genesis", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_export_script_adds_cli_denylist_to_the_config_one(mocker, tmp_path, capsys):
    import aleph.config

    script = _load_script()
    mocker.patch.object(script, "make_engine")
    mocker.patch.object(script, "make_session_factory")
    export = mocker.patch.object(script, "export_genesis", return_value=([], []))
    period = int(PERIOD.timestamp())

    out = tmp_path / "genesis.json"
    assert (
        script.main(["--period", str(period), "--denylist", BOB, "--out", str(out)])
        == 0
    )

    configured = list(aleph.config.app_config.aleph.credits.chain.denylist.value)
    assert configured  # the default config ships a denylist
    assert export.call_args.args[2] == configured + [BOB]
    printed = capsys.readouterr().out
    digest = genesis_hash(out.read_bytes())
    assert f"  credits.genesis.hash: {digest}" in printed
    assert f"  credits.genesis.period: {period}" in printed
    assert f"  credits.attestation.start_period: {period}" in printed


def test_export_script_exits_1_on_a_database_error(mocker, tmp_path, capsys):
    from sqlalchemy.exc import OperationalError

    script = _load_script()
    mocker.patch.object(script, "make_engine")
    mocker.patch.object(script, "make_session_factory")
    mocker.patch.object(
        script,
        "export_genesis",
        side_effect=OperationalError("SELECT 1", {}, Exception("connection refused")),
    )

    out = tmp_path / "genesis.json"
    code = script.main(["--period", str(int(PERIOD.timestamp())), "--out", str(out)])

    assert code == 1
    err = capsys.readouterr().err
    assert err.startswith("Database error: ")
    assert len(err.strip().splitlines()) == 1
    assert not out.exists()


@pytest.mark.asyncio
async def test_exported_genesis_round_trips_through_load_genesis(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        seed(session)
        kept, _ = export_genesis(session, PERIOD, [POOL])

    content = genesis_bytes(kept)
    digest = genesis_hash(content)
    assert genesis_bytes(kept) == content  # deterministic

    class Storage:
        async def get_json(self, content_hash, **kwargs):
            assert content_hash == digest
            return SimpleNamespace(value=json.loads(content))

    with session_factory() as session:
        assert await load_genesis(session, Storage(), digest, PERIOD) == 3
        session.commit()

    with session_factory() as session:
        assert get_fold_state(session).genesis_loaded_hash == digest
        lots = [
            (lot.address, lot.amount_remaining, lot.expiration)
            for lot in session.execute(
                select(CreditLotDb).order_by(CreditLotDb.business_key)
            ).scalars()
        ]
    assert lots == [
        (ALICE_LOWER, 750, None),
        (ALICE_LOWER, 90, PERIOD + DAY),
        (BOB, 500, None),
    ]
