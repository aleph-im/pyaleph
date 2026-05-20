import datetime as dt

from aleph_message.models import Chain, ItemType, MessageType
from sqlalchemy import select

from aleph.db.accessors.metrics import (
    _build_ccn_rows,
    _build_crn_rows,
    insert_node_metrics,
    query_metric_ccn,
    query_metric_crn,
)
from aleph.db.models import CcnMetricDb, CrnMetricDb, MessageDb
from aleph.types.channel import Channel
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.sort_order import SortOrder

_TS_1 = dt.datetime.fromtimestamp(1700000000.0, tz=dt.timezone.utc)
_TS_2 = dt.datetime.fromtimestamp(1700000001.0, tz=dt.timezone.utc)


def test_build_crn_rows_full_payload():
    item_hash = "msg-1"
    crn_array = [
        {
            "measured_at": 1700000000.0,
            "node_id": "node-A",
            "base_latency": 0.1,
            "base_latency_ipv4": 0.11,
            "full_check_latency": 0.2,
            "diagnostic_vm_latency": 0.3,
        },
        {
            "measured_at": 1700000001.0,
            "node_id": "node-B",
            "base_latency": 0.4,
            "base_latency_ipv4": 0.41,
            "full_check_latency": 0.5,
            "diagnostic_vm_latency": 0.6,
        },
    ]
    rows = _build_crn_rows(item_hash, crn_array)
    assert len(rows) == 2
    assert rows[0] == {
        "item_hash": "msg-1",
        "node_id": "node-A",
        "measured_at": _TS_1,
        "base_latency": 0.1,
        "base_latency_ipv4": 0.11,
        "full_check_latency": 0.2,
        "diagnostic_vm_latency": 0.3,
    }


def test_build_crn_rows_missing_optional_fields():
    rows = _build_crn_rows(
        "msg-2",
        [
            {"measured_at": 1700000000.0, "node_id": "node-A"},
        ],
    )
    assert rows == [
        {
            "item_hash": "msg-2",
            "node_id": "node-A",
            "measured_at": _TS_1,
            "base_latency": None,
            "base_latency_ipv4": None,
            "full_check_latency": None,
            "diagnostic_vm_latency": None,
        }
    ]


def test_build_crn_rows_skips_entries_missing_required_fields():
    # Without node_id or measured_at, the existing view emits NULL and
    # the migration's WHERE clause filters those out. The builder skips them.
    rows = _build_crn_rows(
        "msg-3",
        [
            {"measured_at": 1700000000.0},  # missing node_id
            {"node_id": "node-A"},  # missing measured_at
            {"measured_at": 1700000001.0, "node_id": "ok"},  # valid
        ],
    )
    assert len(rows) == 1
    assert rows[0]["node_id"] == "ok"


def test_build_crn_rows_non_numeric_field_becomes_none():
    rows = _build_crn_rows(
        "msg-4",
        [
            {
                "measured_at": 1700000000.0,
                "node_id": "node-A",
                "base_latency": "not-a-number",
            },
        ],
    )
    assert rows[0]["base_latency"] is None


def test_build_crn_rows_empty_array():
    assert _build_crn_rows("msg-5", []) == []


def test_build_ccn_rows_full_payload():
    rows = _build_ccn_rows(
        "msg-1",
        [
            {
                "measured_at": 1700000000.0,
                "node_id": "node-A",
                "base_latency": 0.1,
                "base_latency_ipv4": 0.11,
                "metrics_latency": 0.2,
                "aggregate_latency": 0.3,
                "file_download_latency": 0.4,
                "pending_messages": 42,
                "eth_height_remaining": 7,
            },
        ],
    )
    assert rows == [
        {
            "item_hash": "msg-1",
            "node_id": "node-A",
            "measured_at": _TS_1,
            "base_latency": 0.1,
            "base_latency_ipv4": 0.11,
            "metrics_latency": 0.2,
            "aggregate_latency": 0.3,
            "file_download_latency": 0.4,
            "pending_messages": 42,
            "eth_height_remaining": 7,
        }
    ]


def test_build_ccn_rows_non_numeric_pending_messages_becomes_none():
    rows = _build_ccn_rows(
        "msg-2",
        [
            {
                "measured_at": 1700000000.0,
                "node_id": "node-A",
                "pending_messages": "lots",
            },
        ],
    )
    assert rows[0]["pending_messages"] is None


def _seed_scoring_message(session: DbSession, item_hash: str) -> None:
    """Insert a minimal MessageDb so the FK from metric rows resolves."""
    msg = MessageDb(
        item_hash=item_hash,
        type=MessageType.post,
        chain=Chain.ETH,
        sender="0x4D52380D3191274a04846c89c069E6C3F2Ed94e4",
        channel=Channel("aleph-scoring"),
        signature=None,
        item_type=ItemType.inline,
        item_content="{}",
        content={
            "type": "aleph-network-metrics",
            "address": "0x4D52380D3191274a04846c89c069E6C3F2Ed94e4",
            "content": {},
            "time": 1700000000.0,
        },
        time=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
        size=2,
    )
    session.add(msg)
    session.flush()


def test_insert_node_metrics_writes_crn_and_ccn(session_factory: DbSessionFactory):
    item_hash = "abc-test-1"
    content = {
        "metrics": {
            "crn": [
                {
                    "measured_at": 1700000000.0,
                    "node_id": "crn-A",
                    "base_latency": 0.1,
                },
            ],
            "ccn": [
                {
                    "measured_at": 1700000001.0,
                    "node_id": "ccn-A",
                    "pending_messages": 5,
                },
            ],
        }
    }
    with session_factory() as session:
        _seed_scoring_message(session, item_hash)
        insert_node_metrics(session=session, item_hash=item_hash, content=content)
        session.commit()

        crn_rows = list(
            session.execute(
                select(CrnMetricDb).where(CrnMetricDb.item_hash == item_hash)
            ).scalars()
        )
        ccn_rows = list(
            session.execute(
                select(CcnMetricDb).where(CcnMetricDb.item_hash == item_hash)
            ).scalars()
        )

    assert len(crn_rows) == 1
    assert crn_rows[0].node_id == "crn-A"
    assert crn_rows[0].base_latency == 0.1
    assert len(ccn_rows) == 1
    assert ccn_rows[0].node_id == "ccn-A"
    assert ccn_rows[0].pending_messages == 5


def test_insert_node_metrics_missing_metrics_key_is_noop(
    session_factory: DbSessionFactory,
):
    item_hash = "abc-test-2"
    with session_factory() as session:
        _seed_scoring_message(session, item_hash)
        insert_node_metrics(session=session, item_hash=item_hash, content={})
        session.commit()

        assert (
            list(
                session.execute(
                    select(CrnMetricDb).where(CrnMetricDb.item_hash == item_hash)
                ).scalars()
            )
            == []
        )
        assert (
            list(
                session.execute(
                    select(CcnMetricDb).where(CcnMetricDb.item_hash == item_hash)
                ).scalars()
            )
            == []
        )


def test_query_metric_crn_filters_by_node_and_date(session_factory: DbSessionFactory):
    item_hash = "abc-test-q1"
    with session_factory() as session:
        _seed_scoring_message(session, item_hash)
        session.add_all(
            [
                CrnMetricDb(
                    item_hash=item_hash,
                    node_id="node-A",
                    measured_at=dt.datetime.fromtimestamp(100.0, tz=dt.timezone.utc),
                    base_latency=0.1,
                ),
                CrnMetricDb(
                    item_hash=item_hash,
                    node_id="node-A",
                    measured_at=dt.datetime.fromtimestamp(200.0, tz=dt.timezone.utc),
                    base_latency=0.2,
                ),
                CrnMetricDb(
                    item_hash=item_hash,
                    node_id="node-B",
                    measured_at=dt.datetime.fromtimestamp(150.0, tz=dt.timezone.utc),
                    base_latency=0.9,
                ),
            ]
        )
        session.commit()

        result = query_metric_crn(
            session=session,
            node_id="node-A",
            start_date=150.0,
            end_date=None,
            sort_order=SortOrder.ASCENDING,
        )

    # _parse_crn_result returns dict-of-lists keyed by column name. node_id is not exposed.
    assert "node_id" not in result
    assert result["measured_at"] == [200.0]
    assert result["base_latency"] == [0.2]


def test_query_metric_ccn_returns_all_columns(session_factory: DbSessionFactory):
    item_hash = "abc-test-q2"
    with session_factory() as session:
        _seed_scoring_message(session, item_hash)
        session.add(
            CcnMetricDb(
                item_hash=item_hash,
                node_id="ccn-A",
                measured_at=dt.datetime.fromtimestamp(500.0, tz=dt.timezone.utc),
                base_latency=0.1,
                base_latency_ipv4=0.11,
                metrics_latency=0.2,
                aggregate_latency=0.3,
                file_download_latency=0.4,
                pending_messages=5,
                eth_height_remaining=7,
            )
        )
        session.commit()

        result = query_metric_ccn(
            session=session,
            node_id="ccn-A",
            start_date=400.0,
            end_date=None,
            sort_order=SortOrder.ASCENDING,
        )

    assert result["measured_at"] == [500.0]
    assert result["base_latency"] == [0.1]
    assert result["pending_messages"] == [5]
    assert result["eth_height_remaining"] == [7]
