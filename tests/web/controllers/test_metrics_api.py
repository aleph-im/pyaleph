"""
API contract tests for the persisted-metrics endpoints.

/api/v0/compute/{node_id}/metrics  -- CRN
/api/v0/core/{node_id}/metrics     -- CCN

These tests verify that the endpoints serve rows from the CrnMetricDb /
CcnMetricDb tables and preserve the existing response shape.

Note: both accessors default to a 14-day lookback window when no start_date
is given.  To avoid that filter we pass ?start_date=0 on every request so all
rows in the table are visible regardless of their measured_at value.
"""

import datetime as dt

import pytest
from aleph_message.models import Chain, ItemType, MessageType

from aleph.db.models import CcnMetricDb, CrnMetricDb, MessageDb
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory

SCORING_SENDER = "0x4D52380D3191274a04846c89c069E6C3F2Ed94e4"
_MEASURED_AT = 1700000000.0  # Nov 2023 -- well outside the default 14-day window
_MEASURED_AT_DT = dt.datetime.fromtimestamp(_MEASURED_AT, tz=dt.timezone.utc)

# The accessors use `if not start_date` to decide whether to apply the 14-day
# lookback window.  Passing start_date=0 would be falsy and still trigger the
# lookback.  Use 1 (1970-01-01 00:00:01 UTC) instead: truthy, before any real
# metric, so every row is included.
_START_DATE_BYPASS = "start_date=1"


def _seed_message(session, item_hash: str) -> None:
    """Insert a minimal processed MessageDb that metric rows can FK into."""
    session.add(
        MessageDb(
            item_hash=item_hash,
            type=MessageType.post,
            chain=Chain.ETH,
            sender=SCORING_SENDER,
            channel=Channel("aleph-scoring"),
            signature=None,
            item_type=ItemType.inline,
            item_content="{}",
            content={
                "type": "aleph-network-metrics",
                "address": SCORING_SENDER,
                "content": {},
                "time": 0.0,
            },
            time=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            size=2,
        )
    )
    session.flush()


@pytest.mark.asyncio
async def test_get_crn_metrics_returns_persisted_rows(
    ccn_api_client, session_factory: DbSessionFactory
):
    node_id = "crn-node-X"
    with session_factory() as session:
        _seed_message(session, "h-crn")
        session.add(
            CrnMetricDb(
                item_hash="h-crn",
                node_id=node_id,
                measured_at=_MEASURED_AT_DT,
                base_latency=0.1,
                base_latency_ipv4=0.11,
                full_check_latency=0.2,
                diagnostic_vm_latency=0.3,
            )
        )
        session.commit()

    response = await ccn_api_client.get(
        f"/api/v0/compute/{node_id}/metrics?{_START_DATE_BYPASS}"
    )
    assert response.status == 200, await response.text()
    body = await response.json()
    assert "metrics" in body
    metrics = body["metrics"]
    assert metrics["measured_at"] == [_MEASURED_AT]
    assert metrics["base_latency"] == [0.1]
    assert metrics["full_check_latency"] == [0.2]


@pytest.mark.asyncio
async def test_get_ccn_metrics_returns_persisted_rows(
    ccn_api_client, session_factory: DbSessionFactory
):
    node_id = "ccn-node-Y"
    with session_factory() as session:
        _seed_message(session, "h-ccn")
        session.add(
            CcnMetricDb(
                item_hash="h-ccn",
                node_id=node_id,
                measured_at=_MEASURED_AT_DT,
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

    response = await ccn_api_client.get(
        f"/api/v0/core/{node_id}/metrics?{_START_DATE_BYPASS}"
    )
    assert response.status == 200, await response.text()
    body = await response.json()
    assert "metrics" in body
    metrics = body["metrics"]
    assert metrics["measured_at"] == [_MEASURED_AT]
    assert metrics["pending_messages"] == [5]
    assert metrics["eth_height_remaining"] == [7]


@pytest.mark.asyncio
async def test_get_crn_metrics_404_when_node_unknown(ccn_api_client):
    response = await ccn_api_client.get(
        f"/api/v0/compute/no-such-node/metrics?{_START_DATE_BYPASS}"
    )
    assert response.status == 404
