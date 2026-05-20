import datetime as dt
import time
from typing import Any, List, Mapping, Optional

from sqlalchemy import insert, select
from sqlalchemy.orm.session import Session

from aleph.db.models import CcnMetricDb, CrnMetricDb
from aleph.types.db_session import DbSession
from aleph.types.sort_order import SortOrder, SortOrderForMetrics


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_measured_at(value: Any) -> Optional[dt.datetime]:
    """Scoring payloads carry measured_at as a Unix epoch number. The DB
    column is TIMESTAMPTZ so the partition key can be a real time. Return
    None on anything that isn't a usable timestamp."""
    epoch = _coerce_float(value)
    if epoch is None:
        return None
    try:
        return dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _epoch_to_datetime(value: Optional[float]) -> Optional[dt.datetime]:
    if value is None:
        return None
    return dt.datetime.fromtimestamp(value, tz=dt.timezone.utc)


def _datetime_to_epoch(value: Optional[dt.datetime]) -> Optional[float]:
    if value is None:
        return None
    return value.timestamp()


def _build_crn_rows(
    item_hash: str, crn_array: List[Mapping[str, Any]]
) -> List[Mapping[str, Any]]:
    rows: List[Mapping[str, Any]] = []
    for entry in crn_array:
        if not isinstance(entry, Mapping):
            continue
        node_id = entry.get("node_id")
        measured_at = _coerce_measured_at(entry.get("measured_at"))
        if node_id is None or measured_at is None:
            continue
        rows.append(
            {
                "item_hash": item_hash,
                "node_id": str(node_id),
                "measured_at": measured_at,
                "base_latency": _coerce_float(entry.get("base_latency")),
                "base_latency_ipv4": _coerce_float(entry.get("base_latency_ipv4")),
                "full_check_latency": _coerce_float(entry.get("full_check_latency")),
                "diagnostic_vm_latency": _coerce_float(
                    entry.get("diagnostic_vm_latency")
                ),
            }
        )
    return rows


def _build_ccn_rows(
    item_hash: str, ccn_array: List[Mapping[str, Any]]
) -> List[Mapping[str, Any]]:
    rows: List[Mapping[str, Any]] = []
    for entry in ccn_array:
        if not isinstance(entry, Mapping):
            continue
        node_id = entry.get("node_id")
        measured_at = _coerce_measured_at(entry.get("measured_at"))
        if node_id is None or measured_at is None:
            continue
        rows.append(
            {
                "item_hash": item_hash,
                "node_id": str(node_id),
                "measured_at": measured_at,
                "base_latency": _coerce_float(entry.get("base_latency")),
                "base_latency_ipv4": _coerce_float(entry.get("base_latency_ipv4")),
                "metrics_latency": _coerce_float(entry.get("metrics_latency")),
                "aggregate_latency": _coerce_float(entry.get("aggregate_latency")),
                "file_download_latency": _coerce_float(
                    entry.get("file_download_latency")
                ),
                "pending_messages": _coerce_int(entry.get("pending_messages")),
                "eth_height_remaining": _coerce_int(entry.get("eth_height_remaining")),
            }
        )
    return rows


def _parse_ccn_result(result):
    keys = [
        "item_hash",
        "measured_at",
        "base_latency",
        "base_latency_ipv4",
        "metrics_latency",
        "aggregate_latency",
        "file_download_latency",
        "pending_messages",
        "eth_height_remaining",
    ]

    # Transpose the result and create a dictionary
    result_dict = {key: list(values) for key, values in zip(keys, zip(*result))}

    # API contract serializes measured_at as epoch seconds.
    if "measured_at" in result_dict:
        result_dict["measured_at"] = [
            _datetime_to_epoch(v) for v in result_dict["measured_at"]
        ]

    return result_dict


def _parse_crn_result(result):
    keys = [
        "item_hash",
        "measured_at",
        "base_latency",
        "base_latency_ipv4",
        "full_check_latency",
        "diagnostic_vm_latency",
    ]

    # Transpose the result and create a dictionary
    result_dict = {key: list(values) for key, values in zip(keys, zip(*result))}

    if "measured_at" in result_dict:
        result_dict["measured_at"] = [
            _datetime_to_epoch(v) for v in result_dict["measured_at"]
        ]

    return result_dict


def query_metric_ccn(
    session: Session,
    node_id: Optional[str] = None,
    start_date: Optional[float] = None,
    end_date: Optional[float] = None,
    sort_order: Optional[SortOrderForMetrics] = None,
):
    # Default to the last 2 weeks from now, or 2 weeks before the `end_date`.
    if not start_date and not end_date:
        start_date = time.time() - 60 * 60 * 24 * 14
    elif end_date and not start_date:
        start_date = end_date - 60 * 60 * 24 * 14

    start_dt = _epoch_to_datetime(start_date)
    end_dt = _epoch_to_datetime(end_date)

    select_stmt = select(
        CcnMetricDb.item_hash,
        CcnMetricDb.measured_at,
        CcnMetricDb.base_latency,
        CcnMetricDb.base_latency_ipv4,
        CcnMetricDb.metrics_latency,
        CcnMetricDb.aggregate_latency,
        CcnMetricDb.file_download_latency,
        CcnMetricDb.pending_messages,
        CcnMetricDb.eth_height_remaining,
    )

    if node_id:
        select_stmt = select_stmt.where(CcnMetricDb.node_id == node_id)
    if start_dt:
        select_stmt = select_stmt.where(CcnMetricDb.measured_at >= start_dt)
    if end_dt:
        select_stmt = select_stmt.where(CcnMetricDb.measured_at <= end_dt)
    order_col = CcnMetricDb.measured_at
    if sort_order == SortOrder.DESCENDING:
        select_stmt = select_stmt.order_by(order_col.desc())
    else:
        select_stmt = select_stmt.order_by(order_col.asc())

    result = session.execute(select_stmt).fetchall()
    return _parse_ccn_result(result=result)


def query_metric_crn(
    session: DbSession,
    node_id: str,
    start_date: Optional[float] = None,
    end_date: Optional[float] = None,
    sort_order: Optional[SortOrderForMetrics] = None,
):
    # Default to the last 2 weeks from now, or 2 weeks before the `end_date`.
    if not start_date and not end_date:
        start_date = time.time() - 60 * 60 * 24 * 14
    elif end_date and not start_date:
        start_date = end_date - 60 * 60 * 24 * 14

    start_dt = _epoch_to_datetime(start_date)
    end_dt = _epoch_to_datetime(end_date)

    select_stmt = select(
        CrnMetricDb.item_hash,
        CrnMetricDb.measured_at,
        CrnMetricDb.base_latency,
        CrnMetricDb.base_latency_ipv4,
        CrnMetricDb.full_check_latency,
        CrnMetricDb.diagnostic_vm_latency,
    )

    if node_id:
        select_stmt = select_stmt.where(CrnMetricDb.node_id == node_id)
    if start_dt:
        select_stmt = select_stmt.where(CrnMetricDb.measured_at >= start_dt)
    if end_dt:
        select_stmt = select_stmt.where(CrnMetricDb.measured_at <= end_dt)
    order_col = CrnMetricDb.measured_at
    if sort_order == SortOrder.DESCENDING:
        select_stmt = select_stmt.order_by(order_col.desc())
    else:
        select_stmt = select_stmt.order_by(order_col.asc())

    result = session.execute(select_stmt).fetchall()
    return _parse_crn_result(result=result)


def insert_node_metrics(
    session: DbSession,
    item_hash: str,
    content: Mapping[str, Any],
) -> None:
    metrics = content.get("metrics") or {}
    if not isinstance(metrics, Mapping):
        return

    crn_array = metrics.get("crn") or []
    ccn_array = metrics.get("ccn") or []
    if not isinstance(crn_array, list):
        crn_array = []
    if not isinstance(ccn_array, list):
        ccn_array = []

    crn_rows = _build_crn_rows(item_hash, crn_array)
    ccn_rows = _build_ccn_rows(item_hash, ccn_array)

    if crn_rows:
        session.execute(insert(CrnMetricDb), crn_rows)
    if ccn_rows:
        session.execute(insert(CcnMetricDb), ccn_rows)
