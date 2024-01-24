import time
from typing import Optional

from sqlalchemy import select, text
from sqlalchemy.orm.session import Session

from aleph.types.db_session import DbSession


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

    return result_dict


def _build_metric_filter(select_stmt, node_id, start_date, end_date, sort_order):
    if node_id:
        select_stmt = select_stmt.where(text("node_id = :node_id")).params(
            node_id=node_id
        )
    if start_date:
        select_stmt = select_stmt.where(text("measured_at >= :start_date")).params(
            start_date=start_date
        )
    if end_date:
        select_stmt = select_stmt.where(text("measured_at <= :end_date")).params(
            end_date=end_date
        )
    if sort_order:
        select_stmt = select_stmt.order_by(text(f"measured_at {sort_order}"))
    return select_stmt


def query_metric_ccn(
    session: Session,
    node_id: Optional[str] = None,
    start_timestamp: Optional[float] = None,
    end_timestamp: Optional[float] = None,
    sort_order: Optional[str] = None,
):
    # Default to the last 2 weeks from now, or 2 weeks before the `end_timestamp`.
    if not start_timestamp and not end_timestamp:
        start_timestamp = time.time() - 60 * 60 * 24 * 14
    elif end_timestamp and not start_timestamp:
        start_timestamp = end_timestamp - 60 * 60 * 24 * 14

    select_stmt = select(
        [
            text("item_hash"),
            text("measured_at"),
            text("base_latency"),
            text("base_latency_ipv4"),
            text("metrics_latency"),
            text("aggregate_latency"),
            text("file_download_latency"),
            text("pending_messages"),
            text("eth_height_remaining"),
        ]
    ).select_from(text("ccn_metric_view"))

    select_stmt = _build_metric_filter(
        select_stmt=select_stmt,
        node_id=node_id,
        start_date=start_timestamp,
        end_date=end_timestamp,
        sort_order=sort_order,
    )

    result = session.execute(select_stmt).fetchall()

    return _parse_ccn_result(result=result)


def query_metric_crn(
    session: DbSession,
    node_id: str,
    start_timestamp: Optional[float] = None,
    end_timestamp: Optional[float] = None,
    sort_order: Optional[str] = None,
):
    # Default to the last 2 weeks from now, or 2 weeks before the `end_timestamp`.
    if not start_timestamp and not end_timestamp:
        start_timestamp = time.time() - 60 * 60 * 24 * 14
    elif end_timestamp and not start_timestamp:
        start_timestamp = end_timestamp - 60 * 60 * 24 * 14

    select_stmt = select(
        [
            text("item_hash"),
            text("measured_at"),
            text("base_latency"),
            text("base_latency_ipv4"),
            text("full_check_latency"),
            text("diagnostic_vm_latency"),
        ]
    ).select_from(text("crn_metric_view"))

    select_stmt = _build_metric_filter(
        select_stmt=select_stmt,
        node_id=node_id,
        start_date=start_timestamp,
        end_date=end_timestamp,
        sort_order=sort_order,
    )

    result = session.execute(select_stmt).fetchall()

    return _parse_crn_result(result=result)
