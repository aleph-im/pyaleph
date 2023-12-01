from typing import Optional

from sqlalchemy import select, text
from sqlalchemy.orm.session import Session

from aleph.types.db_session import DbSession


def _parse_ccn_result(result):
    result_dict = {
        "item_hash": [],
        "measured_at": [],
        "base_latency": [],
        "base_latency_ipv4": [],
        "metrics_latency": [],
        "aggregate_latency": [],
        "file_download_latency": [],
        "pending_messages": [],
        "eth_height_remaining": [],
    }

    for row in result:
        item_hash = row[0]
        timestamp = row[1]
        base_latency = row[2]
        base_latency_ipv4 = row[3]
        metrics_latency = row[4]
        aggregate_latency = row[5]
        file_download_latency = row[6]
        pending_messages = row[7]
        eth_height_remaining = row[8]

        result_dict["item_hash"].append(item_hash)
        result_dict["measured_at"].append(timestamp)
        result_dict["base_latency"].append(base_latency)
        result_dict["base_latency_ipv4"].append(base_latency_ipv4)
        result_dict["metrics_latency"].append(metrics_latency)
        result_dict["aggregate_latency"].append(aggregate_latency)
        result_dict["file_download_latency"].append(file_download_latency)
        result_dict["pending_messages"].append(pending_messages)
        result_dict["eth_height_remaining"].append(eth_height_remaining)
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
    start_date: Optional[float] = None,
    end_date: Optional[float] = None,
    sort_order: Optional[str] = None,
):
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
        start_date=start_date,
        end_date=end_date,
        sort_order=sort_order,
    )

    result = session.execute(select_stmt).fetchall()

    return _parse_ccn_result(result=result)


def _parse_crn_result(result):
    result_dict = {
        "item_hash": [],
        "measured_at": [],
        "base_latency": [],
        "base_latency_ipv4": [],
        "full_check_latency": [],
        "diagnostic_vm_latency": [],
    }
    for row in result:
        item_hash = row[0]
        timestamp = row[1]
        base_latency = row[2]
        base_latency_ipv4 = row[3]
        full_check_latency = row[4]
        diagnostic_vm_latency = row[5]

        result_dict["item_hash"].append(item_hash)
        result_dict["measured_at"].append(timestamp)
        result_dict["base_latency"].append(base_latency)
        result_dict["base_latency_ipv4"].append(base_latency_ipv4)
        result_dict["full_check_latency"].append(full_check_latency)
        result_dict["diagnostic_vm_latency"].append(diagnostic_vm_latency)

    return result_dict


def query_metric_crn(
    session: DbSession,
    node_id: str,
    start_date: Optional[float] = None,
    end_date: Optional[float] = None,
    sort_order: Optional[str] = None,
):
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
        start_date=start_date,
        end_date=end_date,
        sort_order=sort_order,
    )

    result = session.execute(select_stmt).fetchall()

    return _parse_crn_result(result=result)
