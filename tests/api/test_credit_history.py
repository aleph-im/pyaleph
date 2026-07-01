import datetime as dt

import pytest
from aleph_message.models import Chain, ItemType, MessageType

from aleph.db.accessors.balances import (
    update_credit_balances_distribution,
    update_credit_balances_expense,
)
from aleph.db.models import MessageDb

CREDIT_HISTORY_URI = "/api/v0/addresses/0xtest/credit_history"


@pytest.mark.asyncio
async def test_credit_history_rejects_negative_start_date(ccn_api_client):
    response = await ccn_api_client.get(CREDIT_HISTORY_URI, params={"startDate": "-1"})
    assert response.status == 422


@pytest.mark.asyncio
async def test_credit_history_rejects_non_numeric_end_date(ccn_api_client):
    response = await ccn_api_client.get(
        CREDIT_HISTORY_URI, params={"endDate": "yesterday"}
    )
    assert response.status == 422


@pytest.mark.asyncio
async def test_credit_history_accepts_valid_time_filters(ccn_api_client):
    # No data for this address: a valid filtered query returns 404, not 422.
    response = await ccn_api_client.get(
        CREDIT_HISTORY_URI,
        params={"startDate": "1768000000", "endDate": "1769000000"},
    )
    assert response.status == 404


@pytest.mark.asyncio
async def test_credit_history_rejects_out_of_range_start_date(ccn_api_client):
    response = await ccn_api_client.get(
        CREDIT_HISTORY_URI, params={"startDate": "1e308"}
    )
    assert response.status == 422


@pytest.mark.asyncio
async def test_credit_history_validates_snake_case_params(ccn_api_client):
    # The model uses populate_by_name=True (mirroring the messages query params
    # convention), so snake_case spellings are also accepted as field names.
    # The 422 (rather than the 404 an unbound param would yield) proves the
    # snake_case alias was bound and then validated against the negative value.
    response = await ccn_api_client.get(CREDIT_HISTORY_URI, params={"start_date": "-1"})
    assert response.status == 422


@pytest.mark.asyncio
async def test_credit_history_rejects_inverted_date_range(ccn_api_client):
    # Mirrors the messages query params convention: end_date < start_date is
    # rejected up front rather than silently returning empty results.
    response = await ccn_api_client.get(
        CREDIT_HISTORY_URI,
        params={"startDate": "1769000000", "endDate": "1768000000"},
    )
    assert response.status == 422


@pytest.mark.asyncio
async def test_credit_history_time_filter_filters_entries(
    ccn_api_client, session_factory
):
    with session_factory() as session:
        for month, message_hash in [(3, "e2e_dist_march"), (5, "e2e_dist_may")]:
            update_credit_balances_distribution(
                session=session,
                credits_list=[
                    {
                        "address": "0xe2etimefilter",
                        "amount": 1000,
                        "price": "0.000001",
                        "tx_hash": f"0xtx_{message_hash}",
                        "provider": "test_provider",
                        "expiration": None,
                        "origin": "test_origin",
                        "ref": f"ref_{message_hash}",
                        "payment_method": "test_payment",
                    }
                ],
                token="ALEPH",
                chain="ETH",
                message_hash=message_hash,
                message_timestamp=dt.datetime(2026, month, 1, tzinfo=dt.timezone.utc),
            )
        session.commit()

    # Window covering only the March entry
    start = dt.datetime(2026, 2, 15, tzinfo=dt.timezone.utc).timestamp()
    end = dt.datetime(2026, 4, 15, tzinfo=dt.timezone.utc).timestamp()
    response = await ccn_api_client.get(
        "/api/v0/addresses/0xe2etimefilter/credit_history",
        params={"startDate": str(start), "endDate": str(end)},
    )
    assert response.status == 200
    data = await response.json()
    refs = [entry["credit_ref"] for entry in data["credit_history"]]
    assert refs == ["e2e_dist_march"]


@pytest.mark.asyncio
async def test_credit_history_rejects_invalid_direction(ccn_api_client):
    response = await ccn_api_client.get(
        CREDIT_HISTORY_URI, params={"direction": "sideways"}
    )
    assert response.status == 422


@pytest.mark.asyncio
async def test_credit_history_direction_filter_filters_entries(
    ccn_api_client, session_factory
):
    with session_factory() as session:
        update_credit_balances_distribution(
            session=session,
            credits_list=[
                {
                    "address": "0xe2edirection",
                    "amount": 1000,
                    "price": "0.000001",
                    "tx_hash": "0xtx_e2e_dir",
                    "provider": "test_provider",
                    "expiration": None,
                    "origin": "test_origin",
                    "ref": "ref_e2e_dir",
                    "payment_method": "test_payment",
                }
            ],
            token="ALEPH",
            chain="ETH",
            message_hash="e2e_dir_dist",
            message_timestamp=dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc),
        )
        update_credit_balances_expense(
            session=session,
            credits_list=[
                {
                    "address": "0xe2edirection",
                    "amount": 250,
                    "ref": "expense_ref_e2e_dir",
                    "execution_id": "exec_e2e_dir",
                    "node_id": "node_e2e_dir",
                    "price": "0.000001",
                }
            ],
            message_hash="e2e_dir_expense",
            message_timestamp=dt.datetime(2026, 4, 1, tzinfo=dt.timezone.utc),
        )
        session.commit()

    uri = "/api/v0/addresses/0xe2edirection/credit_history"

    response = await ccn_api_client.get(uri, params={"direction": "incoming"})
    assert response.status == 200
    data = await response.json()
    refs = [entry["credit_ref"] for entry in data["credit_history"]]
    assert refs == ["e2e_dir_dist"]
    assert all(entry["amount"] > 0 for entry in data["credit_history"])

    response = await ccn_api_client.get(uri, params={"direction": "outgoing"})
    assert response.status == 200
    data = await response.json()
    refs = [entry["credit_ref"] for entry in data["credit_history"]]
    assert refs == ["e2e_dir_expense"]
    assert all(entry["amount"] < 0 for entry in data["credit_history"])


CREDIT_HISTORY_SUMMARY_URI = "/api/v0/addresses/0xtest/credit_history/summary"


@pytest.mark.asyncio
async def test_credit_history_summary_returns_zeros_for_unknown_address(
    ccn_api_client,
):
    response = await ccn_api_client.get(CREDIT_HISTORY_SUMMARY_URI)
    assert response.status == 200
    data = await response.json()
    assert data["address"] == "0xtest"
    assert data["entry_count"] == 0
    assert data["total_amount"] == 0
    assert data["total_incoming"] == 0
    assert data["total_outgoing"] == 0


@pytest.mark.asyncio
async def test_credit_history_summary_rejects_negative_start_date(ccn_api_client):
    response = await ccn_api_client.get(
        CREDIT_HISTORY_SUMMARY_URI, params={"startDate": "-1"}
    )
    assert response.status == 422


@pytest.mark.asyncio
async def test_credit_history_summary_rejects_invalid_direction(ccn_api_client):
    response = await ccn_api_client.get(
        CREDIT_HISTORY_SUMMARY_URI, params={"direction": "sideways"}
    )
    assert response.status == 422


@pytest.mark.asyncio
async def test_credit_history_summary_aggregates_entries(
    ccn_api_client, session_factory
):
    with session_factory() as session:
        update_credit_balances_distribution(
            session=session,
            credits_list=[
                {
                    "address": "0xe2esummary",
                    "amount": 1000,
                    "price": "0.000001",
                    "tx_hash": "0xtx_e2e_summary",
                    "provider": "test_provider",
                    "expiration": None,
                    "origin": "test_origin",
                    "ref": "ref_e2e_summary",
                    "payment_method": "test_payment",
                }
            ],
            token="ALEPH",
            chain="ETH",
            message_hash="e2e_summary_dist",
            message_timestamp=dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc),
        )
        update_credit_balances_expense(
            session=session,
            credits_list=[
                {
                    "address": "0xe2esummary",
                    "amount": 250,
                    "ref": "expense_ref_e2e_summary",
                    "execution_id": "exec_e2e_summary",
                    "node_id": "node_e2e_summary",
                    "price": "0.000001",
                }
            ],
            message_hash="e2e_summary_expense",
            message_timestamp=dt.datetime(2026, 4, 1, tzinfo=dt.timezone.utc),
        )
        session.commit()

    uri = "/api/v0/addresses/0xe2esummary/credit_history/summary"

    response = await ccn_api_client.get(uri)
    assert response.status == 200
    data = await response.json()
    assert data["address"] == "0xe2esummary"
    assert data["entry_count"] == 2
    assert data["total_amount"] == 750
    assert data["total_incoming"] == 1000
    assert data["total_outgoing"] == -250

    # A filter flows through to the aggregate
    response = await ccn_api_client.get(uri, params={"direction": "outgoing"})
    assert response.status == 200
    data = await response.json()
    assert data["entry_count"] == 1
    assert data["total_amount"] == -250
    assert data["total_incoming"] == 0
    assert data["total_outgoing"] == -250


@pytest.mark.asyncio
async def test_credit_history_rejects_invalid_resource_type(ccn_api_client):
    # An unknown value anywhere in the comma-separated list is rejected.
    response = await ccn_api_client.get(
        CREDIT_HISTORY_URI, params={"resourceTypes": "STORE,BANANA"}
    )
    assert response.status == 422


@pytest.mark.asyncio
async def test_credit_history_accepts_valid_resource_types(ccn_api_client):
    # No data for this address: a valid filtered query returns 404, not 422.
    response = await ccn_api_client.get(
        CREDIT_HISTORY_URI, params={"resourceTypes": "INSTANCE,PROGRAM"}
    )
    assert response.status == 404


@pytest.mark.asyncio
async def test_credit_history_summary_accepts_valid_resource_types(ccn_api_client):
    response = await ccn_api_client.get(
        CREDIT_HISTORY_SUMMARY_URI, params={"resourceTypes": "STORE"}
    )
    assert response.status == 200
    data = await response.json()
    assert data["entry_count"] == 0


@pytest.mark.asyncio
async def test_credit_history_resource_types_filter_filters_entries(
    ccn_api_client, session_factory
):
    with session_factory() as session:
        session.add(
            MessageDb(
                item_hash="e2e_store_resource",
                chain=Chain.ETH,
                sender="0xe2eorigintype",
                signature=None,
                item_type=ItemType.storage,
                type=MessageType.store,
                item_content=None,
                content={"address": "0xe2eorigintype", "time": 1772323200.0},
                size=100,
                time=dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc),
            )
        )
        session.add(
            MessageDb(
                item_hash="e2e_instance_resource",
                chain=Chain.ETH,
                sender="0xe2eorigintype",
                signature=None,
                item_type=ItemType.storage,
                type=MessageType.instance,
                item_content=None,
                content={"address": "0xe2eorigintype", "time": 1772323200.0},
                size=100,
                time=dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc),
            )
        )
        session.flush()
        update_credit_balances_expense(
            session=session,
            credits_list=[
                {
                    "address": "0xe2eorigintype",
                    "amount": 100,
                    "ref": "e2e_store_resource",
                    "execution_id": "",
                    "node_id": "node_e2e_ot_a",
                    "price": "0.000001",
                }
            ],
            message_hash="e2e_ot_expense_store",
            message_timestamp=dt.datetime(2026, 4, 1, tzinfo=dt.timezone.utc),
        )
        update_credit_balances_expense(
            session=session,
            credits_list=[
                {
                    "address": "0xe2eorigintype",
                    "amount": 200,
                    "ref": "",
                    "execution_id": "e2e_instance_resource",
                    "node_id": "node_e2e_ot_b",
                    "price": "0.000001",
                }
            ],
            message_hash="e2e_ot_expense_instance",
            message_timestamp=dt.datetime(2026, 4, 2, tzinfo=dt.timezone.utc),
        )
        session.commit()

    listing_uri = "/api/v0/addresses/0xe2eorigintype/credit_history"
    summary_uri = "/api/v0/addresses/0xe2eorigintype/credit_history/summary"

    response = await ccn_api_client.get(listing_uri, params={"resourceTypes": "STORE"})
    assert response.status == 200
    data = await response.json()
    refs = [entry["credit_ref"] for entry in data["credit_history"]]
    assert refs == ["e2e_ot_expense_store"]

    response = await ccn_api_client.get(
        listing_uri, params={"resourceTypes": "INSTANCE"}
    )
    assert response.status == 200
    data = await response.json()
    refs = [entry["credit_ref"] for entry in data["credit_history"]]
    assert refs == ["e2e_ot_expense_instance"]

    # A comma-separated list matches entries of any of the given types.
    response = await ccn_api_client.get(
        listing_uri, params={"resourceTypes": "STORE,INSTANCE"}
    )
    assert response.status == 200
    data = await response.json()
    refs = {entry["credit_ref"] for entry in data["credit_history"]}
    assert refs == {"e2e_ot_expense_store", "e2e_ot_expense_instance"}

    response = await ccn_api_client.get(
        summary_uri, params={"resourceTypes": "INSTANCE"}
    )
    assert response.status == 200
    data = await response.json()
    assert data["entry_count"] == 1
    assert data["total_amount"] == -200


@pytest.mark.asyncio
async def test_credit_history_accepts_valid_resource(ccn_api_client):
    # No data for this address: a valid filtered query returns 404, not 422.
    response = await ccn_api_client.get(
        CREDIT_HISTORY_URI, params={"resource": "some_resource_hash"}
    )
    assert response.status == 404


@pytest.mark.asyncio
async def test_credit_history_summary_accepts_valid_resource(ccn_api_client):
    response = await ccn_api_client.get(
        CREDIT_HISTORY_SUMMARY_URI, params={"resource": "some_resource_hash"}
    )
    assert response.status == 200
    data = await response.json()
    assert data["entry_count"] == 0


@pytest.mark.asyncio
async def test_credit_history_resource_filter_filters_entries(
    ccn_api_client, session_factory
):
    with session_factory() as session:
        # Expense referencing the target VM via origin (execution_id).
        update_credit_balances_expense(
            session=session,
            credits_list=[
                {
                    "address": "0xe2evmhash",
                    "amount": 100,
                    "ref": "",
                    "execution_id": "target_vm",
                    "node_id": "node_e2e_vm_a",
                    "price": "0.000001",
                }
            ],
            message_hash="e2e_vm_expense_origin",
            message_timestamp=dt.datetime(2026, 4, 1, tzinfo=dt.timezone.utc),
        )
        # Expense referencing the target VM via origin_ref (ref), origin empty.
        update_credit_balances_expense(
            session=session,
            credits_list=[
                {
                    "address": "0xe2evmhash",
                    "amount": 200,
                    "ref": "target_vm",
                    "execution_id": "",
                    "node_id": "node_e2e_vm_b",
                    "price": "0.000001",
                }
            ],
            message_hash="e2e_vm_expense_originref",
            message_timestamp=dt.datetime(2026, 4, 2, tzinfo=dt.timezone.utc),
        )
        # Unrelated expense for a different VM.
        update_credit_balances_expense(
            session=session,
            credits_list=[
                {
                    "address": "0xe2evmhash",
                    "amount": 300,
                    "ref": "",
                    "execution_id": "other_vm",
                    "node_id": "node_e2e_vm_c",
                    "price": "0.000001",
                }
            ],
            message_hash="e2e_vm_expense_other",
            message_timestamp=dt.datetime(2026, 4, 3, tzinfo=dt.timezone.utc),
        )
        session.commit()

    listing_uri = "/api/v0/addresses/0xe2evmhash/credit_history"
    summary_uri = "/api/v0/addresses/0xe2evmhash/credit_history/summary"

    # Listing: matches the two entries for target_vm regardless of column.
    response = await ccn_api_client.get(listing_uri, params={"resource": "target_vm"})
    assert response.status == 200
    data = await response.json()
    refs = {entry["credit_ref"] for entry in data["credit_history"]}
    assert refs == {"e2e_vm_expense_origin", "e2e_vm_expense_originref"}

    # Summary: aggregates only the matching entries (both outgoing).
    response = await ccn_api_client.get(summary_uri, params={"resource": "target_vm"})
    assert response.status == 200
    data = await response.json()
    assert data["entry_count"] == 2
    assert data["total_amount"] == -300
    assert data["total_incoming"] == 0
    assert data["total_outgoing"] == -300

    # Composes with direction: incoming + target_vm matches nothing -> 404.
    response = await ccn_api_client.get(
        listing_uri, params={"resource": "target_vm", "direction": "incoming"}
    )
    assert response.status == 404
