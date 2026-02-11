import pytest

from aleph.db.models import AlephBalanceDb
from aleph.jobs.process_pending_messages import PendingMessageProcessor

COSTS_URI = "/api/v0/costs"


@pytest.mark.asyncio
async def test_get_costs_empty_db(ccn_api_client):
    """Test getting costs when no resources exist."""
    response = await ccn_api_client.get(COSTS_URI)
    assert response.status == 200, await response.text()
    data = await response.json()

    assert "summary" in data
    assert data["summary"]["total_consumed_credits"] == 0
    assert data["summary"]["resource_count"] == 0
    # Costs are formatted with high precision
    assert float(data["summary"]["total_cost_hold"]) == 0.0
    assert float(data["summary"]["total_cost_stream"]) == 0.0
    assert float(data["summary"]["total_cost_credit"]) == 0.0

    assert "filters" in data
    assert data["filters"]["address"] is None
    assert data["filters"]["item_hash"] is None
    assert data["filters"]["payment_type"] is None

    # No resources list without include_details
    assert data.get("resources") is None


@pytest.mark.asyncio
async def test_get_costs_with_resources(
    ccn_api_client,
    message_processor: PendingMessageProcessor,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    instance_message_with_volumes_in_db,
    fixture_instance_message,
    user_balance: AlephBalanceDb,
):
    """Test getting costs with existing resources."""
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    response = await ccn_api_client.get(COSTS_URI)
    assert response.status == 200, await response.text()
    data = await response.json()

    assert "summary" in data
    # Should have at least one resource
    assert data["summary"]["resource_count"] >= 1
    # At least one cost type should be non-zero
    assert (
        float(data["summary"]["total_cost_hold"]) > 0
        or float(data["summary"]["total_cost_stream"]) > 0
        or float(data["summary"]["total_cost_credit"]) > 0
    )


@pytest.mark.asyncio
async def test_get_costs_with_address_filter(
    ccn_api_client,
    message_processor: PendingMessageProcessor,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    instance_message_with_volumes_in_db,
    fixture_instance_message,
    user_balance: AlephBalanceDb,
):
    """Test getting costs filtered by address."""
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    address = user_balance.address
    response = await ccn_api_client.get(f"{COSTS_URI}?address={address}")
    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["filters"]["address"] == address


@pytest.mark.asyncio
async def test_get_costs_with_payment_type_filter(
    ccn_api_client,
    message_processor: PendingMessageProcessor,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    instance_message_with_volumes_in_db,
    fixture_instance_message,
):
    """Test getting costs filtered by payment type."""
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    response = await ccn_api_client.get(f"{COSTS_URI}?payment_type=hold")
    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["filters"]["payment_type"] == "hold"


@pytest.mark.asyncio
async def test_get_costs_with_details_level_1(
    ccn_api_client,
    message_processor: PendingMessageProcessor,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    instance_message_with_volumes_in_db,
    fixture_instance_message,
):
    """Test getting costs with include_details=1 (resource list)."""
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    response = await ccn_api_client.get(f"{COSTS_URI}?include_details=1")
    assert response.status == 200, await response.text()
    data = await response.json()

    # Should have pagination info
    assert "pagination_page" in data
    assert "pagination_total" in data
    assert "pagination_per_page" in data
    assert "pagination_item" in data
    assert data["pagination_item"] == "resources"

    # Should have resources list
    assert "resources" in data
    assert isinstance(data["resources"], list)

    if len(data["resources"]) > 0:
        resource = data["resources"][0]
        assert "item_hash" in resource
        assert "owner" in resource
        assert "payment_type" in resource
        assert "consumed_credits" in resource
        assert "cost_hold" in resource
        assert "cost_stream" in resource
        assert "cost_credit" in resource
        # Level 1 should not have detail breakdown
        assert resource.get("detail") is None


@pytest.mark.asyncio
async def test_get_costs_with_details_level_2(
    ccn_api_client,
    message_processor: PendingMessageProcessor,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    instance_message_with_volumes_in_db,
    fixture_instance_message,
):
    """Test getting costs with include_details=2 (resource list with breakdown)."""
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    response = await ccn_api_client.get(f"{COSTS_URI}?include_details=2")
    assert response.status == 200, await response.text()
    data = await response.json()

    assert "resources" in data
    assert isinstance(data["resources"], list)

    if len(data["resources"]) > 0:
        resource = data["resources"][0]
        # Level 2 should have detail breakdown
        assert "detail" in resource
        assert isinstance(resource["detail"], list)

        if len(resource["detail"]) > 0:
            detail = resource["detail"][0]
            assert "type" in detail
            assert "name" in detail
            assert "cost_hold" in detail
            assert "cost_stream" in detail
            assert "cost_credit" in detail


@pytest.mark.asyncio
async def test_get_costs_pagination(
    ccn_api_client,
    message_processor: PendingMessageProcessor,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    instance_message_with_volumes_in_db,
    fixture_instance_message,
):
    """Test pagination of costs endpoint."""
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    # Minimum pagination is 10
    response = await ccn_api_client.get(
        f"{COSTS_URI}?include_details=1&pagination=10&page=1"
    )
    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["pagination_page"] == 1
    assert data["pagination_per_page"] == 10
    # Resources list should respect pagination
    assert len(data["resources"]) <= 10


@pytest.mark.asyncio
async def test_get_costs_pagination_below_minimum(ccn_api_client):
    """Test that pagination below minimum (10) returns an error."""
    response = await ccn_api_client.get(f"{COSTS_URI}?include_details=1&pagination=5")
    assert response.status == 422  # Unprocessable Entity


@pytest.mark.asyncio
async def test_get_costs_pagination_above_maximum(ccn_api_client):
    """Test that pagination above maximum (1000) returns an error."""
    response = await ccn_api_client.get(
        f"{COSTS_URI}?include_details=1&pagination=2000"
    )
    assert response.status == 422  # Unprocessable Entity


@pytest.mark.asyncio
async def test_get_costs_invalid_payment_type(ccn_api_client):
    """Test that invalid payment_type returns an error."""
    response = await ccn_api_client.get(f"{COSTS_URI}?payment_type=invalid")
    assert response.status == 422  # Unprocessable Entity


@pytest.mark.asyncio
async def test_get_costs_invalid_include_details(ccn_api_client):
    """Test that invalid include_details value returns an error."""
    response = await ccn_api_client.get(f"{COSTS_URI}?include_details=5")
    assert response.status == 422  # Unprocessable Entity


@pytest.mark.asyncio
async def test_get_costs_with_item_hash_filter(
    ccn_api_client,
    message_processor: PendingMessageProcessor,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    instance_message_with_volumes_in_db,
    fixture_instance_message,
    user_balance: AlephBalanceDb,
):
    """Test getting costs filtered by item_hash."""
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    # First get all resources to find an item_hash
    response = await ccn_api_client.get(f"{COSTS_URI}?include_details=1")
    assert response.status == 200, await response.text()
    data = await response.json()

    if len(data["resources"]) > 0:
        item_hash = data["resources"][0]["item_hash"]

        # Now filter by that item_hash
        response = await ccn_api_client.get(f"{COSTS_URI}?item_hash={item_hash}")
        assert response.status == 200, await response.text()
        data = await response.json()

        assert data["filters"]["item_hash"] == item_hash
        # Should only have 1 resource
        assert data["summary"]["resource_count"] == 1


@pytest.mark.asyncio
async def test_get_costs_combined_filters(
    ccn_api_client,
    message_processor: PendingMessageProcessor,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    instance_message_with_volumes_in_db,
    fixture_instance_message,
    user_balance: AlephBalanceDb,
):
    """Test getting costs with multiple filters combined."""
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    address = user_balance.address
    response = await ccn_api_client.get(
        f"{COSTS_URI}?address={address}&payment_type=hold&include_details=1"
    )
    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["filters"]["address"] == address
    assert data["filters"]["payment_type"] == "hold"
    assert "resources" in data

    # All resources should belong to the filtered address
    for resource in data["resources"]:
        assert resource["owner"] == address
        assert resource["payment_type"] == "hold"


@pytest.mark.asyncio
async def test_get_costs_nonexistent_address(ccn_api_client):
    """Test getting costs for an address that doesn't exist."""
    response = await ccn_api_client.get(
        f"{COSTS_URI}?address=0x0000000000000000000000000000000000000000"
    )
    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["summary"]["resource_count"] == 0
    assert float(data["summary"]["total_cost_hold"]) == 0.0


@pytest.mark.asyncio
async def test_get_costs_nonexistent_item_hash(ccn_api_client):
    """Test getting costs for an item_hash that doesn't exist."""
    response = await ccn_api_client.get(
        f"{COSTS_URI}?item_hash=nonexistent_hash_that_does_not_exist"
    )
    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["summary"]["resource_count"] == 0
    assert float(data["summary"]["total_cost_hold"]) == 0.0


@pytest.mark.asyncio
async def test_get_costs_page_out_of_range(
    ccn_api_client,
    message_processor: PendingMessageProcessor,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    instance_message_with_volumes_in_db,
    fixture_instance_message,
):
    """Test requesting a page beyond available data returns empty resources."""
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    response = await ccn_api_client.get(f"{COSTS_URI}?include_details=1&page=9999")
    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["pagination_page"] == 9999
    assert len(data["resources"]) == 0
    # Summary should still show totals (not affected by pagination)
    assert data["summary"]["resource_count"] >= 0


@pytest.mark.asyncio
async def test_get_costs_resource_cost_values_are_strings(
    ccn_api_client,
    message_processor: PendingMessageProcessor,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    instance_message_with_volumes_in_db,
    fixture_instance_message,
):
    """Test that cost values in resources are properly formatted strings."""
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    response = await ccn_api_client.get(f"{COSTS_URI}?include_details=1")
    assert response.status == 200, await response.text()
    data = await response.json()

    if len(data["resources"]) > 0:
        resource = data["resources"][0]
        # Verify cost fields are strings (properly formatted)
        assert isinstance(resource["cost_hold"], str)
        assert isinstance(resource["cost_stream"], str)
        assert isinstance(resource["cost_credit"], str)
        # Should be parseable as floats
        float(resource["cost_hold"])
        float(resource["cost_stream"])
        float(resource["cost_credit"])


@pytest.mark.asyncio
async def test_get_costs_all_payment_types(ccn_api_client):
    """Test that all valid payment types are accepted."""
    for payment_type in ["hold", "superfluid", "credit"]:
        response = await ccn_api_client.get(f"{COSTS_URI}?payment_type={payment_type}")
        assert response.status == 200, await response.text()
        data = await response.json()
        assert data["filters"]["payment_type"] == payment_type


@pytest.mark.asyncio
async def test_get_costs_summary_totals_match_resources(
    ccn_api_client,
    message_processor: PendingMessageProcessor,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    instance_message_with_volumes_in_db,
    fixture_instance_message,
):
    """Test that summary resource_count matches actual resources when no pagination limit."""
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    response = await ccn_api_client.get(
        f"{COSTS_URI}?include_details=1&pagination=1000"
    )
    assert response.status == 200, await response.text()
    data = await response.json()

    # When pagination is large enough, resource count should match
    if data["summary"]["resource_count"] <= 1000:
        assert len(data["resources"]) == data["summary"]["resource_count"]


@pytest.mark.asyncio
async def test_get_costs_detail_breakdown_types(
    ccn_api_client,
    message_processor: PendingMessageProcessor,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    instance_message_with_volumes_in_db,
    fixture_instance_message,
):
    """Test that detail breakdown contains expected cost types."""
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    response = await ccn_api_client.get(f"{COSTS_URI}?include_details=2")
    assert response.status == 200, await response.text()
    data = await response.json()

    if len(data["resources"]) > 0 and data["resources"][0].get("detail"):
        detail_types = {d["type"] for d in data["resources"][0]["detail"]}
        # Should have some known cost types
        known_types = {
            "EXECUTION",
            "STORAGE",
            "EXECUTION_VOLUME_PERSISTENT",
            "EXECUTION_VOLUME_INMUTABLE",
            "EXECUTION_VOLUME_DISCOUNT",
            "EXECUTION_INSTANCE_VOLUME_ROOTFS",
            "EXECUTION_PROGRAM_VOLUME_CODE",
            "EXECUTION_PROGRAM_VOLUME_RUNTIME",
            "EXECUTION_PROGRAM_VOLUME_DATA",
        }
        # At least some types should be from known types
        assert len(detail_types) > 0
        for dt in detail_types:
            assert dt in known_types, f"Unknown cost type: {dt}"
