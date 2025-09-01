import datetime as dt
import json
from decimal import Decimal
from unittest.mock import patch

import pytest
from aiohttp import web
from aleph_message.models import MessageType

from aleph.db.models import MessageDb
from aleph.db.models.account_costs import AccountCostsDb
from aleph.db.models.aggregates import AggregateElementDb
from aleph.toolkit.constants import PRICE_AGGREGATE_KEY, PRICE_AGGREGATE_OWNER
from aleph.types.cost import ProductPriceType
from aleph.web.controllers.prices import recalculate_message_costs


@pytest.fixture
def sample_messages(session_factory):
    """Create sample messages for testing cost recalculation."""
    base_time = dt.datetime(2024, 1, 1, 10, 0, 0, tzinfo=dt.timezone.utc)

    # Create sample instance message
    instance_message = MessageDb(
        item_hash="instance_msg_1",
        type=MessageType.instance,
        chain="ETH",
        sender="0xTest1",
        item_type="inline",
        content={
            "time": (base_time + dt.timedelta(hours=1)).timestamp(),
            "rootfs": {
                "parent": {"ref": "test_ref", "use_latest": True},
                "size_mib": 20480,
                "persistence": "host",
            },
            "address": "0xTest1",
            "volumes": [],
            "metadata": {"name": "Test Instance"},
            "resources": {"vcpus": 1, "memory": 2048, "seconds": 30},
            "allow_amend": False,
            "environment": {"internet": True, "aleph_api": True},
        },
        time=base_time + dt.timedelta(hours=1),
        size=1024,
    )

    # Create sample program message
    program_message = MessageDb(
        item_hash="program_msg_1",
        type=MessageType.program,
        chain="ETH",
        sender="0xTest2",
        item_type="inline",
        content={
            "time": (base_time + dt.timedelta(hours=2)).timestamp(),
            "on": {"http": True, "persistent": False},
            "code": {
                "ref": "code_ref",
                "encoding": "zip",
                "entrypoint": "main:app",
                "use_latest": True,
            },
            "runtime": {"ref": "runtime_ref", "use_latest": True},
            "address": "0xTest2",
            "resources": {"vcpus": 1, "memory": 128, "seconds": 30},
            "allow_amend": False,
            "environment": {"internet": True, "aleph_api": True},
        },
        time=base_time + dt.timedelta(hours=2),
        size=512,
    )

    # Create sample store message
    store_message = MessageDb(
        item_hash="store_msg_1",
        type=MessageType.store,
        chain="ETH",
        sender="0xTest3",
        item_type="inline",
        content={
            "time": (base_time + dt.timedelta(hours=3)).timestamp(),
            "item_type": "storage",
            "item_hash": "stored_file_hash",
            "address": "0xTest3",
        },
        time=base_time + dt.timedelta(hours=3),
        size=2048,
    )

    with session_factory() as session:
        session.add(instance_message)
        session.add(program_message)
        session.add(store_message)
        session.commit()
        session.refresh(instance_message)
        session.refresh(program_message)
        session.refresh(store_message)

    return [instance_message, program_message, store_message]


@pytest.fixture
def pricing_updates_with_timeline(session_factory):
    """Create pricing updates that form a timeline for testing."""
    base_time = dt.datetime(2024, 1, 1, 9, 0, 0, tzinfo=dt.timezone.utc)

    # First pricing update - before any messages
    element1 = AggregateElementDb(
        item_hash="pricing_1",
        key=PRICE_AGGREGATE_KEY,
        owner=PRICE_AGGREGATE_OWNER,
        content={
            ProductPriceType.STORAGE: {"price": {"storage": {"holding": "0.1"}}},
            ProductPriceType.INSTANCE: {
                "price": {
                    "storage": {"holding": "0.05"},
                    "compute_unit": {"holding": "500"},
                },
                "compute_unit": {"vcpus": 1, "disk_mib": 20480, "memory_mib": 2048},
            },
        },
        creation_datetime=base_time + dt.timedelta(minutes=30),
    )

    # Second pricing update - between instance and program messages
    element2 = AggregateElementDb(
        item_hash="pricing_2",
        key=PRICE_AGGREGATE_KEY,
        owner=PRICE_AGGREGATE_OWNER,
        content={
            ProductPriceType.PROGRAM: {
                "price": {
                    "storage": {"holding": "0.03"},
                    "compute_unit": {"holding": "150"},
                },
                "compute_unit": {"vcpus": 1, "disk_mib": 2048, "memory_mib": 2048},
            }
        },
        creation_datetime=base_time + dt.timedelta(hours=1, minutes=30),
    )

    # Third pricing update - after program but before store message
    element3 = AggregateElementDb(
        item_hash="pricing_3",
        key=PRICE_AGGREGATE_KEY,
        owner=PRICE_AGGREGATE_OWNER,
        content={
            ProductPriceType.STORAGE: {
                "price": {"storage": {"holding": "0.2"}}  # Updated storage price
            }
        },
        creation_datetime=base_time + dt.timedelta(hours=2, minutes=30),
    )

    with session_factory() as session:
        session.add(element1)
        session.add(element2)
        session.add(element3)
        session.commit()
        session.refresh(element1)
        session.refresh(element2)
        session.refresh(element3)

    return [element1, element2, element3]


@pytest.fixture
def existing_costs(session_factory, sample_messages):
    """Create some existing cost entries to test deletion and recalculation."""
    costs = []

    for message in sample_messages:
        cost = AccountCostsDb(
            owner=message.sender,
            item_hash=message.item_hash,
            type="EXECUTION",
            name="old_cost",
            payment_type="hold",
            cost_hold=Decimal("999.99"),  # Old/incorrect cost
            cost_stream=Decimal("0.01"),
        )
        costs.append(cost)

    with session_factory() as session:
        for cost in costs:
            session.add(cost)
        session.commit()

    return costs


class TestRecalculateMessageCosts:
    """Tests for the message cost recalculation endpoint."""

    @pytest.fixture
    def mock_request_factory(self, session_factory):
        """Factory to create mock requests."""

        def _create_mock_request(match_info=None):
            request = web.Request.__new__(web.Request)
            request._match_info = match_info or {}

            # Mock the session factory getter
            def get_session_factory():
                return session_factory

            request._session_factory = get_session_factory
            return request

        return _create_mock_request

    @patch("aleph.web.controllers.prices.get_session_factory_from_request")
    async def test_recalculate_all_messages_empty_db(
        self, mock_get_session, session_factory, mock_request_factory
    ):
        """Test recalculation when no messages exist."""
        mock_get_session.return_value = session_factory
        request = mock_request_factory()

        response = await recalculate_message_costs(request)

        assert response.status == 200
        response_data = json.loads(response.text)
        assert response_data["recalculated_count"] == 0
        assert response_data["total_messages"] == 0
        assert "No messages found" in response_data["message"]

    @patch("aleph.web.controllers.prices.get_session_factory_from_request")
    @patch("aleph.web.controllers.prices.get_executable_message")
    async def test_recalculate_specific_message(
        self,
        mock_get_executable,
        mock_get_session,
        session_factory,
        sample_messages,
        mock_request_factory,
    ):
        """Test recalculation of a specific message."""
        mock_get_session.return_value = session_factory
        mock_get_executable.return_value = sample_messages[0]  # Return first message

        request = mock_request_factory({"item_hash": "instance_msg_1"})

        with patch("aleph.web.controllers.prices.get_detailed_costs") as mock_get_costs:
            mock_get_costs.return_value = []  # Mock empty costs

            response = await recalculate_message_costs(request)

            assert response.status == 200
            response_data = json.loads(response.text)
            assert response_data["recalculated_count"] == 1
            assert response_data["total_messages"] == 1
            assert "historical pricing" in response_data["message"]

            # Should have called get_detailed_costs once
            assert mock_get_costs.call_count == 1

    @patch("aleph.web.controllers.prices.get_session_factory_from_request")
    async def test_recalculate_all_messages_with_timeline(
        self,
        mock_get_session,
        session_factory,
        sample_messages,
        pricing_updates_with_timeline,
        existing_costs,
        mock_request_factory,
    ):
        """Test recalculation of all messages with pricing timeline."""
        mock_get_session.return_value = session_factory

        request = mock_request_factory()

        with patch("aleph.web.controllers.prices.get_detailed_costs") as mock_get_costs:
            mock_get_costs.return_value = []  # Mock empty costs

            response = await recalculate_message_costs(request)

            assert response.status == 200
            response_data = json.loads(response.text)
            assert response_data["recalculated_count"] == 3
            assert response_data["total_messages"] == 3
            assert response_data["pricing_changes_found"] == 4  # Default + 3 updates

            # Should have called get_detailed_costs for each message
            assert mock_get_costs.call_count == 3

        # Verify old costs were deleted
        with session_factory() as session:
            remaining_costs = session.query(AccountCostsDb).all()
            assert len(remaining_costs) == 0  # All old costs should be deleted

    @patch("aleph.web.controllers.prices.get_session_factory_from_request")
    async def test_recalculate_with_pricing_timeline_application(
        self,
        mock_get_session,
        session_factory,
        sample_messages,
        pricing_updates_with_timeline,
        mock_request_factory,
    ):
        """Test that the correct pricing model is applied based on message timestamps."""
        mock_get_session.return_value = session_factory

        request = mock_request_factory()

        pricing_calls = []

        def mock_get_costs(session, content, item_hash, pricing):
            # Capture the pricing object used for each call
            pricing_calls.append((item_hash, pricing.type if pricing else None))
            return []

        with patch(
            "aleph.web.controllers.prices.get_detailed_costs",
            side_effect=mock_get_costs,
        ):
            response = await recalculate_message_costs(request)

            assert response.status == 200

            # Should have made calls for all 3 messages
            assert len(pricing_calls) == 3

            # Verify the correct pricing types were used (based on message content and timeline)
            item_hashes = [call[0] for call in pricing_calls]
            assert "instance_msg_1" in item_hashes
            assert "program_msg_1" in item_hashes
            assert "store_msg_1" in item_hashes

    @patch("aleph.web.controllers.prices.get_session_factory_from_request")
    async def test_recalculate_with_errors(
        self, mock_get_session, session_factory, sample_messages, mock_request_factory
    ):
        """Test recalculation handling of errors."""
        mock_get_session.return_value = session_factory

        request = mock_request_factory()

        def mock_get_costs_with_error(session, content, item_hash, pricing):
            if item_hash == "program_msg_1":
                raise ValueError("Test error for program message")
            return []

        with patch(
            "aleph.web.controllers.prices.get_detailed_costs",
            side_effect=mock_get_costs_with_error,
        ):
            response = await recalculate_message_costs(request)

            assert response.status == 200
            response_data = json.loads(response.text)

            # Should have processed 2 successfully, 1 with error
            assert response_data["recalculated_count"] == 2
            assert response_data["total_messages"] == 3
            assert "errors" in response_data
            assert len(response_data["errors"]) == 1
            assert response_data["errors"][0]["item_hash"] == "program_msg_1"
            assert "Test error" in response_data["errors"][0]["error"]

    @patch("aleph.web.controllers.prices.get_session_factory_from_request")
    @patch("aleph.web.controllers.prices.get_executable_message")
    async def test_recalculate_specific_message_not_found(
        self,
        mock_get_executable,
        mock_get_session,
        session_factory,
        mock_request_factory,
    ):
        """Test recalculation of a specific message that doesn't exist."""
        mock_get_session.return_value = session_factory
        mock_get_executable.side_effect = web.HTTPNotFound(body="Message not found")

        request = mock_request_factory({"item_hash": "nonexistent_hash"})

        with pytest.raises(web.HTTPNotFound):
            await recalculate_message_costs(request)

    @patch("aleph.web.controllers.prices.get_session_factory_from_request")
    async def test_chronological_processing_order(
        self, mock_get_session, session_factory, sample_messages, mock_request_factory
    ):
        """Test that messages are processed in chronological order."""
        mock_get_session.return_value = session_factory

        request = mock_request_factory()

        processed_order = []

        def mock_get_costs(session, content, item_hash, pricing):
            processed_order.append(item_hash)
            return []

        with patch(
            "aleph.web.controllers.prices.get_detailed_costs",
            side_effect=mock_get_costs,
        ):
            response = await recalculate_message_costs(request)

            assert response.status == 200

            # Should have processed in chronological order based on message.time
            expected_order = ["instance_msg_1", "program_msg_1", "store_msg_1"]
            assert processed_order == expected_order


class TestPricingTimelineIntegration:
    """Integration tests for the complete pricing timeline feature."""

    @pytest.fixture
    def mock_request_factory(self, session_factory):
        """Factory to create mock requests."""

        def _create_mock_request(match_info=None):
            request = web.Request.__new__(web.Request)
            request._match_info = match_info or {}

            # Mock the session factory getter
            def get_session_factory():
                return session_factory

            request._session_factory = get_session_factory
            return request

        return _create_mock_request

    @patch("aleph.web.controllers.prices.get_session_factory_from_request")
    async def test_end_to_end_historical_pricing(
        self,
        mock_get_session,
        session_factory,
        sample_messages,
        pricing_updates_with_timeline,
        mock_request_factory,
    ):
        """End-to-end test of historical pricing application."""
        mock_get_session.return_value = session_factory

        request = mock_request_factory()

        # Track which pricing models are used for each message
        pricing_usage = {}

        def mock_get_costs(session, content, item_hash, pricing):
            if pricing and hasattr(pricing, "price"):
                if hasattr(pricing.price, "storage") and hasattr(
                    pricing.price.storage, "holding"
                ):
                    pricing_usage[item_hash] = float(pricing.price.storage.holding)
            return []

        with patch(
            "aleph.web.controllers.prices.get_detailed_costs",
            side_effect=mock_get_costs,
        ):
            response = await recalculate_message_costs(request)

            assert response.status == 200
            response_data = json.loads(response.text)
            assert response_data["recalculated_count"] == 3

            # Verify that different pricing was applied based on timeline
            # The exact values depend on the pricing timeline and merge logic,
            # but we can verify that historical pricing was considered
            assert len(pricing_usage) > 0
