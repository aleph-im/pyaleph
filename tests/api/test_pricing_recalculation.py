import datetime as dt
import json
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from aiohttp import web
from aleph_message.models import MessageType

from aleph.db.models import MessageDb
from aleph.db.models.account_costs import AccountCostsDb
from aleph.db.models.aggregates import AggregateElementDb
from aleph.toolkit.constants import (
    PRICE_AGGREGATE_KEY,
    PRICE_AGGREGATE_OWNER,
    ProductPriceType,
)
from aleph.web.controllers.prices import recalculate_message_costs


@pytest.fixture
def sample_messages(session_factory):
    """Create sample messages for testing cost recalculation."""
    base_time = dt.datetime(2024, 1, 1, 10, 0, 0, tzinfo=dt.timezone.utc)

    # Create sample instance message
    instance_message = MessageDb(
        item_hash="6e46535560b4372551e39a531e2ec24f6869766624921631e84e56598c8942b1",
        type=MessageType.instance,
        chain="ETH",
        sender="0xTest1",
        item_type="inline",
        content={
            "time": (base_time + dt.timedelta(hours=1)).timestamp(),
            "rootfs": {
                "parent": {
                    "ref": "6e46535560b4372551e39a531e2ec24f6869766624921631e84e56598c8942b1",
                    "use_latest": True,
                },
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
        item_hash="5369766624921631e84e56598c8942b16e46535560b4372551e39a531e2ec24f",
        type=MessageType.program,
        chain="ETH",
        sender="0xTest2",
        item_type="inline",
        content={
            "time": (base_time + dt.timedelta(hours=2)).timestamp(),
            "type": "vm-function",
            "on": {"http": True, "persistent": False},
            "code": {
                "ref": "5369766624921631e84e56598c8942b16e46535560b4372551e39a531e2ec24f",
                "encoding": "zip",
                "entrypoint": "main:app",
                "use_latest": True,
            },
            "runtime": {
                "ref": "1e84e56598c8942b16e46535560b4372551e39a531e2ec24f536976662492163",
                "use_latest": True,
                "comment": "test runtime",
            },
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
        item_hash="1e84e56598c8942b16e46535560b4372551e39a531e2ec24f536976662492163",
        type=MessageType.store,
        chain="ETH",
        sender="0xTest3",
        item_type="inline",
        content={
            "time": (base_time + dt.timedelta(hours=3)).timestamp(),
            "item_type": "storage",
            "item_hash": "1e84e56598c8942b16e46535560b4372551e39a531e2ec24f536976662492163",
            "address": "0xTest3",
        },
        time=base_time + dt.timedelta(hours=3),
        size=2048,
    )

    with session_factory() as session:
        session.add(instance_message)
        session.add(program_message)
        session.add(store_message)

        # Add MessageStatusDb entries as well, since get_executable_message checks for them
        from aleph.db.models import MessageStatusDb
        from aleph.types.message_status import MessageStatus

        for msg in [instance_message, program_message, store_message]:
            # Recalculation orders by observed time (confirmation/reception),
            # not the sender-supplied time. Anchor reception_time on the
            # staggered message time so the chronological order is deterministic
            # (MessageDb.__init__ otherwise defaults it to ~utc_now() for all).
            msg.reception_time = msg.time
            status = MessageStatusDb(
                item_hash=msg.item_hash,
                status=MessageStatus.PROCESSED,
                reception_time=msg.time,
            )
            session.add(status)

        session.commit()
        session.refresh(instance_message)
        session.refresh(program_message)
        session.refresh(store_message)

    # Return refreshed messages from a new session to ensure they are bound to a session if needed
    # but the fixture generally returns them detached or ready to be used.
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
            from unittest.mock import MagicMock

            from multidict import CIMultiDict

            from aleph.web.controllers.app_state_getters import (
                APP_STATE_SESSION_FACTORY,
                APP_STATE_STORAGE_SERVICE,
            )

            # Create a more robust mock request
            request = MagicMock(spec=web.Request)
            request.match_info = MagicMock()
            request.match_info.get.side_effect = lambda key, default=None: (
                match_info.get(key, default) if match_info else default
            )

            # Mock headers for auth token
            request.headers = CIMultiDict({"X-Auth-Token": "test-token"})

            # Mock the app state
            request.app = {
                APP_STATE_SESSION_FACTORY: session_factory,
                APP_STATE_STORAGE_SERVICE: MagicMock(),
            }

            return request

        return _create_mock_request

    @pytest.mark.asyncio
    @patch("aleph.web.controllers.prices.get_session_factory_from_request")
    async def test_recalculate_all_messages_empty_db(
        self, mock_get_session, session_factory, mock_request_factory
    ):
        """Test recalculation when no messages exist."""
        mock_get_session.return_value = session_factory

        # Ensure the DB is empty for this test
        with session_factory() as session:
            from aleph.db.models import MessageDb, MessageStatusDb

            session.query(MessageStatusDb).delete()
            session.query(MessageDb).delete()
            session.commit()

        request = mock_request_factory()

        response = await recalculate_message_costs.__wrapped__(request)

        assert response.status == 200
        response_data = json.loads(response.text)
        assert response_data["recalculated_count"] == 0
        assert response_data["total_messages"] == 0
        assert "No messages found" in response_data["message"]

    @pytest.mark.asyncio
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

        request = mock_request_factory(
            {
                "item_hash": "6e46535560b4372551e39a531e2ec24f6869766624921631e84e56598c8942b1"
            }
        )

        with patch("aleph.web.controllers.prices.get_detailed_costs") as mock_get_costs:
            mock_get_costs.return_value = []  # Mock empty costs

            response = await recalculate_message_costs.__wrapped__(request)

            assert response.status == 200
            response_data = json.loads(response.text)
            assert response_data["recalculated_count"] == 1
            assert response_data["total_messages"] == 1
            assert "historical pricing" in response_data["message"]

            # Should have called get_detailed_costs once
            assert mock_get_costs.call_count == 1

    @pytest.mark.asyncio
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

            response = await recalculate_message_costs.__wrapped__(request)

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

    @pytest.mark.asyncio
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

        def mock_get_costs(session, content, item_hash, pricing, extra_volumes=()):
            # Capture the pricing object used for each call
            pricing_calls.append((item_hash, pricing.type if pricing else None))
            return []

        with patch(
            "aleph.web.controllers.prices.get_detailed_costs",
            side_effect=mock_get_costs,
        ):
            response = await recalculate_message_costs.__wrapped__(request)

            assert response.status == 200

            # Should have made calls for all 3 messages
            assert len(pricing_calls) == 3

            # Verify the correct pricing types were used (based on message content and timeline)
            item_hashes = [call[0] for call in pricing_calls]
            assert (
                "6e46535560b4372551e39a531e2ec24f6869766624921631e84e56598c8942b1"
                in item_hashes
            )
            assert (
                "5369766624921631e84e56598c8942b16e46535560b4372551e39a531e2ec24f"
                in item_hashes
            )
            assert (
                "1e84e56598c8942b16e46535560b4372551e39a531e2ec24f536976662492163"
                in item_hashes
            )

    @pytest.mark.asyncio
    @patch("aleph.web.controllers.prices.get_session_factory_from_request")
    async def test_recalculate_with_errors(
        self, mock_get_session, session_factory, sample_messages, mock_request_factory
    ):
        """Test recalculation handling of errors."""
        mock_get_session.return_value = session_factory

        request = mock_request_factory()

        def mock_get_costs_with_error(
            session, content, item_hash, pricing, extra_volumes=()
        ):
            if (
                item_hash
                == "5369766624921631e84e56598c8942b16e46535560b4372551e39a531e2ec24f"
            ):
                raise ValueError("Test error for program message")
            return []

        with patch(
            "aleph.web.controllers.prices.get_detailed_costs",
            side_effect=mock_get_costs_with_error,
        ):
            response = await recalculate_message_costs.__wrapped__(request)

            assert response.status == 200
            response_data = json.loads(response.text)

            # Should have processed 2 successfully, 1 with error
            assert response_data["recalculated_count"] == 2
            assert response_data["total_messages"] == 3
            assert "errors" in response_data
            assert len(response_data["errors"]) == 1
            assert (
                response_data["errors"][0]["item_hash"]
                == "5369766624921631e84e56598c8942b16e46535560b4372551e39a531e2ec24f"
            )
            assert "Test error" in response_data["errors"][0]["error"]

    @pytest.mark.asyncio
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

        request = mock_request_factory(
            {
                "item_hash": "6e46535560b4372551e39a531e2ec24f6869766624921631e84e56598c8942b2"
            }
        )

        with pytest.raises(web.HTTPNotFound):
            await recalculate_message_costs.__wrapped__(request)

    @pytest.mark.asyncio
    @patch("aleph.web.controllers.prices.get_session_factory_from_request")
    async def test_chronological_processing_order(
        self, mock_get_session, session_factory, sample_messages, mock_request_factory
    ):
        """Test that messages are processed in chronological order."""
        mock_get_session.return_value = session_factory

        request = mock_request_factory()

        processed_order = []

        def mock_get_costs(session, content, item_hash, pricing, extra_volumes=()):
            processed_order.append(item_hash)
            return []

        with patch(
            "aleph.web.controllers.prices.get_detailed_costs",
            side_effect=mock_get_costs,
        ):
            response = await recalculate_message_costs.__wrapped__(request)

            assert response.status == 200

            # Should have processed in chronological order based on observed time
            expected_order = [
                "6e46535560b4372551e39a531e2ec24f6869766624921631e84e56598c8942b1",
                "5369766624921631e84e56598c8942b16e46535560b4372551e39a531e2ec24f",
                "1e84e56598c8942b16e46535560b4372551e39a531e2ec24f536976662492163",
            ]
            assert processed_order == expected_order

    @pytest.mark.asyncio
    @patch("aleph.web.controllers.prices.get_session_factory_from_request")
    async def test_recalculate_vprogram_keeps_type_on_fallback(
        self, mock_get_session, session_factory, mock_request_factory
    ):
        """A V-PROGRAM message recalculated against a historical pricing
        model that predates the `vprogram` key must still be priced with
        pricing.type == VPROGRAM: the numbers are borrowed from
        instance_confidential, but the product identity must not silently
        change to INSTANCE_CONFIDENTIAL (see resolve_price_type_key /
        ProductPricing.with_type).
        """
        from aleph_message.models import MessageType
        from messages.test_vprogram import VPROGRAM_CONTENT, VPROGRAM_ITEM_HASH

        from aleph.db.models import MessageStatusDb
        from aleph.types.message_status import MessageStatus

        base_time = dt.datetime(2024, 1, 1, 10, 0, 0, tzinfo=dt.timezone.utc)

        vprogram_message = MessageDb(
            item_hash=VPROGRAM_ITEM_HASH,
            type=MessageType.v_program,
            chain="ETH",
            sender=VPROGRAM_CONTENT["address"],
            item_type="inline",
            content=VPROGRAM_CONTENT,
            time=base_time,
            size=1024,
        )
        vprogram_message.reception_time = vprogram_message.time

        # Historical pricing update that only ever set instance_confidential,
        # i.e. from before the vprogram product type existed on chain.
        pricing_element = AggregateElementDb(
            item_hash="pricing_vprogram_fallback",
            key=PRICE_AGGREGATE_KEY,
            owner=PRICE_AGGREGATE_OWNER,
            content={
                ProductPriceType.INSTANCE_CONFIDENTIAL: {
                    "price": {
                        "storage": {
                            "holding": "0.05",
                            "credit": "0.17967489030626108",
                        },
                        "compute_unit": {"holding": "2000", "credit": "28500"},
                    },
                    "compute_unit": {
                        "vcpus": 1,
                        "disk_mib": 20480,
                        "memory_mib": 2048,
                    },
                }
            },
            creation_datetime=base_time - dt.timedelta(minutes=30),
        )

        with session_factory() as session:
            session.add(vprogram_message)
            session.add(pricing_element)
            session.add(
                MessageStatusDb(
                    item_hash=vprogram_message.item_hash,
                    status=MessageStatus.PROCESSED,
                    reception_time=vprogram_message.time,
                )
            )
            session.commit()

        mock_get_session.return_value = session_factory
        request = mock_request_factory()

        captured_pricing_types = {}

        def mock_get_costs(session, content, item_hash, pricing, extra_volumes=()):
            captured_pricing_types[item_hash] = pricing.type
            return []

        with (
            patch(
                "aleph.web.controllers.prices.get_detailed_costs",
                side_effect=mock_get_costs,
            ),
            patch(
                "aleph.web.controllers.prices.resolve_runtime_bundle_ref",
                new=AsyncMock(return_value="ba" * 32),
            ),
        ):
            response = await recalculate_message_costs.__wrapped__(request)

        assert response.status == 200
        response_data = json.loads(response.text)
        assert response_data["recalculated_count"] == 1
        assert not response_data.get("errors")
        assert captured_pricing_types[VPROGRAM_ITEM_HASH] == ProductPriceType.VPROGRAM

    @pytest.mark.asyncio
    @patch("aleph.web.controllers.prices.get_session_factory_from_request")
    async def test_recalculate_vprogram_keeps_runtime_bundle_row(
        self, mock_get_session, session_factory, mock_request_factory
    ):
        """Recalculating a V-PROGRAM message's costs must still resolve the
        runtime manifest to its bundle: the persisted `runtime` cost row
        must keep pointing at the bundle (not the manifest), with the
        EXECUTION_VPROGRAM_VOLUME type, exactly as it did when the message
        was first processed.
        """
        from api.test_vprogram_api import (
            BUNDLE_REF,
            insert_bundle_pin,
            insert_vprogram_refs,
        )
        from messages.test_vprogram import VPROGRAM_CONTENT, VPROGRAM_ITEM_HASH

        from aleph.db.accessors.cost import get_message_costs
        from aleph.db.models import MessageStatusDb
        from aleph.types.message_status import MessageStatus

        base_time = dt.datetime(2024, 1, 1, 10, 0, 0, tzinfo=dt.timezone.utc)

        vprogram_message = MessageDb(
            item_hash=VPROGRAM_ITEM_HASH,
            type=MessageType.v_program,
            chain="ETH",
            sender=VPROGRAM_CONTENT["address"],
            item_type="inline",
            content=VPROGRAM_CONTENT,
            time=base_time,
            size=1024,
        )
        vprogram_message.reception_time = vprogram_message.time

        with session_factory() as session:
            insert_vprogram_refs(session)
            insert_bundle_pin(session)
            session.add(vprogram_message)
            session.add(
                MessageStatusDb(
                    item_hash=vprogram_message.item_hash,
                    status=MessageStatus.PROCESSED,
                    reception_time=vprogram_message.time,
                )
            )
            session.commit()

        mock_get_session.return_value = session_factory
        request = mock_request_factory()

        with patch(
            "aleph.web.controllers.prices.resolve_runtime_bundle_ref",
            new=AsyncMock(return_value=BUNDLE_REF),
        ):
            response = await recalculate_message_costs.__wrapped__(request)

        assert response.status == 200
        response_data = json.loads(response.text)
        assert response_data["recalculated_count"] == 1
        assert not response_data.get("errors")

        with session_factory() as session:
            costs = list(
                get_message_costs(session=session, item_hash=VPROGRAM_ITEM_HASH)
            )
        runtime = next(c for c in costs if c.name == "runtime")
        assert runtime.ref == BUNDLE_REF
        assert runtime.type == "EXECUTION_VPROGRAM_VOLUME"

    @staticmethod
    def _seed_vprogram(session_factory, runtime_bundle_ref):
        """A processed V-PROGRAM message, its vms row (with or without the
        persisted bundle ref) and one pre-existing cost row."""
        from api.test_vprogram_api import insert_bundle_pin, insert_vprogram_refs
        from messages.test_vprogram import VPROGRAM_CONTENT, VPROGRAM_ITEM_HASH

        from aleph.db.models import MessageStatusDb
        from aleph.handlers.content.vprogram import vprogram_message_to_db
        from aleph.types.message_status import MessageStatus

        base_time = dt.datetime(2024, 1, 1, 10, 0, 0, tzinfo=dt.timezone.utc)

        vprogram_message = MessageDb(
            item_hash=VPROGRAM_ITEM_HASH,
            type=MessageType.v_program,
            chain="ETH",
            sender=VPROGRAM_CONTENT["address"],
            item_type="inline",
            content=VPROGRAM_CONTENT,
            time=base_time,
            size=1024,
        )
        vprogram_message.reception_time = vprogram_message.time

        with session_factory() as session:
            insert_vprogram_refs(session)
            insert_bundle_pin(session)
            session.add(vprogram_message)
            session.add(
                MessageStatusDb(
                    item_hash=VPROGRAM_ITEM_HASH,
                    status=MessageStatus.PROCESSED,
                    reception_time=vprogram_message.time,
                )
            )
            session.flush()

            vm = vprogram_message_to_db(vprogram_message, "ba" * 32)
            # Legacy rows (processed before the column existed) carry None.
            vm.runtime_bundle_ref = runtime_bundle_ref
            session.add(vm)

            session.add(
                AccountCostsDb(
                    owner=VPROGRAM_CONTENT["address"],
                    item_hash=VPROGRAM_ITEM_HASH,
                    type="EXECUTION",
                    name="old_cost",
                    payment_type="credit",
                    cost_hold=Decimal("999.99"),
                    cost_stream=Decimal("0.01"),
                )
            )
            session.commit()

        return VPROGRAM_ITEM_HASH

    @pytest.mark.asyncio
    @patch("aleph.web.controllers.prices.get_session_factory_from_request")
    async def test_recalculate_legacy_vprogram_keeps_costs_when_resolver_fails(
        self, mock_get_session, session_factory, mock_request_factory
    ):
        """A row written before `runtime_bundle_ref` existed still falls back
        to the resolver. When the resolver fails, the recalculation must
        report the error and leave the message's previous cost rows in place:
        never delete rows before their replacements exist."""
        from aleph.db.accessors.cost import get_message_costs
        from aleph.types.message_status import InvalidVProgramRuntime

        item_hash = self._seed_vprogram(session_factory, runtime_bundle_ref=None)

        mock_get_session.return_value = session_factory
        request = mock_request_factory()

        with patch(
            "aleph.web.controllers.prices.resolve_runtime_bundle_ref",
            new=AsyncMock(side_effect=InvalidVProgramRuntime("manifest is gone")),
        ) as resolver:
            response = await recalculate_message_costs.__wrapped__(request)

        assert resolver.await_count == 1
        assert response.status == 200
        response_data = json.loads(response.text)
        assert response_data["recalculated_count"] == 0
        assert len(response_data["errors"]) == 1
        assert response_data["errors"][0]["item_hash"] == item_hash

        with session_factory() as session:
            costs = list(get_message_costs(session=session, item_hash=item_hash))
        assert [c.name for c in costs] == ["old_cost"]
        assert Decimal(costs[0].cost_hold) == Decimal("999.99")

    @pytest.mark.asyncio
    @patch("aleph.web.controllers.prices.get_session_factory_from_request")
    async def test_recalculate_vprogram_reads_the_persisted_bundle_ref(
        self, mock_get_session, session_factory, mock_request_factory
    ):
        """With the bundle ref persisted on the vms row, recalculation must
        not read the runtime manifest again."""
        from api.test_vprogram_api import BUNDLE_REF

        from aleph.db.accessors.cost import get_message_costs

        item_hash = self._seed_vprogram(session_factory, runtime_bundle_ref=BUNDLE_REF)

        mock_get_session.return_value = session_factory
        request = mock_request_factory()

        with patch(
            "aleph.web.controllers.prices.resolve_runtime_bundle_ref",
            new=AsyncMock(side_effect=AssertionError("resolver must not be called")),
        ) as resolver:
            response = await recalculate_message_costs.__wrapped__(request)

        assert resolver.await_count == 0
        assert response.status == 200
        response_data = json.loads(response.text)
        assert response_data["recalculated_count"] == 1
        assert not response_data.get("errors")

        with session_factory() as session:
            costs = list(get_message_costs(session=session, item_hash=item_hash))
        assert "old_cost" not in {c.name for c in costs}
        runtime = next(c for c in costs if c.name == "runtime")
        assert runtime.ref == BUNDLE_REF


class TestPricingTimelineIntegration:
    """Integration tests for the complete pricing timeline feature."""

    @pytest.fixture
    def mock_request_factory(self, session_factory):
        """Factory to create mock requests."""

        def _create_mock_request(match_info=None):
            from unittest.mock import MagicMock

            from multidict import CIMultiDict

            from aleph.web.controllers.app_state_getters import (
                APP_STATE_SESSION_FACTORY,
                APP_STATE_STORAGE_SERVICE,
            )

            # Create a more robust mock request
            request = MagicMock(spec=web.Request)
            request.match_info = MagicMock()
            request.match_info.get.side_effect = lambda key, default=None: (
                match_info.get(key, default) if match_info else default
            )

            # Mock headers for auth token
            request.headers = CIMultiDict({"X-Auth-Token": "test-token"})

            # Mock the app state
            request.app = {
                APP_STATE_SESSION_FACTORY: session_factory,
                APP_STATE_STORAGE_SERVICE: MagicMock(),
            }

            return request

        return _create_mock_request

    @pytest.mark.asyncio
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

        def mock_get_costs(session, content, item_hash, pricing, extra_volumes=()):
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
            response = await recalculate_message_costs.__wrapped__(request)

            assert response.status == 200
            response_data = json.loads(response.text)
            assert response_data["recalculated_count"] == 3

            # Verify that different pricing was applied based on timeline
            # The exact values depend on the pricing timeline and merge logic,
            # but we can verify that historical pricing was considered
            assert len(pricing_usage) > 0
