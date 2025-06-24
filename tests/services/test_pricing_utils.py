import datetime as dt
from decimal import Decimal
from unittest.mock import patch

import pytest
from aleph_message.models import InstanceContent, ProgramContent, StoreContent

from aleph.db.models.aggregates import AggregateElementDb
from aleph.services.pricing_utils import (
    build_default_pricing_model,
    build_pricing_model_from_aggregate,
    get_pricing_timeline,
    get_pricing_aggregate_history,
)
from aleph.toolkit.constants import (
    PRICE_AGGREGATE_KEY,
    PRICE_AGGREGATE_OWNER,
)
from aleph.types.cost import ProductPriceType, ProductPricing
from aleph.types.db_session import DbSessionFactory


@pytest.fixture
def sample_pricing_aggregate_content():
    """Sample pricing aggregate content with ProductPriceType keys."""
    return {
        ProductPriceType.STORAGE: {
            "price": {"storage": {"holding": "0.5"}}
        },
        ProductPriceType.PROGRAM: {
            "price": {
                "storage": {"payg": "0.000001", "holding": "0.1"},
                "compute_unit": {"payg": "0.02", "holding": "300"},
            },
            "compute_unit": {
                "vcpus": 1,
                "disk_mib": 2048,
                "memory_mib": 2048,
            },
        },
        ProductPriceType.INSTANCE: {
            "price": {
                "storage": {"payg": "0.000001", "holding": "0.1"},
                "compute_unit": {"payg": "0.1", "holding": "1500"},
            },
            "compute_unit": {
                "vcpus": 1,
                "disk_mib": 20480,
                "memory_mib": 2048,
            },
        },
    }


@pytest.fixture
def pricing_aggregate_elements(session_factory):
    """Create sample pricing aggregate elements for timeline testing."""
    base_time = dt.datetime(2024, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
    
    # First pricing update - only storage pricing
    element1 = AggregateElementDb(
        item_hash="pricing_update_1",
        key=PRICE_AGGREGATE_KEY,
        owner=PRICE_AGGREGATE_OWNER,
        content={
            ProductPriceType.STORAGE: {
                "price": {"storage": {"holding": "0.2"}}
            }
        },
        creation_datetime=base_time + dt.timedelta(hours=1)
    )
    
    # Second pricing update - adds program pricing
    element2 = AggregateElementDb(
        item_hash="pricing_update_2", 
        key=PRICE_AGGREGATE_KEY,
        owner=PRICE_AGGREGATE_OWNER,
        content={
            ProductPriceType.PROGRAM: {
                "price": {
                    "storage": {"payg": "0.000001", "holding": "0.08"},
                    "compute_unit": {"payg": "0.015", "holding": "250"},
                },
                "compute_unit": {
                    "vcpus": 1,
                    "disk_mib": 2048,
                    "memory_mib": 2048,
                },
            }
        },
        creation_datetime=base_time + dt.timedelta(hours=2)
    )
    
    # Third pricing update - updates storage and adds instance pricing
    element3 = AggregateElementDb(
        item_hash="pricing_update_3",
        key=PRICE_AGGREGATE_KEY,
        owner=PRICE_AGGREGATE_OWNER,
        content={
            ProductPriceType.STORAGE: {
                "price": {"storage": {"holding": "0.3"}}
            },
            ProductPriceType.INSTANCE: {
                "price": {
                    "storage": {"payg": "0.000001", "holding": "0.05"},
                    "compute_unit": {"payg": "0.06", "holding": "1200"},
                },
                "compute_unit": {
                    "vcpus": 1,
                    "disk_mib": 20480,
                    "memory_mib": 2048,
                },
            }
        },
        creation_datetime=base_time + dt.timedelta(hours=3)
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


class TestBuildPricingModelFromAggregate:
    """Tests for building pricing models from aggregate content."""
    
    def test_build_pricing_model_from_aggregate(self, sample_pricing_aggregate_content):
        """Test building pricing model from aggregate content."""
        pricing_model = build_pricing_model_from_aggregate(sample_pricing_aggregate_content)
        
        # Check that we got ProductPricing objects for each type
        assert len(pricing_model) == 3
        assert ProductPriceType.STORAGE in pricing_model
        assert ProductPriceType.PROGRAM in pricing_model
        assert ProductPriceType.INSTANCE in pricing_model
        
        # Check that each is a ProductPricing object
        for price_type, pricing in pricing_model.items():
            assert isinstance(pricing, ProductPricing)
            assert pricing.type == price_type
    
    def test_build_pricing_model_with_missing_types(self):
        """Test building pricing model with some missing product types."""
        partial_content = {
            ProductPriceType.STORAGE: {
                "price": {"storage": {"holding": "0.5"}}
            }
        }
        
        pricing_model = build_pricing_model_from_aggregate(partial_content)
        
        assert len(pricing_model) == 1
        assert ProductPriceType.STORAGE in pricing_model
        assert ProductPriceType.PROGRAM not in pricing_model
    
    def test_build_pricing_model_with_invalid_data(self):
        """Test building pricing model with invalid pricing data."""
        invalid_content = {
            ProductPriceType.STORAGE: {
                "invalid": "data"  # Missing required "price" key
            }
        }
        
        # Should handle the error gracefully and return empty model
        pricing_model = build_pricing_model_from_aggregate(invalid_content)
        assert len(pricing_model) == 0


class TestBuildDefaultPricingModel:
    """Tests for building the default pricing model."""
    
    def test_build_default_pricing_model(self):
        """Test building the default pricing model from constants."""
        pricing_model = build_default_pricing_model()
        
        # Should contain all the expected product types from DEFAULT_PRICE_AGGREGATE
        expected_types = [
            ProductPriceType.PROGRAM,
            ProductPriceType.STORAGE,
            ProductPriceType.INSTANCE,
            ProductPriceType.PROGRAM_PERSISTENT,
            ProductPriceType.INSTANCE_GPU_PREMIUM,
            ProductPriceType.INSTANCE_CONFIDENTIAL,
            ProductPriceType.INSTANCE_GPU_STANDARD,
            ProductPriceType.WEB3_HOSTING,
        ]
        
        for price_type in expected_types:
            assert price_type in pricing_model
            assert isinstance(pricing_model[price_type], ProductPricing)


class TestGetPricingAggregateHistory:
    """Tests for retrieving pricing aggregate history."""
    
    def test_get_pricing_aggregate_history_empty(self, session_factory):
        """Test getting pricing history when no elements exist."""
        with session_factory() as session:
            history = get_pricing_aggregate_history(session)
            assert len(history) == 0
    
    def test_get_pricing_aggregate_history_with_elements(self, session_factory, pricing_aggregate_elements):
        """Test getting pricing history with existing elements."""
        with session_factory() as session:
            history = get_pricing_aggregate_history(session)
            
            assert len(history) == 3
            
            # Should be ordered chronologically
            assert history[0].creation_datetime < history[1].creation_datetime
            assert history[1].creation_datetime < history[2].creation_datetime
            
            # Check content
            assert ProductPriceType.STORAGE in history[0].content
            assert ProductPriceType.PROGRAM in history[1].content
            assert ProductPriceType.STORAGE in history[2].content
            assert ProductPriceType.INSTANCE in history[2].content


class TestGetPricingTimeline:
    """Tests for getting the pricing timeline."""
    
    def test_get_pricing_timeline_empty(self, session_factory):
        """Test getting pricing timeline when no aggregate elements exist."""
        with session_factory() as session:
            timeline = get_pricing_timeline(session)
            
            # Should have at least the default pricing
            assert len(timeline) == 1
            timestamp, pricing_model = timeline[0]
            
            # Should use minimum datetime for default pricing
            assert timestamp == dt.datetime.min.replace(tzinfo=dt.timezone.utc)
            assert isinstance(pricing_model, dict)
            assert ProductPriceType.STORAGE in pricing_model
    
    def test_get_pricing_timeline_with_elements(self, session_factory, pricing_aggregate_elements):
        """Test getting pricing timeline with aggregate elements."""
        with session_factory() as session:
            timeline = get_pricing_timeline(session)
            
            # Should have default + 3 pricing updates
            assert len(timeline) == 4
            
            # Check chronological order
            for i in range(len(timeline) - 1):
                assert timeline[i][0] <= timeline[i + 1][0]
            
            # Check content evolution
            default_timestamp, default_model = timeline[0]
            first_timestamp, first_model = timeline[1]
            second_timestamp, second_model = timeline[2]
            third_timestamp, third_model = timeline[3]
            
            # First update: only storage pricing
            assert ProductPriceType.STORAGE in first_model
            storage_pricing_1 = first_model[ProductPriceType.STORAGE]
            assert storage_pricing_1.price.storage.holding == Decimal("0.2")
            
            # Second update: storage + program pricing (cumulative)
            assert ProductPriceType.STORAGE in second_model
            assert ProductPriceType.PROGRAM in second_model
            storage_pricing_2 = second_model[ProductPriceType.STORAGE]
            assert storage_pricing_2.price.storage.holding == Decimal("0.2")  # Still from first update
            
            # Third update: updated storage + program + instance pricing (cumulative)
            assert ProductPriceType.STORAGE in third_model
            assert ProductPriceType.PROGRAM in third_model
            assert ProductPriceType.INSTANCE in third_model
            storage_pricing_3 = third_model[ProductPriceType.STORAGE]
            assert storage_pricing_3.price.storage.holding == Decimal("0.3")  # Updated value
    
    def test_pricing_timeline_cumulative_merging(self, session_factory):
        """Test that pricing timeline properly merges cumulative changes."""
        base_time = dt.datetime(2024, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
        
        # Create elements that update different parts of pricing
        element1 = AggregateElementDb(
            item_hash="test_1",
            key=PRICE_AGGREGATE_KEY,
            owner=PRICE_AGGREGATE_OWNER,
            content={
                ProductPriceType.STORAGE: {
                    "price": {"storage": {"holding": "1.0"}}
                },
                ProductPriceType.PROGRAM: {
                    "price": {
                        "storage": {"holding": "0.1"},
                        "compute_unit": {"holding": "100"},
                    },
                    "compute_unit": {"vcpus": 1, "disk_mib": 1024, "memory_mib": 1024},
                }
            },
            creation_datetime=base_time + dt.timedelta(hours=1)
        )
        
        # Second element only updates storage, should preserve program settings
        element2 = AggregateElementDb(
            item_hash="test_2",
            key=PRICE_AGGREGATE_KEY,
            owner=PRICE_AGGREGATE_OWNER,
            content={
                ProductPriceType.STORAGE: {
                    "price": {"storage": {"holding": "2.0"}}  # Updated price
                }
            },
            creation_datetime=base_time + dt.timedelta(hours=2)
        )
        
        with session_factory() as session:
            session.add(element1)
            session.add(element2)
            session.commit()
            
            timeline = get_pricing_timeline(session)
            
            # Should have default + 2 updates
            assert len(timeline) == 3
            
            # Check final state has both storage and program, with updated storage price
            final_timestamp, final_model = timeline[2]
            
            assert ProductPriceType.STORAGE in final_model
            assert ProductPriceType.PROGRAM in final_model
            
            # Storage should have updated price
            storage_pricing = final_model[ProductPriceType.STORAGE]
            assert storage_pricing.price.storage.holding == Decimal("2.0")
            
            # Program should still have original settings
            program_pricing = final_model[ProductPriceType.PROGRAM]
            assert program_pricing.price.storage.holding == Decimal("0.1")
            assert program_pricing.price.compute_unit.holding == Decimal("100")


class TestPricingTimelineIntegration:
    """Integration tests for pricing timeline with real message types."""
    
    @pytest.fixture
    def sample_instance_content(self):
        """Sample instance content for testing."""
        return InstanceContent.model_validate({
            "time": 1701099523.849,
            "rootfs": {
                "parent": {
                    "ref": "549ec451d9b099cad112d4aaa2c00ac40fb6729a92ff252ff22eef0b5c3cb613",
                    "use_latest": True,
                },
                "size_mib": 20480,
                "persistence": "host",
            },
            "address": "0xTest",
            "volumes": [],
            "metadata": {"name": "Test Instance"},
            "resources": {"vcpus": 1, "memory": 2048, "seconds": 30},
            "allow_amend": False,
            "environment": {"internet": True, "aleph_api": True},
        })
    
    def test_pricing_timeline_with_message_processing(self, session_factory, pricing_aggregate_elements, sample_instance_content):
        """Test that pricing timeline can be used for historical message cost calculation."""
        with session_factory() as session:
            timeline = get_pricing_timeline(session)
            
            # Simulate processing a message at different points in time
            message_time_1 = dt.datetime(2024, 1, 1, 13, 30, 0, tzinfo=dt.timezone.utc)  # Between first and second update
            message_time_2 = dt.datetime(2024, 1, 1, 15, 30, 0, tzinfo=dt.timezone.utc)  # After all updates
            
            # Find applicable pricing for each message time
            pricing_1 = None
            pricing_2 = None
            
            for timestamp, pricing_model in timeline:
                if timestamp <= message_time_1:
                    pricing_1 = pricing_model
                if timestamp <= message_time_2:
                    pricing_2 = pricing_model
            
            # At time 1, should have storage pricing but not instance pricing
            assert pricing_1 is not None
            assert ProductPriceType.STORAGE in pricing_1
            # Instance pricing not added until third update
            assert ProductPriceType.INSTANCE not in pricing_1
            
            # At time 2, should have both storage and instance pricing
            assert pricing_2 is not None
            assert ProductPriceType.STORAGE in pricing_2
            assert ProductPriceType.INSTANCE in pricing_2
            
            # Storage pricing should be different between the two time points
            if ProductPriceType.STORAGE in pricing_1 and ProductPriceType.STORAGE in pricing_2:
                storage_1 = pricing_1[ProductPriceType.STORAGE]
                storage_2 = pricing_2[ProductPriceType.STORAGE]
                # Should have different prices due to the third update
                assert storage_1.price.storage.holding != storage_2.price.storage.holding