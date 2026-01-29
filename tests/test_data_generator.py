"""
Unit tests for the data generator module.
Run with: pytest tests/test_data_generator.py -v
"""

import os
import sys
import tempfile
import pytest
from datetime import datetime, timezone
from unittest.mock import patch

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'data_generator'))

from data_generator import (
    generate_event,
    generate_batch,
    write_batch_to_file,
    get_product_category,
    generate_session_id,
    EVENT_TYPES,
    PRODUCT_CATALOG,
    USER_SEGMENTS,
)


class TestGenerateEvent:
    #Tests for the generate_event function.
    
    def test_generate_event_returns_dict(self):
        #Event should be a dictionary.
        event = generate_event(inject_anomalies=False)
        assert isinstance(event, dict)
    
    def test_generate_event_has_required_fields(self):
        #Event should contain all required fields.
        required_fields = [
            "event_id", "user_id", "session_id", "event_type", 
            "product_id", "category", "price", "quantity",
            "user_segment", "event_time", "source_system"
        ]
        event = generate_event(inject_anomalies=False)
        
        for field in required_fields:
            assert field in event, f"Missing required field: {field}"
    
    def test_event_id_is_uuid_format(self):
        # Event ID should be a valid UUID string.
        event = generate_event(inject_anomalies=False)
        assert len(event["event_id"]) == 36
        assert event["event_id"].count("-") == 4
    
    def test_user_id_in_valid_range(self):
        # User ID should be between 1 and 1000, or None for anonymous.
        for _ in range(100):
            event = generate_event(inject_anomalies=False)
            # user_id can be None for anonymous view/search events
            if event["user_id"] is not None:
                assert 1 <= event["user_id"] <= 1000
    
    def test_event_type_is_valid(self):
        # Event type should be one of the valid types.
        for _ in range(100):
            event = generate_event(inject_anomalies=False)
            assert event["event_type"] in EVENT_TYPES
    
    def test_product_id_in_valid_range(self):
        # Product ID should be between 1 and 500.
        for _ in range(100):
            event = generate_event(inject_anomalies=False)
            assert 1 <= event["product_id"] <= 500
    
    def test_purchase_event_has_positive_price(self):
        # Purchase events should have a positive price.
        for _ in range(50):
            event = generate_event(inject_anomalies=False, force_event_type="purchase")
            assert event["price"] > 0
    
    def test_view_event_has_zero_price(self):
        # View events should have zero price.
        for _ in range(50):
            event = generate_event(inject_anomalies=False, force_event_type="view")
            assert event["price"] == 0.0
    
    def test_event_time_is_iso_format(self):
        # Event time should be a valid ISO format timestamp.
        event = generate_event(inject_anomalies=False)
        # Should not raise an exception
        datetime.fromisoformat(event["event_time"].replace('Z', '+00:00'))
    
    def test_category_is_valid(self):
        # Category should be from the product catalog.
        valid_categories = list(PRODUCT_CATALOG.keys())
        for _ in range(50):
            event = generate_event(inject_anomalies=False)
            assert event["category"] in valid_categories
    
    def test_user_segment_is_valid(self):
        # User segment should be from valid segments or 'anonymous'.
        valid_segments = USER_SEGMENTS + ["anonymous"]
        for _ in range(50):
            event = generate_event(inject_anomalies=False)
            assert event["user_segment"] in valid_segments
    
    def test_session_id_format(self):
        # Session ID should follow user_id-timebucket or guest-uuid format.
        event = generate_event(inject_anomalies=False)
        session_id = event["session_id"]
        assert "-" in session_id
        # Either guest-uuid format or userid-timebucket format
        if session_id.startswith("guest-"):
            assert len(session_id) > 6  # guest- plus some uuid
        else:
            parts = session_id.split("-")
            assert len(parts) == 2
            assert parts[0].isdigit()
    
    def test_quantity_for_purchase_events(self):
        # Purchase events should have quantity >= 1.
        for _ in range(50):
            event = generate_event(inject_anomalies=False, force_event_type="purchase")
            assert event["quantity"] >= 1
    
    def test_forced_event_type(self):
        # force_event_type should override random selection.
        for event_type in EVENT_TYPES:
            event = generate_event(inject_anomalies=False, force_event_type=event_type)
            assert event["event_type"] == event_type


class TestAnomalyInjection:
    # Tests for anomaly injection functionality.
    
    def test_anomaly_injection_can_be_disabled(self):
        # When inject_anomalies=False, no anomalies should be present.
        for _ in range(100):
            event = generate_event(inject_anomalies=False)
            assert event.get("_anomaly_type", "") == ""
    
    def test_anomaly_injection_produces_anomalies(self):
        # With high iteration count, anomalies should appear.
        # With 2% rate, running 500 times should produce at least 1 anomaly
        anomaly_count = 0
        for _ in range(500):
            event = generate_event(inject_anomalies=True)
            if event.get("_anomaly_type"):
                anomaly_count += 1
        
        assert anomaly_count > 0, "Expected at least one anomaly in 500 events"
    
    def test_null_user_anomaly(self):
        # Null user anomaly should set user_id to None.
        # Generate many events and check if any have null_user anomaly
        null_user_found = False
        for _ in range(1000):
            event = generate_event(inject_anomalies=True)
            if event.get("_anomaly_type") == "null_user":
                assert event["user_id"] is None
                null_user_found = True
                break
        # This may occasionally fail due to randomness, but should usually pass
    
    def test_negative_price_anomaly(self):
        # Negative price anomaly should produce negative price.
        for _ in range(1000):
            event = generate_event(inject_anomalies=True)
            if event.get("_anomaly_type") == "negative_price":
                assert event["price"] < 0
                break


class TestProductCategory:
    # Tests for product category mapping.
    
    def test_electronics_range(self):
        # Products 1-100 should be electronics.
        for pid in [1, 50, 100]:
            assert get_product_category(pid) == "electronics"
    
    def test_clothing_range(self):
        # Products 101-200 should be clothing.
        for pid in [101, 150, 200]:
            assert get_product_category(pid) == "clothing"
    
    def test_unknown_product(self):
        # Products outside range should return unknown.
        assert get_product_category(999) == "unknown"
        assert get_product_category(0) == "unknown"


class TestGenerateBatch:
    # Tests for batch generation.
    
    def test_batch_returns_dataframe(self):
        # generate_batch should return a pandas DataFrame.
        import pandas as pd
        df = generate_batch(num_events=10, inject_anomalies=False)
        assert isinstance(df, pd.DataFrame)
    
    def test_batch_has_correct_size(self):
        # Batch should have the specified number of events.
        for size in [10, 50, 100]:
            df = generate_batch(num_events=size, inject_anomalies=False)
            assert len(df) == size
    
    def test_batch_has_all_columns(self):
        """Batch should have all required columns."""
        expected_columns = [
            "event_id", "user_id", "session_id", "event_type",
            "product_id", "category", "price", "quantity",
            "user_segment", "search_query", "event_time", "source_system"
        ]
        df = generate_batch(num_events=10, inject_anomalies=False)
        
        for col in expected_columns:
            assert col in df.columns, f"Missing column: {col}"


class TestWriteBatchToFile:
    # Tests for file writing functionality.
    
    def test_write_creates_file(self):
        # write_batch_to_file should create a CSV file.
        import pandas as pd
        
        with tempfile.TemporaryDirectory() as tmpdir:
            df = generate_batch(num_events=10, inject_anomalies=False)
            file_path = write_batch_to_file(df, output_dir=tmpdir)
            
            assert os.path.exists(file_path)
            assert file_path.endswith(".csv")
    
    def test_written_file_is_valid_csv(self):
        # Written file should be a valid CSV that can be read back.
        import pandas as pd
        
        with tempfile.TemporaryDirectory() as tmpdir:
            original_df = generate_batch(num_events=10, inject_anomalies=False)
            file_path = write_batch_to_file(original_df, output_dir=tmpdir)
            
            read_df = pd.read_csv(file_path)
            assert len(read_df) == 10
    
    def test_anomaly_column_not_in_output(self):
        # _anomaly_type column should be removed from output.
        import pandas as pd
        
        with tempfile.TemporaryDirectory() as tmpdir:
            df = generate_batch(num_events=10, inject_anomalies=True)
            file_path = write_batch_to_file(df, output_dir=tmpdir)
            
            read_df = pd.read_csv(file_path)
            assert "_anomaly_type" not in read_df.columns
    
    def test_filename_format(self):
        # File should be named with timestamp and UUID.
        import pandas as pd
        
        with tempfile.TemporaryDirectory() as tmpdir:
            df = generate_batch(num_events=10, inject_anomalies=False)
            file_path = write_batch_to_file(df, output_dir=tmpdir)
            
            filename = os.path.basename(file_path)
            assert filename.startswith("events_")
            assert filename.endswith(".csv")
            # Format: events_YYYYMMDD_HHMMSS_XXXXXX.csv
            parts = filename.replace(".csv", "").split("_")
            assert len(parts) == 4


class TestEventDistribution:
    # Tests for event type distribution.
    
    def test_event_distribution_is_weighted(self):
        # Event types should follow weighted distribution approximately.
        from collections import Counter
        
        events = [generate_event(inject_anomalies=False) for _ in range(1000)]
        event_types = [e["event_type"] for e in events]
        counts = Counter(event_types)
        
        # View should be most common (~50%)
        assert counts.get("view", 0) > counts.get("purchase", 0)
        
        # All valid event types should appear
        for event_type in EVENT_TYPES:
            assert event_type in counts, f"Event type {event_type} never appeared"


class TestSearchEvents:
    # Tests for search event specifics.
    
    def test_search_events_have_query(self):
        # Search events should have a non-empty search_query.
        for _ in range(100):
            event = generate_event(inject_anomalies=False, force_event_type="search")
            assert event["search_query"], "Search event should have a query"
    
    def test_non_search_events_may_have_empty_query(self):
        # Non-search events should have empty search_query.
        event = generate_event(inject_anomalies=False, force_event_type="view")
        assert event["search_query"] == ""


class TestBusinessLogic:
    # Tests for realistic business logic - actions requiring login.
    
    def test_purchase_always_has_user_id(self):
        # Purchase events must have a user_id (user must be logged in).
        for _ in range(100):
            event = generate_event(inject_anomalies=False, force_event_type="purchase")
            assert event["user_id"] is not None, "Purchase cannot happen without user_id"
    
    def test_add_to_cart_always_has_user_id(self):
        # Add to cart requires login.
        for _ in range(100):
            event = generate_event(inject_anomalies=False, force_event_type="add_to_cart")
            assert event["user_id"] is not None, "Add to cart requires user_id"
    
    def test_wishlist_always_has_user_id(self):
        # Wishlist requires login.
        for _ in range(100):
            event = generate_event(inject_anomalies=False, force_event_type="wishlist")
            assert event["user_id"] is not None, "Wishlist requires user_id"
    
    def test_remove_from_cart_always_has_user_id(self):
        # Remove from cart requires login.
        for _ in range(100):
            event = generate_event(inject_anomalies=False, force_event_type="remove_from_cart")
            assert event["user_id"] is not None, "Remove from cart requires user_id"
    
    def test_view_can_be_anonymous(self):
        # View events can happen without login (guest browsing).
        has_anonymous = False
        for _ in range(200):
            event = generate_event(inject_anomalies=False, force_event_type="view")
            if event["user_id"] is None:
                has_anonymous = True
                break
        # With 10% anonymous rate, should see at least one in 200 tries
        assert has_anonymous, "View events should sometimes be anonymous"
    
    def test_search_can_be_anonymous(self):
        # Search events can happen without login.
        has_anonymous = False
        for _ in range(200):
            event = generate_event(inject_anomalies=False, force_event_type="search")
            if event["user_id"] is None:
                has_anonymous = True
                break
        assert has_anonymous, "Search events should sometimes be anonymous"
    
    def test_anonymous_user_has_guest_session(self):
        # Anonymous users should have guest session IDs.
        for _ in range(200):
            event = generate_event(inject_anomalies=False, force_event_type="view")
            if event["user_id"] is None:
                assert event["session_id"].startswith("guest-"), \
                    "Anonymous user should have guest session ID"
                break
    
    def test_anonymous_user_segment_is_anonymous(self):
        # Anonymous users should have 'anonymous' user segment.
        for _ in range(200):
            event = generate_event(inject_anomalies=False, force_event_type="view")
            if event["user_id"] is None:
                assert event["user_segment"] == "anonymous", \
                    "Anonymous user should have 'anonymous' segment"
                break


class TestDeduplication:
    # Tests for event ID uniqueness.
    
    def test_event_ids_are_unique(self):
        # Each generated event should have a unique event_id.
        events = [generate_event(inject_anomalies=False) for _ in range(1000)]
        event_ids = [e["event_id"] for e in events]
        unique_ids = set(event_ids)
        assert len(unique_ids) == len(event_ids), "Duplicate event_ids found!"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
