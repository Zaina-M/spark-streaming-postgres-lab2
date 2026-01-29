"""
Unit tests for Spark transformations (without requiring a full Spark session).
These tests validate the transformation logic independently.
Run with: pytest tests/test_transformations.py -v
"""

from multiprocessing import Event
import pytest
from datetime import datetime, timezone, timedelta


class TestValidationRules:
    # Tests for data validation rules that will be applied in Spark.
    
    # Define validation rules as pure Python functions for testing
    VALID_EVENT_TYPES = ["view", "purchase", "add_to_cart", "remove_from_cart", "wishlist", "search"]
    MAX_VALID_PRICE = 10000.0
    MIN_VALID_PRICE = 0.0
    
    def validate_event(self, event: dict) -> tuple:
    
        #Validate an event and return (is_valid, error_message).
        
        if event.get("event_id") is None:
            return False, "null_event_id"
        if event.get("user_id") is None:
            return False, "null_user_id"
        if event.get("product_id") is None:
            return False, "null_product_id"
        if event.get("event_type") is None:
            return False, "null_event_type"
        if event.get("event_type") not in self.VALID_EVENT_TYPES:
            return False, "invalid_event_type"
        if event.get("price", 0) < self.MIN_VALID_PRICE:
            return False, "negative_price"
        if event.get("price", 0) > self.MAX_VALID_PRICE:
            return False, "extreme_price"
        if event.get("event_type") == "purchase" and event.get("price", 0) <= 0:
            return False, "purchase_zero_price"
        
        return True, None
    
    def test_valid_view_event(self):
        # Valid view event should pass validation.
        event = {
            "event_id": "abc-123",
            "user_id": 1,
            "product_id": 100,
            "event_type": "view",
            "price": 0.0
        }
        is_valid, error = self.validate_event(event)
        assert is_valid is True
        assert error is None
    
    def test_valid_purchase_event(self):
        # Valid purchase event should pass validation.
        event = {
            "event_id": "abc-123",
            "user_id": 1,
            "product_id": 100,
            "event_type": "purchase",
            "price": 99.99
        }
        is_valid, error = self.validate_event(event)
        assert is_valid is True
    
    def test_null_user_id_fails(self):
        # Event with null user_id should fail validation.
        event = {
            "event_id": "abc-123",
            "user_id": None,
            "product_id": 100,
            "event_type": "view",
            "price": 0.0
        }
        is_valid, error = self.validate_event(event)
        assert is_valid is False
        assert error == "null_user_id"
    
    def test_null_event_id_fails(self):
        # Event with null event_id should fail validation.
        event = {
            "event_id": None,
            "user_id": 1,
            "product_id": 100,
            "event_type": "view",
            "price": 0.0
        }
        is_valid, error = self.validate_event(event)
        assert is_valid is False
        assert error == "null_event_id"
    
    def test_invalid_event_type_fails(self):
        # Event with invalid event_type should fail validation.
        event = {
            "event_id": "abc-123",
            "user_id": 1,
            "product_id": 100,
            "event_type": "INVALID_TYPE",
            "price": 0.0
        }
        is_valid, error = self.validate_event(event)
        assert is_valid is False
        assert error == "invalid_event_type"
    
    def test_negative_price_fails(self):
        # Event with negative price should fail validation.
        event = {
            "event_id": "abc-123",
            "user_id": 1,
            "product_id": 100,
            "event_type": "purchase",
            "price": -10.0
        }
        is_valid, error = self.validate_event(event)
        assert is_valid is False
        assert error == "negative_price"
    
    def test_extreme_price_fails(self):
        # Event with extremely high price should fail validation.
        event = {
            "event_id": "abc-123",
            "user_id": 1,
            "product_id": 100,
            "event_type": "purchase",
            "price": 99999.99
        }
        is_valid, error = self.validate_event(event)
        assert is_valid is False
        assert error == "extreme_price"
    
    def test_purchase_with_zero_price_fails(self):
        # Purchase event with zero price should fail validation.
        event = {
            "event_id": "abc-123",
            "user_id": 1,
            "product_id": 100,
            "event_type": "purchase",
            "price": 0.0
        }
        is_valid, error = self.validate_event(event)
        assert is_valid is False
        assert error == "purchase_zero_price"
    
    def test_all_valid_event_types(self):
        # All valid event types should pass validation.
        for event_type in self.VALID_EVENT_TYPES:
            event = {
                "event_id": "abc-123",
                "user_id": 1,
                "product_id": 100,
                "event_type": event_type,
                "price": 10.0 if event_type in ["purchase", "add_to_cart"] else 0.0
            }
            is_valid, error = self.validate_event(event)
            assert is_valid is True, f"Event type {event_type} should be valid"


class TestTransformationLogic:
    # Tests for data transformation logic.
    
    def test_total_amount_calculation_purchase(self):
        # Total amount should be price * quantity for purchases.
        price = 25.50
        quantity = 3
        expected = price * quantity
        assert expected == 76.50
    
    def test_total_amount_calculation_view(self):
        # Total amount should be 0 for view events.
        event_type = "view"
        price = 0.0
        quantity = 0
        
        if event_type in ["purchase", "add_to_cart"]:
            total = price * quantity
        else:
            total = 0.0
        
        assert total == 0.0
    
    def test_late_arrival_detection(self):
        # Events older than 5 minutes should be flagged as late.
        now = datetime.now(timezone.utc)
        
        # 3 minutes ago - not late
        recent_time = now - timedelta(minutes=3)
        is_late = recent_time < (now - timedelta(minutes=5))
        assert is_late is False
        
        # 10 minutes ago - late
        old_time = now - timedelta(minutes=10)
        is_late = old_time < (now - timedelta(minutes=5))
        assert is_late is True
    
    def test_time_extraction(self):
        # Time-based features should be correctly extracted.
        test_time = datetime(2026, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
        
        assert test_time.year == 2026
        assert test_time.month == 1
        assert test_time.day == 15
        assert test_time.hour == 14
        assert test_time.isoweekday() == 4  # Thursday
    
    def test_text_normalization(self):
        # Text fields should be lowercased and trimmed.
        test_cases = [
            ("  VIEW  ", "view"),
            ("PURCHASE", "purchase"),
            ("  Add_To_Cart  ", "add_to_cart"),
        ]
        
        for input_val, expected in test_cases:
            result = input_val.strip().lower()
            assert result == expected
    
    def test_search_query_cleaning(self):
        # Search query should be cleaned of special characters.
        import re
        
        test_cases = [
            ("laptop!!!", "laptop"),
            ("shoes & bags", "shoes  bags"),
            ("t-shirt", "tshirt"),
        ]
        
        for input_val, expected in test_cases:
            result = re.sub(r"[^\w\s]", "", input_val.lower().strip())
            assert result == expected


class TestDeduplicationLogic:
    # Tests for deduplication behavior.
    
    def test_same_event_id_should_dedupe(self):
        # Events with same event_id should be deduplicated.
        events = [
            {"event_id": "abc-123", "user_id": 1, "event_type": "view"},
            {"event_id": "abc-123", "user_id": 1, "event_type": "view"},
            {"event_id": "def-456", "user_id": 2, "event_type": "purchase"},
        ]
        
        unique_ids = set(e["event_id"] for e in events)
        assert len(unique_ids) == 2
    
    def test_different_event_ids_should_not_dedupe(self):
        # Events with different event_ids should not be deduplicated.
        events = [
            {"event_id": "abc-123", "user_id": 1, "event_type": "view"},
            {"event_id": "abc-124", "user_id": 1, "event_type": "view"},
            {"event_id": "abc-125", "user_id": 1, "event_type": "view"},
        ]
        
        unique_ids = set(e["event_id"] for e in events)
        assert len(unique_ids) == 3


class TestDataQualityMetrics:
    # Tests for data quality metrics calculation.
    
    def test_validity_rate_calculation(self):
        # Validity rate should be correctly calculated.
        total_rows = 100
        valid_rows = 95
        
        validity_rate = valid_rows / total_rows
        assert validity_rate == 0.95
    
    def test_null_rate_calculation(self):
        # Null rate should be correctly calculated.
        total_rows = 100
        null_count = 5
        
        null_rate = null_count / total_rows
        assert null_rate == 0.05
    
    def test_null_rate_threshold_alert(self):
        # Alert should trigger when null rate exceeds threshold.
        NULL_RATE_THRESHOLD = 0.1
        
        test_cases = [
            (0.05, False),  # 5% - OK
            (0.10, False),  # 10% - borderline
            (0.15, True),   # 15% - ALERT
            (0.25, True),   # 25% - ALERT
        ]
        
        for null_rate, should_alert in test_cases:
            alert = null_rate > NULL_RATE_THRESHOLD
            assert alert == should_alert, f"Null rate {null_rate} should_alert={should_alert}"


class TestDefaultValues:
    # Tests for default value coalescing.
    
    def test_null_category_defaults_to_unknown(self):
        # Null category should default to 'unknown'.
        category = None
        result = category if category else "unknown"
        assert result == "unknown"
    
    def test_null_quantity_defaults_to_zero(self):
        # Null quantity should default to 0.
        quantity = None
        result = quantity if quantity is not None else 0
        assert result == 0
    
    def test_empty_search_query_stays_empty(self):
        # Empty search query should remain empty string.
        search_query = ""
        result = search_query if search_query else ""
        assert result == ""
    
    def test_null_user_segment_defaults_to_unknown(self):
        # Null user segment should default to 'unknown'.
        user_segment = None
        result = user_segment if user_segment else "unknown"
        assert result == "unknown"


class TestSchemaCompliance:
    # Tests for schema compliance.
    
    EXPECTED_COLUMNS = [
        "event_id", "user_id", "session_id", "event_type", "product_id",
        "category", "price", "quantity", "total_amount", "user_segment",
        "search_query", "event_time", "event_year", "event_month", "event_day",
        "event_hour", "event_dayofweek", "is_late_arrival", "source_file",
        "source_system", "processed_at"
    ]
    
    def test_output_has_all_required_columns(self):
        # Output schema should have all required columns.
        # Simulate transformed event
        transformed_event = {
            "event_id": "abc-123",
            "user_id": 1,
            "session_id": "1-123456",
            "event_type": "view",
            "product_id": 100,
            "category": "electronics",
            "price": 0.0,
            "quantity": 0,
            "total_amount": 0.0,
            "user_segment": "returning",
            "search_query": "",
            "event_time": datetime.now(timezone.utc),
            "event_year": 2026,
            "event_month": 1,
            "event_day": 15,
            "event_hour": 14,
            "event_dayofweek": 4,
            "is_late_arrival": False,
            "source_file": "/data/input/events_123.csv",
            "source_system": "web",
            "processed_at": datetime.now(timezone.utc),
        }
        
        for col in self.EXPECTED_COLUMNS:
            assert col in transformed_event, f"Missing column: {col}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
