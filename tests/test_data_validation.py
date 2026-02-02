#!/usr/bin/env python3
"""
Unit tests for data validation module.
"""

import json
from datetime import datetime

import pandas as pd
import pytest

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.data_validation import (
    EventSchema,
    ProductSchema,
    UserSchema,
    DataQualityChecker,
    validate_events,
)


class TestEventSchema:
    """Tests for EventSchema validation."""

    def test_valid_event(self):
        """Test that valid events pass validation."""
        event = {
            "event_id": "EVT_1234567890ABCDEF",
            "event_type": "page_view",
            "user_id": "USER_ABC123DEF456",
            "session_id": "SES_1234567890ABCDEF",
            "timestamp": datetime.now(),
            "properties": {"page": "/home"},
            "device": "mobile",
            "country": "US",
            "traffic_source": "organic",
            "revenue": 0.0,
        }
        validated = EventSchema(**event)
        assert validated.event_id == event["event_id"]
        assert validated.event_type.value == event["event_type"]

    def test_invalid_event_type(self):
        """Test that invalid event types fail validation."""
        event = {
            "event_id": "EVT_1234567890ABCDEF",
            "event_type": "invalid_type",  # Invalid
            "user_id": "USER_ABC123DEF456",
            "session_id": "SES_1234567890ABCDEF",
            "timestamp": datetime.now(),
            "properties": {},
        }
        with pytest.raises(Exception):
            EventSchema(**event)

    def test_missing_required_field(self):
        """Test that missing required fields fail validation."""
        event = {
            "event_type": "page_view",
            # Missing event_id, user_id, session_id, timestamp
        }
        with pytest.raises(Exception):
            EventSchema(**event)

    def test_json_properties_parsing(self):
        """Test that JSON string properties are parsed correctly."""
        properties = {"key": "value", "nested": {"inner": 123}}
        event = {
            "event_id": "EVT_1234567890ABCDEF",
            "event_type": "page_view",
            "user_id": "USER_ABC123DEF456",
            "session_id": "SES_1234567890ABCDEF",
            "timestamp": datetime.now(),
            "properties": json.dumps(properties),  # JSON string
        }
        validated = EventSchema(**event)
        assert validated.properties == properties

    def test_negative_revenue_fails(self):
        """Test that negative revenue fails validation."""
        event = {
            "event_id": "EVT_1234567890ABCDEF",
            "event_type": "purchase",
            "user_id": "USER_ABC123DEF456",
            "session_id": "SES_1234567890ABCDEF",
            "timestamp": datetime.now(),
            "properties": {},
            "revenue": -100.0,  # Invalid
        }
        with pytest.raises(Exception):
            EventSchema(**event)


class TestProductSchema:
    """Tests for ProductSchema validation."""

    def test_valid_product(self):
        """Test that valid products pass validation."""
        product = {
            "product_id": "PROD_12345678",
            "product_name": "Test Product",
            "category": "Electronics",
            "price": 99.99,
        }
        validated = ProductSchema(**product)
        assert validated.product_id == product["product_id"]

    def test_zero_price_fails(self):
        """Test that zero price fails validation."""
        product = {
            "product_id": "PROD_12345678",
            "product_name": "Free Product",
            "category": "Electronics",
            "price": 0.0,  # Invalid
        }
        with pytest.raises(Exception):
            ProductSchema(**product)

    def test_rating_out_of_range(self):
        """Test that rating > 5 fails validation."""
        product = {
            "product_id": "PROD_12345678",
            "product_name": "Test Product",
            "category": "Electronics",
            "price": 99.99,
            "rating": 6.0,  # Invalid
        }
        with pytest.raises(Exception):
            ProductSchema(**product)


class TestUserSchema:
    """Tests for UserSchema validation."""

    def test_valid_user(self):
        """Test that valid users pass validation."""
        user = {
            "user_id": "USER_ABC123DEF456",
            "segment": "power_buyer",
            "primary_device": "mobile",
            "traffic_source": "organic",
            "country": "US",
            "created_at": datetime.now(),
        }
        validated = UserSchema(**user)
        assert validated.user_id == user["user_id"]

    def test_invalid_segment(self):
        """Test that invalid segment fails validation."""
        user = {
            "user_id": "USER_ABC123DEF456",
            "segment": "invalid_segment",  # Invalid
            "primary_device": "mobile",
            "traffic_source": "organic",
            "country": "US",
            "created_at": datetime.now(),
        }
        with pytest.raises(Exception):
            UserSchema(**user)


class TestDataQualityChecker:
    """Tests for DataQualityChecker."""

    @pytest.fixture
    def sample_df(self):
        """Create a sample DataFrame for testing."""
        return pd.DataFrame({
            "event_id": ["EVT_1", "EVT_2", "EVT_3"],
            "user_id": ["USER_1", "USER_2", "USER_3"],
            "session_id": ["SES_1", "SES_1", "SES_2"],
            "timestamp": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-02"]),
            "revenue": [0.0, 100.0, 50.0],
        })

    def test_no_duplicates(self, sample_df):
        """Test duplicate detection with no duplicates."""
        checker = DataQualityChecker(sample_df)
        checker.check_duplicates()
        duplicate_issues = [i for i in checker.issues if i["check"] == "duplicates"]
        assert len(duplicate_issues) == 0

    def test_with_duplicates(self, sample_df):
        """Test duplicate detection with duplicates."""
        sample_df.loc[3] = sample_df.loc[0]  # Add duplicate
        checker = DataQualityChecker(sample_df)
        checker.check_duplicates()
        duplicate_issues = [i for i in checker.issues if i["check"] == "duplicates"]
        assert len(duplicate_issues) == 1

    def test_null_rate_check(self, sample_df):
        """Test null rate detection."""
        sample_df["nullable_col"] = [None, None, "value"]  # 66% null
        checker = DataQualityChecker(sample_df)
        checker.check_null_rates(threshold=0.5)
        null_issues = [i for i in checker.issues if i["check"] == "null_rate"]
        assert len(null_issues) == 1

    def test_run_all_checks(self, sample_df):
        """Test running all checks."""
        checker = DataQualityChecker(sample_df)
        issues = checker.run_all_checks()
        assert isinstance(issues, list)

    def test_get_report(self, sample_df):
        """Test report generation."""
        checker = DataQualityChecker(sample_df)
        checker.run_all_checks()
        report = checker.get_report()
        assert "DATA QUALITY REPORT" in report
        assert "Total records:" in report


class TestValidateEvents:
    """Tests for validate_events function."""

    def test_empty_dataframe(self):
        """Test validation of empty DataFrame."""
        df = pd.DataFrame()
        valid_df, result = validate_events(df)
        assert len(valid_df) == 0
        assert result.total_records == 0

    def test_mixed_valid_invalid(self):
        """Test DataFrame with mix of valid and invalid records."""
        df = pd.DataFrame({
            "event_id": ["EVT_1234567890ABCDEF", "invalid_id"],
            "event_type": ["page_view", "page_view"],
            "user_id": ["USER_ABC123DEF456", "USER_ABC123DEF456"],
            "session_id": ["SES_1234567890ABCDEF", "SES_1234567890ABCDEF"],
            "timestamp": [datetime.now(), datetime.now()],
            "properties": ["{}", "{}"],
            "device": ["mobile", "mobile"],
            "country": ["US", "US"],
            "traffic_source": ["organic", "organic"],
            "product_id": [None, None],
            "category": [None, None],
            "revenue": [0.0, 0.0],
        })

        valid_df, result = validate_events(df)
        assert result.total_records == 2
        # At least one should be valid (the first one)
        assert result.valid_records >= 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
