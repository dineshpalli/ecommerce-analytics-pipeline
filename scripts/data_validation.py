#!/usr/bin/env python3
"""
Data Validation Module

Provides Pydantic models for schema validation and data quality checks.
Uses strict typing and validation rules to ensure data integrity.

Usage:
    from data_validation import EventSchema, validate_events
    validated_df = validate_events(raw_df)
"""

import json
from datetime import datetime
from enum import Enum
from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator


class EventTypeEnum(str, Enum):
    """Valid event types in the e-commerce platform."""

    PAGE_VIEW = "page_view"
    PRODUCT_VIEW = "product_view"
    ADD_TO_CART = "add_to_cart"
    REMOVE_FROM_CART = "remove_from_cart"
    BEGIN_CHECKOUT = "begin_checkout"
    PURCHASE = "purchase"
    SEARCH = "search"
    SIGNUP = "signup"
    LOGIN = "login"


class DeviceType(str, Enum):
    """Valid device types."""

    MOBILE = "mobile"
    DESKTOP = "desktop"
    TABLET = "tablet"


class TrafficSource(str, Enum):
    """Valid traffic sources."""

    ORGANIC = "organic"
    PAID_SEARCH = "paid_search"
    SOCIAL = "social"
    EMAIL = "email"
    DIRECT = "direct"
    REFERRAL = "referral"


class EventSchema(BaseModel):
    """
    Schema definition for e-commerce events.

    Validates:
    - Required fields are present
    - Field types are correct
    - Values are within acceptable ranges
    - IDs follow expected patterns
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    event_id: str = Field(..., min_length=20, max_length=24, pattern=r"^EVT_[A-F0-9]{16}$")
    event_type: EventTypeEnum
    user_id: str = Field(..., min_length=17, max_length=17, pattern=r"^USER_[A-F0-9]{12}$")
    session_id: str = Field(..., min_length=20, max_length=24, pattern=r"^SES_[A-F0-9]{16}$")
    timestamp: datetime
    properties: dict[str, Any] | str
    device: DeviceType | None = None
    country: str | None = Field(None, min_length=2, max_length=2)
    traffic_source: TrafficSource | None = None
    product_id: str | None = Field(None, pattern=r"^PROD_[A-F0-9]{8}$")
    category: str | None = None
    revenue: float = Field(default=0.0, ge=0.0, le=100000.0)

    @field_validator("properties", mode="before")
    @classmethod
    def parse_properties(cls, v: Any) -> dict[str, Any]:
        """Parse JSON string to dict if needed."""
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in properties: {e}")
        return v

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: Any) -> datetime:
        """Parse timestamp string to datetime if needed."""
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v


class ProductSchema(BaseModel):
    """Schema for product catalog entries."""

    model_config = ConfigDict(str_strip_whitespace=True)

    product_id: str = Field(..., pattern=r"^PROD_[A-F0-9]{8}$")
    product_name: str = Field(..., min_length=1, max_length=200)
    category: str = Field(..., min_length=1, max_length=100)
    subcategory: str | None = None
    price: float = Field(..., gt=0, le=100000)
    brand: str | None = None
    rating: float | None = Field(None, ge=0.0, le=5.0)
    review_count: int | None = Field(None, ge=0)
    in_stock: bool = True


class UserSchema(BaseModel):
    """Schema for user profiles."""

    model_config = ConfigDict(str_strip_whitespace=True)

    user_id: str = Field(..., pattern=r"^USER_[A-F0-9]{12}$")
    segment: Literal["power_buyer", "browser", "occasional", "new_user"]
    primary_device: DeviceType
    traffic_source: TrafficSource
    country: str = Field(..., min_length=2, max_length=2)
    city: str | None = None
    created_at: datetime
    is_subscribed: bool = False
    lifetime_value: float = Field(default=0.0, ge=0.0)


class ValidationResult(BaseModel):
    """Result of a validation run."""

    total_records: int
    valid_records: int
    invalid_records: int
    error_summary: dict[str, int]
    validation_timestamp: datetime = Field(default_factory=datetime.now)

    @property
    def validity_rate(self) -> float:
        """Calculate the percentage of valid records."""
        if self.total_records == 0:
            return 0.0
        return self.valid_records / self.total_records * 100


def validate_events(
    df: pd.DataFrame,
    raise_on_error: bool = False,
    sample_errors: int = 5,
) -> tuple[pd.DataFrame, ValidationResult]:
    """
    Validate a DataFrame of events against the EventSchema.

    Args:
        df: Input DataFrame with event data
        raise_on_error: If True, raise exception on first error
        sample_errors: Number of error samples to collect per error type

    Returns:
        Tuple of (validated DataFrame with only valid rows, ValidationResult)

    Example:
        >>> valid_df, result = validate_events(raw_df)
        >>> print(f"Validity rate: {result.validity_rate:.1f}%")
    """
    valid_records = []
    invalid_indices = []
    error_summary: dict[str, int] = {}
    error_samples: dict[str, list] = {}

    for idx, row in df.iterrows():
        try:
            # Convert row to dict and validate
            record = row.to_dict()
            validated = EventSchema(**record)
            valid_records.append(validated.model_dump())
        except Exception as e:
            invalid_indices.append(idx)
            error_type = type(e).__name__
            error_summary[error_type] = error_summary.get(error_type, 0) + 1

            # Collect sample errors for debugging
            if error_type not in error_samples:
                error_samples[error_type] = []
            if len(error_samples[error_type]) < sample_errors:
                error_samples[error_type].append(
                    {"index": idx, "error": str(e), "data": str(row.to_dict())[:200]}
                )

            if raise_on_error:
                raise

    # Create result
    result = ValidationResult(
        total_records=len(df),
        valid_records=len(valid_records),
        invalid_records=len(invalid_indices),
        error_summary=error_summary,
    )

    # Create validated DataFrame
    valid_df = pd.DataFrame(valid_records) if valid_records else pd.DataFrame()

    # Log summary
    print(f"\nValidation Summary:")
    print(f"  Total records:   {result.total_records:,}")
    print(f"  Valid records:   {result.valid_records:,}")
    print(f"  Invalid records: {result.invalid_records:,}")
    print(f"  Validity rate:   {result.validity_rate:.2f}%")

    if error_summary:
        print(f"\nError Summary:")
        for error_type, count in sorted(error_summary.items(), key=lambda x: -x[1]):
            print(f"  {error_type}: {count:,}")

        print(f"\nSample Errors:")
        for error_type, samples in error_samples.items():
            print(f"\n  {error_type}:")
            for sample in samples[:2]:
                print(f"    Index {sample['index']}: {sample['error'][:100]}")

    return valid_df, result


def validate_products(df: pd.DataFrame) -> tuple[pd.DataFrame, ValidationResult]:
    """Validate product catalog DataFrame."""
    valid_records = []
    error_summary: dict[str, int] = {}

    for _, row in df.iterrows():
        try:
            validated = ProductSchema(**row.to_dict())
            valid_records.append(validated.model_dump())
        except Exception as e:
            error_type = type(e).__name__
            error_summary[error_type] = error_summary.get(error_type, 0) + 1

    result = ValidationResult(
        total_records=len(df),
        valid_records=len(valid_records),
        invalid_records=len(df) - len(valid_records),
        error_summary=error_summary,
    )

    return pd.DataFrame(valid_records) if valid_records else pd.DataFrame(), result


def validate_users(df: pd.DataFrame) -> tuple[pd.DataFrame, ValidationResult]:
    """Validate user profiles DataFrame."""
    valid_records = []
    error_summary: dict[str, int] = {}

    for _, row in df.iterrows():
        try:
            validated = UserSchema(**row.to_dict())
            valid_records.append(validated.model_dump())
        except Exception as e:
            error_type = type(e).__name__
            error_summary[error_type] = error_summary.get(error_type, 0) + 1

    result = ValidationResult(
        total_records=len(df),
        valid_records=len(valid_records),
        invalid_records=len(df) - len(valid_records),
        error_summary=error_summary,
    )

    return pd.DataFrame(valid_records) if valid_records else pd.DataFrame(), result


class DataQualityChecker:
    """
    Advanced data quality checks beyond schema validation.

    Checks for:
    - Duplicate records
    - Missing values
    - Outliers
    - Referential integrity
    - Temporal consistency
    """

    def __init__(self, events_df: pd.DataFrame):
        self.df = events_df
        self.issues: list[dict[str, Any]] = []

    def check_duplicates(self) -> "DataQualityChecker":
        """Check for duplicate event IDs."""
        duplicates = self.df[self.df.duplicated(subset=["event_id"], keep=False)]
        if len(duplicates) > 0:
            self.issues.append(
                {
                    "check": "duplicates",
                    "severity": "high",
                    "count": len(duplicates),
                    "message": f"Found {len(duplicates)} duplicate event IDs",
                }
            )
        return self

    def check_null_rates(self, threshold: float = 0.1) -> "DataQualityChecker":
        """Check null rates for each column."""
        for col in self.df.columns:
            null_rate = self.df[col].isnull().sum() / len(self.df)
            if null_rate > threshold:
                self.issues.append(
                    {
                        "check": "null_rate",
                        "severity": "medium",
                        "column": col,
                        "null_rate": null_rate,
                        "message": f"Column '{col}' has {null_rate:.1%} null values",
                    }
                )
        return self

    def check_revenue_outliers(self, z_threshold: float = 3.0) -> "DataQualityChecker":
        """Check for revenue outliers using z-score."""
        revenue = self.df[self.df["revenue"] > 0]["revenue"]
        if len(revenue) > 0:
            mean = revenue.mean()
            std = revenue.std()
            if std > 0:
                z_scores = (revenue - mean) / std
                outliers = revenue[abs(z_scores) > z_threshold]
                if len(outliers) > 0:
                    self.issues.append(
                        {
                            "check": "revenue_outliers",
                            "severity": "low",
                            "count": len(outliers),
                            "max_value": outliers.max(),
                            "message": f"Found {len(outliers)} revenue outliers",
                        }
                    )
        return self

    def check_temporal_order(self) -> "DataQualityChecker":
        """Check for events with timestamps in the future."""
        now = datetime.now()
        future_events = self.df[pd.to_datetime(self.df["timestamp"]) > now]
        if len(future_events) > 0:
            self.issues.append(
                {
                    "check": "future_timestamps",
                    "severity": "high",
                    "count": len(future_events),
                    "message": f"Found {len(future_events)} events with future timestamps",
                }
            )
        return self

    def check_session_integrity(self) -> "DataQualityChecker":
        """Check that sessions have reasonable event counts."""
        session_counts = self.df.groupby("session_id").size()
        suspicious_sessions = session_counts[session_counts > 1000]
        if len(suspicious_sessions) > 0:
            self.issues.append(
                {
                    "check": "session_integrity",
                    "severity": "medium",
                    "count": len(suspicious_sessions),
                    "message": f"Found {len(suspicious_sessions)} sessions with >1000 events",
                }
            )
        return self

    def run_all_checks(self) -> list[dict[str, Any]]:
        """Run all data quality checks."""
        self.check_duplicates()
        self.check_null_rates()
        self.check_revenue_outliers()
        self.check_temporal_order()
        self.check_session_integrity()
        return self.issues

    def get_report(self) -> str:
        """Generate a human-readable quality report."""
        report_lines = [
            "=" * 60,
            "DATA QUALITY REPORT",
            "=" * 60,
            f"Total records: {len(self.df):,}",
            f"Issues found: {len(self.issues)}",
            "",
        ]

        if not self.issues:
            report_lines.append("No issues detected. Data quality is good!")
        else:
            # Group by severity
            by_severity = {"high": [], "medium": [], "low": []}
            for issue in self.issues:
                by_severity[issue["severity"]].append(issue)

            for severity in ["high", "medium", "low"]:
                issues = by_severity[severity]
                if issues:
                    report_lines.append(f"\n{severity.upper()} SEVERITY ISSUES:")
                    for issue in issues:
                        report_lines.append(f"  - [{issue['check']}] {issue['message']}")

        report_lines.append("\n" + "=" * 60)
        return "\n".join(report_lines)


if __name__ == "__main__":
    # Example usage
    import sys
    from pathlib import Path

    # Load sample data
    data_dir = Path(__file__).parent.parent / "data" / "raw"
    parquet_files = list(data_dir.glob("events_*.parquet"))

    if not parquet_files:
        print("No event files found. Run generate_events.py first.")
        sys.exit(1)

    # Load most recent file
    events_file = sorted(parquet_files)[-1]
    print(f"Loading: {events_file}")

    df = pd.read_parquet(events_file)

    # Validate schema
    print("\n" + "=" * 60)
    print("SCHEMA VALIDATION")
    print("=" * 60)
    valid_df, result = validate_events(df)

    # Run quality checks
    print("\n")
    checker = DataQualityChecker(df)
    checker.run_all_checks()
    print(checker.get_report())
