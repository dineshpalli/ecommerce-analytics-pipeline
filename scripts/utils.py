#!/usr/bin/env python3
"""
Utility Functions for E-Commerce Analytics Pipeline

Common helper functions used across the pipeline.
"""

import hashlib
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


def generate_date_range(
    start_date: str | datetime,
    end_date: str | datetime | None = None,
    days: int | None = None,
) -> list[datetime]:
    """
    Generate a list of dates in a range.

    Args:
        start_date: Start date (string or datetime)
        end_date: End date (optional)
        days: Number of days from start (optional)

    Returns:
        List of datetime objects
    """
    if isinstance(start_date, str):
        start_date = datetime.fromisoformat(start_date)

    if end_date is not None:
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date)
    elif days is not None:
        end_date = start_date + timedelta(days=days)
    else:
        end_date = datetime.now()

    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)

    return dates


def hash_string(value: str, length: int = 8) -> str:
    """Generate a deterministic hash of a string."""
    return hashlib.md5(value.encode()).hexdigest()[:length].upper()


def safe_json_loads(value: str | dict) -> dict[str, Any]:
    """Safely load JSON, handling both strings and dicts."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return {}


def flatten_dict(
    d: dict[str, Any],
    parent_key: str = "",
    sep: str = "_",
) -> dict[str, Any]:
    """
    Flatten a nested dictionary.

    Args:
        d: Dictionary to flatten
        parent_key: Key prefix for nested items
        sep: Separator between keys

    Returns:
        Flattened dictionary

    Example:
        >>> flatten_dict({"a": {"b": 1, "c": 2}})
        {"a_b": 1, "a_c": 2}
    """
    items: list[tuple[str, Any]] = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def get_file_info(file_path: Path | str) -> dict[str, Any]:
    """Get metadata about a file."""
    path = Path(file_path)

    if not path.exists():
        return {"exists": False}

    stat = path.stat()
    return {
        "exists": True,
        "name": path.name,
        "path": str(path.absolute()),
        "size_bytes": stat.st_size,
        "size_mb": round(stat.st_size / (1024 * 1024), 2),
        "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
    }


def calculate_metrics(df: pd.DataFrame, event_col: str = "event_type") -> dict[str, Any]:
    """
    Calculate standard e-commerce metrics from event data.

    Args:
        df: DataFrame with event data
        event_col: Column containing event type

    Returns:
        Dictionary of calculated metrics
    """
    metrics = {
        "total_events": len(df),
        "unique_users": df["user_id"].nunique() if "user_id" in df.columns else 0,
        "unique_sessions": df["session_id"].nunique() if "session_id" in df.columns else 0,
    }

    if event_col in df.columns:
        event_counts = df[event_col].value_counts().to_dict()
        metrics["event_breakdown"] = event_counts

        # Conversion metrics
        views = event_counts.get("product_view", 0)
        carts = event_counts.get("add_to_cart", 0)
        purchases = event_counts.get("purchase", 0)

        metrics["view_to_cart_rate"] = carts / views if views > 0 else 0
        metrics["cart_to_purchase_rate"] = purchases / carts if carts > 0 else 0
        metrics["overall_conversion_rate"] = purchases / views if views > 0 else 0

    if "revenue" in df.columns:
        metrics["total_revenue"] = df["revenue"].sum()
        metrics["avg_order_value"] = (
            df[df["revenue"] > 0]["revenue"].mean() if (df["revenue"] > 0).any() else 0
        )

    return metrics


def format_number(value: float | int, precision: int = 2) -> str:
    """Format a number with thousands separators."""
    if isinstance(value, int) or value == int(value):
        return f"{int(value):,}"
    return f"{value:,.{precision}f}"


def format_percentage(value: float, precision: int = 1) -> str:
    """Format a decimal as a percentage string."""
    return f"{value * 100:.{precision}f}%"


def format_currency(value: float, currency: str = "USD") -> str:
    """Format a number as currency."""
    symbols = {"USD": "$", "EUR": "€", "GBP": "£"}
    symbol = symbols.get(currency, currency + " ")
    return f"{symbol}{value:,.2f}"


def create_date_dimension(
    start_date: str | datetime,
    end_date: str | datetime,
) -> pd.DataFrame:
    """
    Create a date dimension table.

    Args:
        start_date: Start of date range
        end_date: End of date range

    Returns:
        DataFrame with date dimension attributes
    """
    if isinstance(start_date, str):
        start_date = datetime.fromisoformat(start_date)
    if isinstance(end_date, str):
        end_date = datetime.fromisoformat(end_date)

    dates = pd.date_range(start=start_date, end=end_date, freq="D")

    dim_date = pd.DataFrame({"date": dates})
    dim_date["date_key"] = dim_date["date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["year"] = dim_date["date"].dt.year
    dim_date["quarter"] = dim_date["date"].dt.quarter
    dim_date["month"] = dim_date["date"].dt.month
    dim_date["month_name"] = dim_date["date"].dt.month_name()
    dim_date["week"] = dim_date["date"].dt.isocalendar().week
    dim_date["day_of_month"] = dim_date["date"].dt.day
    dim_date["day_of_week"] = dim_date["date"].dt.dayofweek
    dim_date["day_name"] = dim_date["date"].dt.day_name()
    dim_date["is_weekend"] = dim_date["day_of_week"].isin([5, 6])
    dim_date["is_month_start"] = dim_date["date"].dt.is_month_start
    dim_date["is_month_end"] = dim_date["date"].dt.is_month_end

    return dim_date


def get_env_config() -> dict[str, Any]:
    """Get configuration from environment variables."""
    return {
        "database_url": os.getenv("DATABASE_URL", ""),
        "bigquery_project": os.getenv("BIGQUERY_PROJECT", ""),
        "bigquery_dataset": os.getenv("BIGQUERY_DATASET", "analytics"),
        "gcs_bucket": os.getenv("GCS_BUCKET", ""),
        "environment": os.getenv("ENVIRONMENT", "development"),
        "log_level": os.getenv("LOG_LEVEL", "INFO"),
    }


class Timer:
    """Context manager for timing code execution."""

    def __init__(self, name: str = "Operation"):
        self.name = name
        self.start_time: datetime | None = None
        self.end_time: datetime | None = None

    def __enter__(self) -> "Timer":
        self.start_time = datetime.now()
        return self

    def __exit__(self, *args: Any) -> None:
        self.end_time = datetime.now()
        duration = (self.end_time - self.start_time).total_seconds()
        print(f"{self.name}: {duration:.2f} seconds")

    @property
    def elapsed(self) -> float:
        """Get elapsed time in seconds."""
        if self.start_time is None:
            return 0.0
        end = self.end_time or datetime.now()
        return (end - self.start_time).total_seconds()


if __name__ == "__main__":
    # Example usage
    print("Testing utility functions...\n")

    # Date dimension
    print("Date dimension (first 5 rows):")
    dim_date = create_date_dimension("2024-01-01", "2024-01-05")
    print(dim_date.to_string(index=False))

    # Formatting
    print(f"\nFormatted number: {format_number(1234567.89)}")
    print(f"Formatted percentage: {format_percentage(0.1234)}")
    print(f"Formatted currency: {format_currency(1234.56)}")

    # Flatten dict
    nested = {"user": {"name": "John", "location": {"city": "NYC", "country": "US"}}}
    print(f"\nFlattened dict: {flatten_dict(nested)}")

    # Timer
    print("\nTimer test:")
    with Timer("Sleep test"):
        import time

        time.sleep(0.1)
