#!/usr/bin/env python3
"""
ETL Pipeline for E-Commerce Analytics

Orchestrates the end-to-end data pipeline:
1. Extract raw event data
2. Validate and transform
3. Load to analytics-ready format

Designed for both local development (DuckDB) and production (BigQuery/Snowflake).

Usage:
    python etl_pipeline.py
    python etl_pipeline.py --source data/raw --target data/processed
"""

import argparse
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from data_validation import DataQualityChecker, validate_events

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """Configuration for the ETL pipeline."""

    source_dir: Path
    target_dir: Path
    database_path: Path
    min_validity_rate: float = 95.0
    enable_quality_checks: bool = True
    parallel_workers: int = 4


@dataclass
class PipelineMetrics:
    """Metrics collected during pipeline execution."""

    start_time: datetime = field(default_factory=datetime.now)
    end_time: datetime | None = None
    records_extracted: int = 0
    records_validated: int = 0
    records_loaded: int = 0
    validity_rate: float = 0.0
    quality_issues: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        """Calculate pipeline duration in seconds."""
        if self.end_time is None:
            return 0.0
        return (self.end_time - self.start_time).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary for logging/storage."""
        return {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "records_extracted": self.records_extracted,
            "records_validated": self.records_validated,
            "records_loaded": self.records_loaded,
            "validity_rate": self.validity_rate,
            "quality_issues_count": len(self.quality_issues),
            "errors_count": len(self.errors),
        }


class ETLPipeline:
    """
    Main ETL pipeline class.

    Handles extraction, validation, transformation, and loading of
    e-commerce event data.
    """

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.metrics = PipelineMetrics()
        self.conn: duckdb.DuckDBPyConnection | None = None

    def _setup_database(self) -> None:
        """Initialize DuckDB database and create schema."""
        logger.info(f"Setting up database: {self.config.database_path}")

        self.conn = duckdb.connect(str(self.config.database_path))

        # Create schema
        self.conn.execute("""
            CREATE SCHEMA IF NOT EXISTS raw;
            CREATE SCHEMA IF NOT EXISTS staging;
            CREATE SCHEMA IF NOT EXISTS analytics;
        """)

        logger.info("Database schema created successfully")

    def _extract(self) -> pd.DataFrame:
        """Extract raw event data from source files."""
        logger.info(f"Extracting data from: {self.config.source_dir}")

        # Find all parquet files
        parquet_files = list(self.config.source_dir.glob("events_*.parquet"))

        if not parquet_files:
            raise FileNotFoundError(
                f"No event files found in {self.config.source_dir}"
            )

        # Load and concatenate all files
        dfs = []
        for file_path in sorted(parquet_files):
            logger.info(f"  Loading: {file_path.name}")
            df = pd.read_parquet(file_path)
            dfs.append(df)

        combined_df = pd.concat(dfs, ignore_index=True)
        self.metrics.records_extracted = len(combined_df)

        logger.info(f"Extracted {self.metrics.records_extracted:,} records")
        return combined_df

    def _validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate extracted data against schema."""
        logger.info("Validating data schema...")

        valid_df, result = validate_events(df)
        self.metrics.records_validated = result.valid_records
        self.metrics.validity_rate = result.validity_rate

        # Check minimum validity threshold
        if result.validity_rate < self.config.min_validity_rate:
            error_msg = (
                f"Validity rate {result.validity_rate:.1f}% is below "
                f"threshold {self.config.min_validity_rate:.1f}%"
            )
            logger.error(error_msg)
            self.metrics.errors.append(error_msg)
            raise ValueError(error_msg)

        logger.info(f"Validation passed: {result.validity_rate:.1f}% valid")
        return valid_df

    def _run_quality_checks(self, df: pd.DataFrame) -> None:
        """Run data quality checks."""
        if not self.config.enable_quality_checks:
            logger.info("Quality checks disabled, skipping...")
            return

        logger.info("Running data quality checks...")

        checker = DataQualityChecker(df)
        issues = checker.run_all_checks()
        self.metrics.quality_issues = issues

        # Log high severity issues
        high_severity = [i for i in issues if i["severity"] == "high"]
        if high_severity:
            logger.warning(f"Found {len(high_severity)} high severity issues:")
            for issue in high_severity:
                logger.warning(f"  - {issue['message']}")

        logger.info(f"Quality checks complete: {len(issues)} issues found")

    def _transform(self, df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        """
        Transform validated data into analytics-ready tables.

        Creates:
        - fct_events: Fact table with all events
        - fct_sessions: Session-level aggregations
        - fct_daily_metrics: Daily KPIs
        """
        logger.info("Transforming data...")

        transformed = {}

        # Ensure timestamp is datetime
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        # 1. Fact events table (cleaned and enriched)
        fct_events = df.copy()
        fct_events["event_date"] = fct_events["timestamp"].dt.date
        fct_events["event_hour"] = fct_events["timestamp"].dt.hour
        fct_events["day_of_week"] = fct_events["timestamp"].dt.dayofweek
        fct_events["is_weekend"] = fct_events["day_of_week"].isin([5, 6])
        transformed["fct_events"] = fct_events

        # 2. Session aggregations
        fct_sessions = (
            df.groupby(["session_id", "user_id"])
            .agg(
                session_start=("timestamp", "min"),
                session_end=("timestamp", "max"),
                event_count=("event_id", "count"),
                page_views=("event_type", lambda x: (x == "page_view").sum()),
                product_views=("event_type", lambda x: (x == "product_view").sum()),
                add_to_carts=("event_type", lambda x: (x == "add_to_cart").sum()),
                purchases=("event_type", lambda x: (x == "purchase").sum()),
                total_revenue=("revenue", "sum"),
                unique_products=("product_id", "nunique"),
                countries=("country", "first"),
                devices=("device", "first"),
            )
            .reset_index()
        )
        fct_sessions["session_duration_seconds"] = (
            fct_sessions["session_end"] - fct_sessions["session_start"]
        ).dt.total_seconds()
        fct_sessions["is_converted"] = fct_sessions["purchases"] > 0
        transformed["fct_sessions"] = fct_sessions

        # 3. Daily metrics
        df["event_date"] = df["timestamp"].dt.date
        fct_daily = (
            df.groupby("event_date")
            .agg(
                total_events=("event_id", "count"),
                unique_users=("user_id", "nunique"),
                unique_sessions=("session_id", "nunique"),
                page_views=("event_type", lambda x: (x == "page_view").sum()),
                product_views=("event_type", lambda x: (x == "product_view").sum()),
                add_to_carts=("event_type", lambda x: (x == "add_to_cart").sum()),
                purchases=("event_type", lambda x: (x == "purchase").sum()),
                total_revenue=("revenue", "sum"),
            )
            .reset_index()
        )
        fct_daily["conversion_rate"] = fct_daily["purchases"] / fct_daily["unique_sessions"]
        fct_daily["avg_revenue_per_user"] = fct_daily["total_revenue"] / fct_daily["unique_users"]
        transformed["fct_daily_metrics"] = fct_daily

        # 4. Product performance
        product_events = df[df["product_id"].notna()].copy()
        if len(product_events) > 0:
            fct_products = (
                product_events.groupby(["product_id", "category"])
                .agg(
                    view_count=("event_type", lambda x: (x == "product_view").sum()),
                    cart_adds=("event_type", lambda x: (x == "add_to_cart").sum()),
                    purchases=("event_type", lambda x: (x == "purchase").sum()),
                    total_revenue=("revenue", "sum"),
                    unique_viewers=("user_id", "nunique"),
                )
                .reset_index()
            )
            fct_products["cart_rate"] = (
                fct_products["cart_adds"] / fct_products["view_count"].replace(0, 1)
            )
            fct_products["purchase_rate"] = (
                fct_products["purchases"] / fct_products["view_count"].replace(0, 1)
            )
            transformed["fct_product_performance"] = fct_products

        logger.info(f"Created {len(transformed)} transformed tables")
        for name, table in transformed.items():
            logger.info(f"  - {name}: {len(table):,} rows")

        return transformed

    def _load(self, tables: dict[str, pd.DataFrame]) -> None:
        """Load transformed data to database and parquet files."""
        logger.info("Loading data...")

        # Ensure target directory exists
        self.config.target_dir.mkdir(parents=True, exist_ok=True)

        total_loaded = 0

        for table_name, df in tables.items():
            # Save to parquet
            parquet_path = self.config.target_dir / f"{table_name}.parquet"
            df.to_parquet(parquet_path, index=False)
            logger.info(f"  Saved: {parquet_path}")

            # Load to DuckDB
            if self.conn is not None:
                self.conn.execute(f"DROP TABLE IF EXISTS analytics.{table_name}")
                self.conn.execute(
                    f"CREATE TABLE analytics.{table_name} AS SELECT * FROM df"
                )
                logger.info(f"  Loaded to DuckDB: analytics.{table_name}")

            total_loaded += len(df)

        self.metrics.records_loaded = total_loaded
        logger.info(f"Loaded {total_loaded:,} total records")

    def run(self) -> PipelineMetrics:
        """Execute the full ETL pipeline."""
        logger.info("=" * 60)
        logger.info("STARTING ETL PIPELINE")
        logger.info("=" * 60)

        try:
            # Setup
            self._setup_database()

            # Extract
            raw_df = self._extract()

            # Validate
            valid_df = self._validate(raw_df)

            # Quality checks
            self._run_quality_checks(valid_df)

            # Transform
            transformed_tables = self._transform(valid_df)

            # Load
            self._load(transformed_tables)

            self.metrics.end_time = datetime.now()

            logger.info("=" * 60)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)
            logger.info(f"Duration: {self.metrics.duration_seconds:.1f} seconds")
            logger.info(f"Records processed: {self.metrics.records_loaded:,}")

        except Exception as e:
            self.metrics.end_time = datetime.now()
            self.metrics.errors.append(str(e))
            logger.error(f"Pipeline failed: {e}")
            raise

        finally:
            # Close database connection
            if self.conn is not None:
                self.conn.close()

        return self.metrics


def main():
    parser = argparse.ArgumentParser(description="Run ETL pipeline")
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help="Source directory with raw data",
    )
    parser.add_argument(
        "--target",
        type=str,
        default=None,
        help="Target directory for processed data",
    )
    parser.add_argument(
        "--database",
        type=str,
        default=None,
        help="DuckDB database path",
    )
    parser.add_argument(
        "--min-validity",
        type=float,
        default=95.0,
        help="Minimum validity rate threshold",
    )
    parser.add_argument(
        "--skip-quality-checks",
        action="store_true",
        help="Skip data quality checks",
    )

    args = parser.parse_args()

    # Determine paths
    project_dir = Path(__file__).parent.parent
    source_dir = Path(args.source) if args.source else project_dir / "data" / "raw"
    target_dir = Path(args.target) if args.target else project_dir / "data" / "processed"
    database_path = (
        Path(args.database) if args.database else project_dir / "data" / "analytics.duckdb"
    )

    # Create config
    config = PipelineConfig(
        source_dir=source_dir,
        target_dir=target_dir,
        database_path=database_path,
        min_validity_rate=args.min_validity,
        enable_quality_checks=not args.skip_quality_checks,
    )

    # Run pipeline
    pipeline = ETLPipeline(config)

    try:
        metrics = pipeline.run()
        print("\nPipeline Metrics:")
        for key, value in metrics.to_dict().items():
            print(f"  {key}: {value}")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
