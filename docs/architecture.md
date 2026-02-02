# Architecture Documentation

## System Overview

This document describes the architecture of the E-Commerce Analytics Pipeline, designed to transform raw event data into actionable business insights.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        E-COMMERCE ANALYTICS PIPELINE                            │
│                                                                                 │
│  ┌─────────────┐                                                                │
│  │   SOURCE    │                                                                │
│  │   SYSTEMS   │                                                                │
│  │             │                                                                │
│  │ ┌─────────┐ │     ┌──────────────────────────────────────────────────────┐  │
│  │ │ Events  │ │     │                  INGESTION LAYER                      │  │
│  │ │  JSON   │─┼────▶│  ┌────────────┐    ┌─────────────┐    ┌───────────┐  │  │
│  │ └─────────┘ │     │  │   Extract  │───▶│  Validate   │───▶│   Load    │  │  │
│  │             │     │  │  (Python)  │    │  (Pydantic) │    │ (Parquet) │  │  │
│  │ ┌─────────┐ │     │  └────────────┘    └─────────────┘    └───────────┘  │  │
│  │ │ Product │ │     │                                                       │  │
│  │ │ Catalog │─┼────▶│  scripts/generate_events.py                           │  │
│  │ └─────────┘ │     │  scripts/etl_pipeline.py                              │  │
│  │             │     │  scripts/data_validation.py                           │  │
│  │ ┌─────────┐ │     └──────────────────────────────────────────────────────┘  │
│  │ │  Users  │ │                              │                                 │
│  │ │ Profiles│─┼────────────────────────────┐ │                                 │
│  │ └─────────┘ │                            │ │                                 │
│  └─────────────┘                            ▼ ▼                                 │
│                                    ┌────────────────┐                           │
│                                    │   DATA LAKE    │                           │
│                                    │   (Parquet)    │                           │
│                                    │                │                           │
│                                    │ data/raw/      │                           │
│                                    │ data/processed/│                           │
│                                    │ data/seeds/    │                           │
│                                    └───────┬────────┘                           │
│                                            │                                    │
│                                            ▼                                    │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                       TRANSFORMATION LAYER (dbt)                          │  │
│  │                                                                           │  │
│  │  ┌─────────────┐      ┌─────────────────┐      ┌───────────────────┐     │  │
│  │  │   STAGING   │      │  INTERMEDIATE   │      │      MARTS        │     │  │
│  │  │             │      │                 │      │                   │     │  │
│  │  │ stg_events  │─────▶│ int_sessions    │─────▶│ dim_users         │     │  │
│  │  │ stg_products│      │ int_user_journey│      │ dim_products      │     │  │
│  │  │ stg_users   │      │ int_product_perf│      │ dim_date          │     │  │
│  │  │             │      │                 │      │ fct_daily_engage  │     │  │
│  │  │ Clean &     │      │ Business Logic  │      │ fct_funnel        │     │  │
│  │  │ Standardize │      │ & Enrichment    │      │ fct_revenue       │     │  │
│  │  └─────────────┘      └─────────────────┘      └───────────────────┘     │  │
│  │                                                                           │  │
│  │  dbt_project/models/                                                      │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                            │                                    │
│                                            ▼                                    │
│                                    ┌────────────────┐                           │
│                                    │   DATA STORE   │                           │
│                                    │   (DuckDB)     │                           │
│                                    │                │                           │
│                                    │ analytics.     │                           │
│                                    │ duckdb         │                           │
│                                    └───────┬────────┘                           │
│                                            │                                    │
│                                            ▼                                    │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                       PRESENTATION LAYER                                  │  │
│  │                                                                           │  │
│  │  ┌─────────────────┐    ┌─────────────────┐    ┌───────────────────┐     │  │
│  │  │    Streamlit    │    │   dbt Docs      │    │   Data Export     │     │  │
│  │  │    Dashboard    │    │   (Lineage)     │    │   (CSV/Excel)     │     │  │
│  │  │                 │    │                 │    │                   │     │  │
│  │  │ - KPI Metrics   │    │ - Model docs    │    │ - Ad-hoc reports  │     │  │
│  │  │ - Trends        │    │ - Column docs   │    │ - Data extracts   │     │  │
│  │  │ - Funnels       │    │ - DAG view      │    │ - API endpoints   │     │  │
│  │  │ - Products      │    │ - Test results  │    │                   │     │  │
│  │  └─────────────────┘    └─────────────────┘    └───────────────────┘     │  │
│  │                                                                           │  │
│  │  dashboards/streamlit_app.py                                              │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Layer Descriptions

### 1. Source Systems

**Purpose**: Origin of raw data

| Source | Description | Format |
|--------|-------------|--------|
| Events | User behavior events (clicks, purchases) | JSON/Parquet |
| Product Catalog | Product information | Parquet |
| User Profiles | User attributes (no PII) | Parquet |

### 2. Ingestion Layer

**Purpose**: Extract, validate, and load raw data

**Components**:
- `generate_events.py`: Creates synthetic event data for testing
- `data_validation.py`: Pydantic models for schema validation
- `etl_pipeline.py`: Orchestrates the E→T→L process

**Key Features**:
- Schema enforcement via Pydantic
- Data quality checks
- Idempotent operations
- Error handling and logging

### 3. Data Lake (Storage)

**Purpose**: Persistent storage for raw and processed data

**Structure**:
```
data/
├── raw/          # Unprocessed event files
├── processed/    # Validated and transformed data
└── seeds/        # Reference data (products, users)
```

**Format**: Apache Parquet (columnar, compressed)

### 4. Transformation Layer (dbt)

**Purpose**: SQL-based data modeling and business logic

**Model Layers**:

#### Staging (`stg_*`)
- 1:1 mapping to source tables
- Basic cleaning and type casting
- Column renaming and standardization
- No business logic

#### Intermediate (`int_*`)
- Joins and aggregations
- Business logic application
- Session assembly
- Funnel progression

#### Marts (`fct_*`, `dim_*`)
- Star schema design
- Fact tables (measures)
- Dimension tables (attributes)
- Ready for BI consumption

### 5. Data Store (DuckDB)

**Purpose**: Analytical query engine

**Advantages**:
- Embedded (no server required)
- SQL interface
- Fast analytical queries
- Parquet native support

**Production Alternative**: BigQuery, Snowflake, Redshift

### 6. Presentation Layer

**Purpose**: Deliver insights to stakeholders

**Components**:
- Streamlit Dashboard: Interactive visualizations
- dbt Docs: Data documentation and lineage
- Export APIs: Programmatic data access

## Data Flow

```
1. Events Generated
   ↓
2. Schema Validation (Pydantic)
   ↓
3. Quality Checks
   ↓
4. Load to Raw Storage (Parquet)
   ↓
5. dbt Staging Models
   ↓
6. dbt Intermediate Models
   ↓
7. dbt Mart Models
   ↓
8. Dashboard/Reports
```

## Technology Decisions

### Why Parquet?
- Columnar storage: Fast analytical queries
- Compression: Reduced storage costs
- Schema evolution: Add columns without rewriting
- Ecosystem: Wide tool support

### Why dbt?
- Version-controlled SQL
- Built-in testing
- Documentation generation
- Modular design patterns
- DAG-based execution

### Why DuckDB?
- Zero setup for development
- Embeddable in applications
- Excellent performance
- Compatible with production DWH patterns

### Why Pydantic?
- Type safety
- Automatic validation
- Clear error messages
- IDE autocomplete support

## Scalability Considerations

### Current Design (Development)
- Single-node processing
- Local file storage
- DuckDB for queries

### Production Scaling
1. **Storage**: Cloud object storage (S3, GCS)
2. **Compute**: Distributed processing (Spark, Dataflow)
3. **Warehouse**: Cloud DWH (BigQuery, Snowflake)
4. **Orchestration**: Airflow, Dagster, Prefect

## Security Considerations

- No PII in stored data
- Credentials via environment variables
- Input validation on all external data
- Audit logging for data access

## Monitoring & Observability

- ETL pipeline metrics (duration, records)
- Data quality metrics (validity rate)
- Dashboard usage analytics
- dbt test results
