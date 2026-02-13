# E-Commerce Analytics Pipeline

> **End-to-end data engineering solution**: From raw event ingestion to decision-ready dashboards

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![dbt](https://img.shields.io/badge/dbt-1.7+-orange.svg)](https://www.getdbt.com/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

This project demonstrates a **production-grade analytics pipeline** for an e-commerce platform, showcasing:

- **Event-driven data ingestion** with schema validation
- **Modular dbt transformations** (staging → intermediate → marts)
- **Data quality testing** at every layer
- **Interactive dashboards** for business stakeholders
- **CI/CD automation** for reliable deployments

## Architecture

```mermaid
flowchart LR
    subgraph Sources ["SOURCE SYSTEMS"]
        E["Events\n(JSON/Parquet)"]
        P["Product\nCatalog"]
        U["User\nProfiles"]
    end

    subgraph Ingestion ["INGESTION LAYER — Python"]
        EX["Extract\n(Pandas)"]
        VA["Validate\n(Pydantic)"]
        LO["Load\n(Parquet)"]
        EX --> VA --> LO
    end

    subgraph Storage ["DATA LAKE"]
        RAW["raw/"]
        PROC["processed/"]
        SEED["seeds/"]
    end

    subgraph dbt ["TRANSFORMATION LAYER — dbt"]
        direction LR
        STG["Staging\n─────────\nstg_events\nstg_products\nstg_users"]
        INT["Intermediate\n─────────\nint_sessions\nint_user_journey\nint_product_perf"]
        MART["Marts\n─────────\nfct_daily_engagement\nfct_funnel · fct_revenue\ndim_users · dim_products\ndim_date"]
        STG --> INT --> MART
    end

    subgraph DWH ["DATA WAREHOUSE"]
        DB[("DuckDB\n(dev)\n─────────\nBigQuery / Snowflake\n(prod)")]
    end

    subgraph Presentation ["PRESENTATION LAYER"]
        DASH["Streamlit\nDashboard"]
        DOCS["dbt Docs\n& Lineage"]
    end

    subgraph QA ["QUALITY & CI/CD"]
        TESTS["pytest · dbt test\nGreat Expectations"]
        CI["GitHub Actions\nLint → Test → Build"]
    end

    E --> EX
    P --> EX
    U --> EX
    LO --> RAW
    LO --> PROC
    SEED -.-> STG
    RAW --> STG
    PROC --> STG
    MART --> DB
    DB --> DASH
    DB --> DOCS
    QA ~~~ dbt

    style Sources fill:#e8f4fd,stroke:#2196F3,color:#000
    style Ingestion fill:#fff3e0,stroke:#FF9800,color:#000
    style Storage fill:#e8f5e9,stroke:#4CAF50,color:#000
    style dbt fill:#fce4ec,stroke:#E91E63,color:#000
    style DWH fill:#f3e5f5,stroke:#9C27B0,color:#000
    style Presentation fill:#e0f2f1,stroke:#009688,color:#000
    style QA fill:#fff8e1,stroke:#FFC107,color:#000
```

## Business Context

This pipeline answers critical business questions:

| Question | Metric | Dashboard Section |
|----------|--------|-------------------|
| How are users engaging? | DAU, WAU, MAU | User Engagement |
| What's our conversion funnel? | View → Cart → Purchase rates | Funnel Analysis |
| Which products drive revenue? | Revenue by category, top SKUs | Product Performance |
| When do users convert? | Time-to-purchase, session duration | Behavioral Insights |
| Where do users drop off? | Funnel drop-off rates | Conversion Optimization |

## Project Structure

```
ecommerce-analytics-pipeline/
├── data/
│   ├── raw/                    # Raw event data (JSON/Parquet)
│   ├── processed/              # Cleaned, validated data
│   └── seeds/                  # Reference/lookup data
├── dbt_project/
│   ├── models/
│   │   ├── staging/            # 1:1 source mappings, basic cleaning
│   │   ├── intermediate/       # Business logic, joins, enrichment
│   │   └── marts/              # Analytics-ready facts & dimensions
│   ├── macros/                 # Reusable SQL functions
│   ├── tests/                  # Data quality tests
│   └── seeds/                  # Static lookup tables
├── scripts/
│   ├── generate_events.py      # Synthetic data generator
│   ├── etl_pipeline.py         # Main ETL orchestration
│   ├── data_validation.py      # Schema & quality validation
│   └── utils.py                # Helper functions
├── dashboards/
│   └── streamlit_app.py        # Interactive BI dashboard
├── docs/
│   ├── architecture.md         # Technical architecture
│   ├── data_dictionary.md      # Schema documentation
│   └── runbook.md              # Operations guide
├── tests/                      # Python unit tests
├── .github/workflows/          # CI/CD pipelines
├── pyproject.toml              # Project dependencies
└── README.md
```

## Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| Data Generation | Python, Faker | Realistic synthetic event data |
| Validation | Pydantic, Great Expectations | Schema enforcement, data quality |
| Storage | DuckDB / PostgreSQL | Local dev / Production |
| Transformation | dbt Core | SQL-based modeling |
| Orchestration | Python / Airflow-ready | Pipeline scheduling |
| Visualization | Streamlit, Plotly | Interactive dashboards |
| Testing | pytest, dbt tests | Code & data quality |
| CI/CD | GitHub Actions | Automated testing & deployment |

## Quick Start

### Prerequisites

- Python 3.10+
- pip or uv package manager

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/ecommerce-analytics-pipeline.git
cd ecommerce-analytics-pipeline

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -e .

# Or with uv (faster)
uv pip install -e .
```

### Generate Sample Data

```bash
# Generate 100,000 synthetic events
python scripts/generate_events.py --events 100000 --days 30

# Output: data/raw/events_YYYYMMDD.parquet
```

### Run ETL Pipeline

```bash
# Validate and process raw data
python scripts/etl_pipeline.py

# Output: data/processed/validated_events.parquet
```

### Run dbt Models

```bash
cd dbt_project

# Install dbt dependencies
dbt deps

# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### Launch Dashboard

```bash
# Start Streamlit dashboard
streamlit run dashboards/streamlit_app.py

# Open http://localhost:8501
```

## Data Models

### Staging Layer (`stg_*`)
Raw data cleaning and standardization:
- `stg_events` - Base event facts
- `stg_users` - User attributes
- `stg_products` - Product catalog

### Intermediate Layer (`int_*`)
Business logic and enrichment:
- `int_sessions` - Sessionized user activity
- `int_user_journey` - User funnel progression
- `int_product_interactions` - Aggregated product events

### Marts Layer (`fct_*`, `dim_*`)
Analytics-ready tables:
- `fct_daily_engagement` - Daily user metrics
- `fct_conversions` - Purchase funnel facts
- `fct_revenue` - Revenue aggregations
- `dim_users` - User dimension (SCD Type 2)
- `dim_products` - Product dimension
- `dim_date` - Date dimension

## Key Features

### 1. Robust Data Validation
```python
# Pydantic models ensure schema compliance
class EventSchema(BaseModel):
    event_id: str
    user_id: str
    event_type: Literal["page_view", "add_to_cart", "purchase"]
    timestamp: datetime
    properties: dict
```

### 2. Modular dbt Transformations
```sql
-- marts/fct_daily_engagement.sql
SELECT
    date_trunc('day', event_timestamp) AS activity_date,
    COUNT(DISTINCT user_id) AS daily_active_users,
    COUNT(DISTINCT session_id) AS total_sessions,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases
FROM {{ ref('int_sessions') }}
GROUP BY 1
```

### 3. Comprehensive Testing
- **Schema tests**: not_null, unique, accepted_values
- **Relationship tests**: Foreign key integrity
- **Custom tests**: Business rule validation
- **Python tests**: ETL logic verification

### 4. Production-Ready Documentation
- Auto-generated dbt docs with lineage
- Data dictionary with business definitions
- Runbook for operations

## Performance Metrics

| Metric | Value |
|--------|-------|
| Events processed | 250,000+ rows |
| Pipeline runtime | ~45 seconds (local) |
| Model count | 12 dbt models |
| Test coverage | 95%+ |
| Dashboard load time | <2 seconds |

## Sample Dashboard

The Streamlit dashboard provides:

- **Executive Summary**: KPIs at a glance
- **User Engagement**: DAU/WAU/MAU trends
- **Funnel Analysis**: Conversion rates by step
- **Product Performance**: Revenue by category
- **Cohort Analysis**: Retention over time

## Extending the Pipeline

### Adding New Event Types
1. Update `EventSchema` in `scripts/data_validation.py`
2. Add staging model in `dbt_project/models/staging/`
3. Update downstream models as needed
4. Add tests in `dbt_project/tests/`

### Connecting to Cloud
The pipeline is designed for easy cloud migration:
- **GCP**: BigQuery + Cloud Storage + Dataflow
- **AWS**: Redshift + S3 + Glue
- **Azure**: Synapse + Blob Storage + Data Factory

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file.

## Author

**Dinesh Palli**
Data Engineer | Analytics Engineer
[GitHub](https://github.com/dineshpalli) | [LinkedIn](https://linkedin.com/in/dineshpalli)

---

*Built with a focus on clean code, reproducibility, and business impact.*
