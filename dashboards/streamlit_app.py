#!/usr/bin/env python3
"""
E-Commerce Analytics Dashboard

Interactive Streamlit dashboard for exploring e-commerce metrics.
Connects to the processed data from the ETL pipeline.

Usage:
    streamlit run dashboards/streamlit_app.py
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Page configuration
st.set_page_config(
    page_title="E-Commerce Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS
st.markdown(
    """
    <style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 20px;
        margin: 10px 0;
    }
    .big-font {
        font-size: 24px !important;
        font-weight: bold;
    }
    .highlight {
        color: #1f77b4;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=3600)
def load_data() -> dict[str, pd.DataFrame]:
    """Load processed data from parquet files."""
    data_dir = PROJECT_ROOT / "data" / "processed"

    data = {}

    # Load each table if it exists
    tables = [
        "fct_events",
        "fct_sessions",
        "fct_daily_metrics",
        "fct_product_performance",
    ]

    for table in tables:
        file_path = data_dir / f"{table}.parquet"
        if file_path.exists():
            data[table] = pd.read_parquet(file_path)
        else:
            st.warning(f"Data file not found: {file_path}")

    return data


def format_number(value: float, precision: int = 0) -> str:
    """Format number with thousands separators."""
    if precision == 0:
        return f"{int(value):,}"
    return f"{value:,.{precision}f}"


def format_currency(value: float) -> str:
    """Format as currency."""
    return f"${value:,.2f}"


def format_percentage(value: float) -> str:
    """Format as percentage."""
    return f"{value:.1%}"


def render_kpi_metrics(data: dict[str, pd.DataFrame]) -> None:
    """Render KPI metric cards."""
    st.header("📈 Key Performance Indicators")

    if "fct_daily_metrics" not in data:
        st.info("Daily metrics data not available. Run the ETL pipeline first.")
        return

    df = data["fct_daily_metrics"]

    # Get latest day and comparison
    latest = df.iloc[-1] if len(df) > 0 else None
    prev = df.iloc[-2] if len(df) > 1 else None

    if latest is None:
        st.warning("No data available")
        return

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        delta = None
        if prev is not None and prev["unique_users"] > 0:
            delta = f"{((latest['unique_users'] - prev['unique_users']) / prev['unique_users']):.1%}"
        st.metric(
            label="Daily Active Users",
            value=format_number(latest["unique_users"]),
            delta=delta,
        )

    with col2:
        delta = None
        if prev is not None and prev["unique_sessions"] > 0:
            delta = f"{((latest['unique_sessions'] - prev['unique_sessions']) / prev['unique_sessions']):.1%}"
        st.metric(
            label="Sessions",
            value=format_number(latest["unique_sessions"]),
            delta=delta,
        )

    with col3:
        st.metric(
            label="Total Revenue",
            value=format_currency(latest["total_revenue"]),
            delta=(
                f"{((latest['total_revenue'] - prev['total_revenue']) / prev['total_revenue']):.1%}"
                if prev is not None and prev["total_revenue"] > 0
                else None
            ),
        )

    with col4:
        conversion = (
            latest["purchases"] / latest["unique_sessions"]
            if latest["unique_sessions"] > 0
            else 0
        )
        st.metric(
            label="Conversion Rate",
            value=format_percentage(conversion),
        )


def render_engagement_trends(data: dict[str, pd.DataFrame]) -> None:
    """Render engagement trend charts."""
    st.header("📊 Engagement Trends")

    if "fct_daily_metrics" not in data:
        return

    df = data["fct_daily_metrics"].copy()
    df["event_date"] = pd.to_datetime(df["event_date"])

    # Date range selector
    col1, col2 = st.columns(2)
    with col1:
        date_range = st.selectbox(
            "Date Range",
            ["Last 7 Days", "Last 14 Days", "Last 30 Days", "All Time"],
        )

    # Filter by date range
    if date_range == "Last 7 Days":
        df = df[df["event_date"] >= df["event_date"].max() - timedelta(days=7)]
    elif date_range == "Last 14 Days":
        df = df[df["event_date"] >= df["event_date"].max() - timedelta(days=14)]
    elif date_range == "Last 30 Days":
        df = df[df["event_date"] >= df["event_date"].max() - timedelta(days=30)]

    # Create subplot
    fig = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=(
            "Daily Active Users",
            "Total Revenue",
            "Sessions",
            "Conversion Rate",
        ),
    )

    # DAU
    fig.add_trace(
        go.Scatter(
            x=df["event_date"],
            y=df["unique_users"],
            mode="lines+markers",
            name="DAU",
            line=dict(color="#1f77b4"),
        ),
        row=1,
        col=1,
    )

    # Revenue
    fig.add_trace(
        go.Bar(
            x=df["event_date"],
            y=df["total_revenue"],
            name="Revenue",
            marker_color="#2ca02c",
        ),
        row=1,
        col=2,
    )

    # Sessions
    fig.add_trace(
        go.Scatter(
            x=df["event_date"],
            y=df["unique_sessions"],
            mode="lines",
            name="Sessions",
            fill="tozeroy",
            line=dict(color="#ff7f0e"),
        ),
        row=2,
        col=1,
    )

    # Conversion Rate
    df["conversion_rate"] = df["purchases"] / df["unique_sessions"].replace(0, 1)
    fig.add_trace(
        go.Scatter(
            x=df["event_date"],
            y=df["conversion_rate"],
            mode="lines+markers",
            name="Conversion",
            line=dict(color="#d62728"),
        ),
        row=2,
        col=2,
    )

    fig.update_layout(height=600, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)


def render_funnel_analysis(data: dict[str, pd.DataFrame]) -> None:
    """Render conversion funnel visualization."""
    st.header("🔄 Conversion Funnel")

    if "fct_daily_metrics" not in data:
        return

    df = data["fct_daily_metrics"]

    # Aggregate funnel metrics
    funnel_data = {
        "Stage": ["Page Views", "Product Views", "Add to Cart", "Checkout", "Purchase"],
        "Count": [
            df["page_views"].sum(),
            df["product_views"].sum(),
            df["add_to_carts"].sum(),
            df.get("checkout_starts", df["purchases"]).sum(),
            df["purchases"].sum(),
        ],
    }

    funnel_df = pd.DataFrame(funnel_data)

    # Calculate conversion rates
    funnel_df["Conversion Rate"] = funnel_df["Count"] / funnel_df["Count"].iloc[0]

    col1, col2 = st.columns([2, 1])

    with col1:
        # Funnel chart
        fig = go.Figure(
            go.Funnel(
                y=funnel_df["Stage"],
                x=funnel_df["Count"],
                textposition="inside",
                textinfo="value+percent initial",
                marker=dict(
                    color=["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]
                ),
            )
        )
        fig.update_layout(title="Conversion Funnel", height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Stage Metrics")
        for i, row in funnel_df.iterrows():
            st.metric(
                label=row["Stage"],
                value=format_number(row["Count"]),
                delta=(
                    format_percentage(row["Count"] / funnel_df.iloc[i - 1]["Count"])
                    if i > 0
                    else None
                ),
            )


def render_product_performance(data: dict[str, pd.DataFrame]) -> None:
    """Render product performance analysis."""
    st.header("📦 Product Performance")

    if "fct_product_performance" not in data:
        st.info("Product performance data not available.")
        return

    df = data["fct_product_performance"]

    col1, col2 = st.columns(2)

    with col1:
        # Top products by revenue
        st.subheader("Top Products by Revenue")
        top_products = df.nlargest(10, "total_revenue")[
            ["category", "total_revenue", "purchases"]
        ]

        fig = px.bar(
            top_products,
            x="total_revenue",
            y="category",
            orientation="h",
            color="purchases",
            color_continuous_scale="Blues",
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Conversion by category
        st.subheader("Conversion Rate by Category")
        category_conv = (
            df.groupby("category")
            .agg({"view_count": "sum", "purchases": "sum"})
            .reset_index()
        )
        category_conv["conversion_rate"] = (
            category_conv["purchases"] / category_conv["view_count"].replace(0, 1)
        )

        fig = px.bar(
            category_conv.nlargest(10, "conversion_rate"),
            x="category",
            y="conversion_rate",
            color="conversion_rate",
            color_continuous_scale="Greens",
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)


def render_session_analysis(data: dict[str, pd.DataFrame]) -> None:
    """Render session behavior analysis."""
    st.header("👥 Session Analysis")

    if "fct_sessions" not in data:
        st.info("Session data not available.")
        return

    df = data["fct_sessions"]

    col1, col2 = st.columns(2)

    with col1:
        # Session duration distribution
        st.subheader("Session Duration Distribution")
        fig = px.histogram(
            df[df["session_duration_seconds"] < 3600],
            x="session_duration_seconds",
            nbins=50,
            labels={"session_duration_seconds": "Duration (seconds)"},
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Device breakdown
        st.subheader("Sessions by Device")
        device_counts = df["devices"].value_counts()
        fig = px.pie(
            values=device_counts.values,
            names=device_counts.index,
            hole=0.4,
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    # Session quality breakdown
    st.subheader("Session Quality Distribution")
    if "session_quality" in df.columns:
        quality_counts = df["session_quality"].value_counts()
        fig = px.bar(
            x=quality_counts.index,
            y=quality_counts.values,
            labels={"x": "Quality Tier", "y": "Session Count"},
            color=quality_counts.values,
            color_continuous_scale="Viridis",
        )
        st.plotly_chart(fig, use_container_width=True)


def render_traffic_analysis(data: dict[str, pd.DataFrame]) -> None:
    """Render traffic source analysis."""
    st.header("🚦 Traffic Analysis")

    if "fct_events" not in data:
        st.info("Event data not available.")
        return

    df = data["fct_events"]

    col1, col2 = st.columns(2)

    with col1:
        # Traffic source breakdown
        st.subheader("Events by Traffic Source")
        source_counts = df["traffic_source"].value_counts()
        fig = px.pie(
            values=source_counts.values,
            names=source_counts.index,
            hole=0.3,
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Revenue by traffic source
        st.subheader("Revenue by Traffic Source")
        revenue_by_source = df.groupby("traffic_source")["revenue"].sum().reset_index()
        fig = px.bar(
            revenue_by_source,
            x="traffic_source",
            y="revenue",
            color="revenue",
            color_continuous_scale="Oranges",
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)


def main():
    """Main dashboard application."""
    # Sidebar
    st.sidebar.title("🛒 E-Commerce Analytics")
    st.sidebar.markdown("---")

    # Navigation
    page = st.sidebar.radio(
        "Navigation",
        [
            "📈 Overview",
            "📊 Engagement Trends",
            "🔄 Funnel Analysis",
            "📦 Product Performance",
            "👥 Session Analysis",
            "🚦 Traffic Analysis",
        ],
    )

    # Load data
    try:
        data = load_data()
    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.info(
            """
            To populate the dashboard:
            1. Run `python scripts/generate_events.py` to generate sample data
            2. Run `python scripts/etl_pipeline.py` to process the data
            3. Refresh this page
            """
        )
        return

    if not data:
        st.warning("No data available. Please run the ETL pipeline first.")
        return

    # Render selected page
    if page == "📈 Overview":
        render_kpi_metrics(data)
        st.markdown("---")
        render_engagement_trends(data)

    elif page == "📊 Engagement Trends":
        render_engagement_trends(data)

    elif page == "🔄 Funnel Analysis":
        render_funnel_analysis(data)

    elif page == "📦 Product Performance":
        render_product_performance(data)

    elif page == "👥 Session Analysis":
        render_session_analysis(data)

    elif page == "🚦 Traffic Analysis":
        render_traffic_analysis(data)

    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown(
        """
        **About**

        E-Commerce Analytics Dashboard
        Built with Streamlit & Plotly

        Data refreshed: Real-time

        [View Source Code](https://github.com/dineshpalli/ecommerce-analytics-pipeline)
        """
    )


if __name__ == "__main__":
    main()
