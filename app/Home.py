import streamlit as st

from db import run_query

st.set_page_config(page_title="Food Price Intelligence", layout="wide")

# -----------------------------
# Queries
# -----------------------------
HOME_SUMMARY_QUERY = """
select
    count(distinct standardized_product_name) as product_count,
    count(*) as observation_count,
    round(avg(price_per_unit)::numeric, 2) as avg_price_per_unit
from fact_price_observations
where price_per_unit is not null;
"""

LATEST_RUN_QUERY = """
select
    run_id,
    status,
    finished_at,
    pipeline_health_status,
    total_checks,
    passed_checks,
    failed_checks
from mart_pipeline_health
order by run_id desc
limit 1;
"""

QUICK_INSIGHTS_QUERY = """
select
    standardized_product_name,
    category_name,
    pct_change
from mart_top_movers
where pct_change is not null
order by pct_change desc
limit 3;
"""

LATEST_DATE_QUERY = """
select max(date) as latest_date
from mart_daily_prices;
"""

# -----------------------------
# Data
# -----------------------------
summary_df = run_query(HOME_SUMMARY_QUERY)
latest_run_df = run_query(LATEST_RUN_QUERY)
quick_insights_df = run_query(QUICK_INSIGHTS_QUERY)
latest_date_df = run_query(LATEST_DATE_QUERY)

product_count = None
observation_count = None
avg_price_per_unit = None

if not summary_df.empty:
    product_count = int(summary_df.iloc[0]["product_count"])
    observation_count = int(summary_df.iloc[0]["observation_count"])
    avg_price_per_unit = summary_df.iloc[0]["avg_price_per_unit"]

latest_run = latest_run_df.iloc[0] if not latest_run_df.empty else None
latest_date = latest_date_df.iloc[0]["latest_date"] if not latest_date_df.empty else None

# -----------------------------
# Title
# -----------------------------
st.title("Food Price Intelligence Dashboard")

if latest_date is not None:
    st.caption(f"Latest analytics date: {latest_date}")

# -----------------------------
# KPI Cards
# -----------------------------
c1, c2, c3, c4 = st.columns(4)

c1.metric("Products", product_count)
c2.metric("Observations", observation_count)
c3.metric("Avg Price / Unit", avg_price_per_unit)

if latest_run is not None:
    c4.metric("Latest Run Health", latest_run["pipeline_health_status"])
else:
    c4.metric("Latest Run Health", "N/A")

# -----------------------------
# Latest run summary
# -----------------------------
if latest_run is not None:
    st.info(
        f"Latest run #{latest_run['run_id']} finished with status "
        f"**{latest_run['status']}**. "
        f"Checks passed: **{latest_run['passed_checks']} / {latest_run['total_checks']}**."
    )

# -----------------------------
# System overview
# -----------------------------
st.markdown("### System Overview")
st.markdown(
    """
A production-style retail price intelligence system built with:

- layered data architecture (`raw → staging → fact → mart`)
- automated data quality checks
- unit normalization
- anomaly detection
- GitHub Actions CI/CD
- Neon PostgreSQL
- Streamlit dashboard
"""
)

st.code("Scraper → Raw → Staging → Fact → Mart → Dashboard", language="text")

# -----------------------------
# Quick insights
# -----------------------------
st.markdown("## Quick Insights")

if quick_insights_df.empty:
    st.warning("No insight data available yet.")
else:
    for _, row in quick_insights_df.iterrows():
        product = row["standardized_product_name"]
        category = row["category_name"]
        pct_change = row["pct_change"]

        category_text = f" ({category})" if category else ""
        st.markdown(
            f"- **{product}**{category_text} increased by **{pct_change}%** in the most recent comparison window."
        )

# -----------------------------
# Two-column explanation area
# -----------------------------
left, right = st.columns(2)

with left:
    st.markdown("## What this project does")
    st.markdown(
        """
- Collects supermarket price observations
- Standardizes units and product names
- Builds analytics-ready fact and mart layers
- Detects volatile / anomalous products
- Monitors pipeline health and data quality
"""
    )

with right:
    st.markdown("## Why it matters")
    st.markdown(
        """
- Identifies sudden food price increases
- Monitors product-level pricing movements
- Detects unstable / volatile items
- Provides reliable analytics-ready data
- Supports future forecasting and alerting workflows
"""
    )

# -----------------------------
# Navigation helper
# -----------------------------
st.success("Use the left sidebar to explore Trend Analysis, Top Movers, Anomalies, and Pipeline Health.")
