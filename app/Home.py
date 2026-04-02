import streamlit as st
from db import get_connection
import pandas as pd


conn = get_connection()


kpi = pd.read_sql("""
select
    count(distinct standardized_product_name) as product_count,
    count(*) as observations,
    round(avg(price_per_unit),2) as avg_price
from fact_price_observations
""", conn)

col1, col2, col3 = st.columns(3)

col1.metric("Products", kpi["product_count"][0])
col2.metric("Observations", kpi["observations"][0])
col3.metric("Avg Price/unit", kpi["avg_price"][0])


st.set_page_config(
    page_title="Food Price Intelligence",
    page_icon="📈",
    layout="wide",
)

st.title("Food Price Intelligence Dashboard")

st.markdown(
    """
    A production-style retail price intelligence system built with:

    - layered data architecture (`raw -> staging -> fact -> mart`)
    - automated data quality checks
    - unit normalization
    - anomaly detection
    - GitHub Actions CI/CD
    - Neon PostgreSQL
    - Streamlit dashboard
    """
)

st.info("Use the left sidebar to navigate between dashboard pages.")

col1, col2 = st.columns(2)

with col1:
    st.subheader("What this project does")
    st.markdown(
        """
        - Collects supermarket price observations
        - Standardizes units and product names
        - Builds analytics-ready fact and mart layers
        - Detects volatile / anomalous products
        - Monitors pipeline health and data quality
        """
    )

with col2:
    st.subheader("Why it matters")
    st.markdown(
        """
        This project is designed not just as a scraper, but as a reusable
        data product architecture that can evolve into:

        - multi-market price comparison
        - price trend analytics
        - anomaly alerting
        - forecasting-ready datasets
        """
    )
