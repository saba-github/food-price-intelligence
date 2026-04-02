import streamlit as st
from db import run_query

st.set_page_config(
    page_title="Food Price Intelligence",
    page_icon="📈",
    layout="wide",
)

kpi = run_query("""
select
    count(distinct standardized_product_name) as product_count,
    count(*) as observations,
    round(avg(price_per_unit), 2) as avg_price
from fact_price_observations
where price_per_unit is not null
""")

st.title("Food Price Intelligence Dashboard")

col1, col2, col3 = st.columns(3)
col1.metric("Products", int(kpi["product_count"][0]))
col2.metric("Observations", int(kpi["observations"][0]))
col3.metric("Avg Price / Unit", float(kpi["avg_price"][0]) if kpi["avg_price"][0] is not None else 0.0)

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

col4, col5 = st.columns(2)

with col4:
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

with col5:
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
