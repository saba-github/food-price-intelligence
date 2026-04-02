import streamlit as st

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
