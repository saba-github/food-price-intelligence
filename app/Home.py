
import streamlit as st

st.set_page_config(
    page_title="Food Price Intelligence",
    page_icon="📈",
    layout="wide",
)

st.title("Food Price Intelligence Dashboard")
st.markdown(
    """
    This dashboard presents a production-style retail price intelligence system built on:

    - layered data architecture (raw → staging → fact → mart)
    - automated data quality checks
    - unit normalization
    - anomaly detection
    - CI/CD with GitHub Actions and Neon PostgreSQL
    """
)

st.info("Use the left sidebar to navigate between Price Intelligence and Pipeline Health.")
