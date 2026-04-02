import streamlit as st
import plotly.express as px

from app.db import run_query
from app.queries import PIPELINE_HEALTH_QUERY, QUALITY_RESULTS_QUERY

st.set_page_config(page_title="Pipeline Health", layout="wide")

st.title("Pipeline Health")

health_df = run_query(PIPELINE_HEALTH_QUERY)
quality_df = run_query(QUALITY_RESULTS_QUERY)

if not health_df.empty:
    latest = health_df.iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Latest Run ID", latest["run_id"])
    c2.metric("Latest Status", latest["status"])
    c3.metric("Records Scraped", latest["records_scraped"])
    c4.metric("Health", latest["pipeline_health_status"])

st.subheader("Recent Pipeline Runs")
st.dataframe(health_df, use_container_width=True)

if not health_df.empty:
    status_counts = health_df["pipeline_health_status"].value_counts().reset_index()
    status_counts.columns = ["pipeline_health_status", "count"]

    fig = px.pie(
        status_counts,
        names="pipeline_health_status",
        values="count",
        title="Pipeline Health Status Distribution",
    )
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Recent Data Quality Results")
st.dataframe(quality_df, use_container_width=True)
