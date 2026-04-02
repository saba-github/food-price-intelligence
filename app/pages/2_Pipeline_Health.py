
import streamlit as st
import plotly.express as px

from app.db import run_query
from app.queries import PIPELINE_RUNS_QUERY, QUALITY_RESULTS_QUERY

st.set_page_config(page_title="Pipeline Health", layout="wide")

st.title("Pipeline Health")

runs_df = run_query(PIPELINE_RUNS_QUERY)
quality_df = run_query(QUALITY_RESULTS_QUERY)

c1, c2, c3 = st.columns(3)

with c1:
    latest_status = runs_df.iloc[0]["status"] if not runs_df.empty else "-"
    st.metric("Latest Run Status", latest_status)

with c2:
    latest_run_id = runs_df.iloc[0]["run_id"] if not runs_df.empty else "-"
    st.metric("Latest Run ID", latest_run_id)

with c3:
    latest_records = runs_df.iloc[0]["records_scraped"] if not runs_df.empty else "-"
    st.metric("Latest Records Scraped", latest_records)

st.subheader("Recent Pipeline Runs")
st.dataframe(runs_df, use_container_width=True)

if not runs_df.empty:
    status_counts = runs_df["status"].value_counts().reset_index()
    status_counts.columns = ["status", "count"]

    fig_status = px.pie(
        status_counts,
        names="status",
        values="count",
        title="Run Status Distribution"
    )
    st.plotly_chart(fig_status, use_container_width=True)

st.subheader("Recent Data Quality Results")
st.dataframe(quality_df, use_container_width=True)
