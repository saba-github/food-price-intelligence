import streamlit as st
import plotly.express as px

from db import run_query
from queries import PIPELINE_HEALTH_QUERY, QUALITY_RESULTS_QUERY

st.set_page_config(page_title="Pipeline Health", layout="wide")

st.title("Pipeline Health")
st.caption("Operational monitoring for ETL runs, data quality checks, and pipeline health status.")

health_df = run_query(PIPELINE_HEALTH_QUERY)
quality_df = run_query(QUALITY_RESULTS_QUERY)

if health_df.empty:
    st.warning("No pipeline health data available yet.")
    st.stop()

latest = health_df.iloc[0]

# -----------------------------
# Top summary cards
# -----------------------------
healthy_count = int((health_df["pipeline_health_status"] == "healthy").sum())
running_count = int((health_df["pipeline_health_status"] == "running").sum())
failed_count = int((health_df["pipeline_health_status"] == "failed").sum())
warning_count = int((health_df["pipeline_health_status"] == "warning").sum())

c1, c2, c3, c4 = st.columns(4)
c1.metric("Latest Run ID", latest["run_id"])
c2.metric("Latest Status", latest["status"])
c3.metric("Records Fact", latest["records_fact"] if "records_fact" in latest else None)
c4.metric("Run Duration (s)", round(float(latest["run_duration_seconds"]), 2) if latest["run_duration_seconds"] is not None else None)

c5, c6, c7, c8 = st.columns(4)
c5.metric("Healthy Runs", healthy_count)
c6.metric("Running Runs", running_count)
c7.metric("Warning Runs", warning_count)
c8.metric("Failed Runs", failed_count)

# -----------------------------
# Latest run details
# -----------------------------
st.subheader("Latest Run Details")

d1, d2 = st.columns([2, 1])

with d1:
    st.dataframe(
        health_df.head(1),
        use_container_width=True,
        hide_index=True,
    )

with d2:
    latest_failed_check = latest.get("last_failed_check_name")
    latest_failed_details = latest.get("last_failed_check_details")

    if latest_failed_check:
        st.error(f"Last Failed Check: {latest_failed_check}")
        st.write(latest_failed_details if latest_failed_details else "No details available.")
    else:
        st.success("No failed checks for the latest run.")

# -----------------------------
# Health distribution
# -----------------------------
st.subheader("Pipeline Health Status Distribution")

status_counts = health_df["pipeline_health_status"].value_counts().reset_index()
status_counts.columns = ["pipeline_health_status", "count"]

fig = px.pie(
    status_counts,
    names="pipeline_health_status",
    values="count",
    title="Pipeline Health Status Distribution",
)
st.plotly_chart(fig, use_container_width=True)

# -----------------------------
# Run duration trend
# -----------------------------
duration_df = health_df.dropna(subset=["run_duration_seconds"]).copy()

if not duration_df.empty:
    st.subheader("Run Duration Trend")

    duration_fig = px.line(
        duration_df.sort_values("run_id"),
        x="run_id",
        y="run_duration_seconds",
        markers=True,
        title="Run Duration by Run ID",
    )
    st.plotly_chart(duration_fig, use_container_width=True)

# -----------------------------
# Recent runs table
# -----------------------------
st.subheader("Recent Pipeline Runs")
st.dataframe(health_df, use_container_width=True, hide_index=True)

# -----------------------------
# Recent quality results
# -----------------------------
st.subheader("Recent Data Quality Results")
st.dataframe(quality_df, use_container_width=True, hide_index=True)
