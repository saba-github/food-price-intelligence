import streamlit as st
from db import run_query
from queries import GLOBAL_FRESHNESS_QUERY

st.set_page_config(page_title="Food Price Intelligence", layout="wide")

st.title("Food Price Intelligence")
st.caption("Choose a page from the sidebar.")

freshness_df = run_query(GLOBAL_FRESHNESS_QUERY)
if not freshness_df.empty:
    row = freshness_df.iloc[0]
    latest_data_date = row.get("latest_data_date")
    latest_success_started_at = row.get("latest_success_started_at")
    if latest_data_date is not None:
        st.caption(f"Data freshness: {latest_data_date}")
    if latest_success_started_at is not None:
        st.caption(f"Last successful pipeline run: {latest_success_started_at}")
