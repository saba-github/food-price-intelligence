import streamlit as st
import plotly.express as px

from db import run_query
from queries import TOP_VOLATILE_QUERY

st.set_page_config(page_title="Anomalies", layout="wide")

st.title("Price Anomalies")

anomaly_df = run_query(TOP_VOLATILE_QUERY)

st.dataframe(anomaly_df, use_container_width=True)

if not anomaly_df.empty:
    fig = px.bar(
        anomaly_df,
        x="standardized_product_name",
        y="volatility",
        color="volatility_level",
        hover_data=["avg_price_per_unit", "observation_count", "category_name"],
        title="Most Volatile Products",
    )
    st.plotly_chart(fig, use_container_width=True)
