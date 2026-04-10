import streamlit as st
import pandas as pd
import plotly.express as px

from db import run_query
from queries import (
    CROSS_RETAILER_PRODUCTS_QUERY,
    CROSS_RETAILER_COMPARISON_QUERY,
    CHEAPEST_RETAILER_TODAY_QUERY,
)

st.set_page_config(page_title="Cross Retailer Comparison", layout="wide")

st.title("Cross Retailer Comparison")
st.caption("Compare the same product across Migros and A101")

# --------------------------------------------------
# Product selector
# --------------------------------------------------
products_df = run_query(CROSS_RETAILER_PRODUCTS_QUERY)

if products_df.empty:
    st.warning("No product data found in mart_daily_prices_by_retailer.")
    st.stop()

product_list = products_df["standardized_product_name"].dropna().tolist()

selected_product = st.selectbox(
    "Select a product",
    product_list,
)

# --------------------------------------------------
# Trend comparison
# --------------------------------------------------
comparison_df = run_query(
    CROSS_RETAILER_COMPARISON_QUERY,
    params=(selected_product,),
)

st.subheader(f"Price trend: {selected_product}")

if comparison_df.empty:
    st.info("No comparison data available for this product.")
else:
    fig = px.line(
        comparison_df,
        x="date",
        y="avg_price",
        color="source_name",
        markers=True,
        title=f"Average price over time — {selected_product}",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        comparison_df.sort_values(["date", "source_name"]),
        use_container_width=True,
    )

# --------------------------------------------------
# Cheapest retailer today
# --------------------------------------------------
st.subheader("Cheapest retailer on the latest date")

cheapest_df = run_query(CHEAPEST_RETAILER_TODAY_QUERY)

if cheapest_df.empty:
    st.info("No cheapest retailer data available.")
else:
    st.dataframe(cheapest_df, use_container_width=True)
