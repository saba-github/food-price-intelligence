import streamlit as st
import pandas as pd
import plotly.express as px

from db import run_query
from queries import (
    CROSS_RETAILER_PRODUCTS_QUERY,
    CROSS_RETAILER_COMPARISON_QUERY,
    CHEAPEST_RETAILER_TODAY_QUERY,
    RETAILER_FRESHNESS_QUERY,
)

st.set_page_config(page_title="Cross Retailer Comparison", layout="wide")

st.title("Cross Retailer Comparison")
st.caption("Compare the same product across Migros and A101")


def format_date_label(value):
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return "-"
    return parsed.strftime("%b %d, %Y").replace(" 0", " ")


# --------------------------------------------------
# Retailer freshness
# --------------------------------------------------
freshness_df = run_query(RETAILER_FRESHNESS_QUERY)

st.subheader("Retailer Freshness")

if freshness_df.empty:
    st.warning("No retailer freshness data found in mart_daily_prices_by_retailer.")
    st.stop()

freshness_df = freshness_df.copy()
freshness_df["latest_date"] = pd.to_datetime(freshness_df["latest_date"], errors="coerce")

freshness_display = freshness_df.rename(
    columns={
        "source_name": "Retailer",
        "latest_date": "Latest Date",
        "tracked_products": "Tracked Products",
    }
).copy()
freshness_display["Latest Date"] = freshness_display["Latest Date"].map(format_date_label)

st.dataframe(freshness_display, use_container_width=True, hide_index=True)

retailer_count = freshness_df["source_name"].dropna().nunique()

if retailer_count < 2:
    st.warning(
        "Only one retailer currently has surfaced mart data. Cross-retailer comparison needs at least two retailers."
    )
    st.stop()

# --------------------------------------------------
# Product selector
# --------------------------------------------------
products_df = run_query(CROSS_RETAILER_PRODUCTS_QUERY)

if products_df.empty:
    st.warning(
        "No products are currently available in at least two retailers. Check the freshness table above for retailer coverage."
    )
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
    comparison_df = comparison_df.copy()
    comparison_df["date"] = pd.to_datetime(comparison_df["date"], errors="coerce")

    comparison_retailer_count = comparison_df["source_name"].dropna().nunique()
    if comparison_retailer_count < 2:
        st.warning(
            "Only one retailer is currently surfaced for this product, so the comparison may be incomplete."
        )

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
    st.info("No latest common-date comparison is available across at least two retailers.")
else:
    st.dataframe(cheapest_df, use_container_width=True)
