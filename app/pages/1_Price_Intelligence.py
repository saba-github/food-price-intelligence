
import streamlit as st
import plotly.express as px

from app.db import run_query
from app.queries import (
    LATEST_DATES_QUERY,
    CATEGORY_LIST_QUERY,
    TOP_EXPENSIVE_QUERY,
    TOP_CHEAPEST_QUERY,
    TOP_VOLATILE_QUERY,
    PRICE_TREND_QUERY,
)

st.set_page_config(page_title="Price Intelligence", layout="wide")

st.title("Price Intelligence")

dates_df = run_query(LATEST_DATES_QUERY)
available_dates = dates_df["date"].tolist()

categories_df = run_query(CATEGORY_LIST_QUERY)
available_categories = categories_df["category_name"].dropna().tolist()

col1, col2 = st.columns([1, 1])

with col1:
    selected_date = st.selectbox("Select date", available_dates)

with col2:
    selected_category = st.selectbox(
        "Filter by category",
        ["All"] + available_categories
    )

expensive_df = run_query(TOP_EXPENSIVE_QUERY, {"selected_date": selected_date})
cheap_df = run_query(TOP_CHEAPEST_QUERY, {"selected_date": selected_date})
volatile_df = run_query(TOP_VOLATILE_QUERY)

if selected_category != "All":
    expensive_df = expensive_df[expensive_df["category_name"] == selected_category]
    cheap_df = cheap_df[cheap_df["category_name"] == selected_category]

c1, c2, c3 = st.columns(3)

with c1:
    st.metric("Selected Date", str(selected_date))

with c2:
    st.metric("Most Expensive Product", expensive_df.iloc[0]["standardized_product_name"] if not expensive_df.empty else "-")

with c3:
    st.metric("Cheapest Product", cheap_df.iloc[0]["standardized_product_name"] if not cheap_df.empty else "-")

st.subheader("Top 10 Most Expensive Products")
fig_expensive = px.bar(
    expensive_df,
    x="standardized_product_name",
    y="avg_price",
    hover_data=["category_name"],
    title="Most Expensive Products"
)
st.plotly_chart(fig_expensive, use_container_width=True)

st.subheader("Top 10 Cheapest Products")
fig_cheap = px.bar(
    cheap_df,
    x="standardized_product_name",
    y="avg_price",
    hover_data=["category_name"],
    title="Cheapest Products"
)
st.plotly_chart(fig_cheap, use_container_width=True)

st.subheader("Most Volatile Products")
st.dataframe(volatile_df, use_container_width=True)

product_options = sorted(expensive_df["standardized_product_name"].dropna().unique().tolist())
selected_product = st.selectbox("Select product for trend analysis", product_options)

trend_df = run_query(PRICE_TREND_QUERY, {"product_name": selected_product})

st.subheader(f"Price Trend — {selected_product}")
fig_trend = px.line(
    trend_df,
    x="date",
    y="avg_price",
    title=f"Daily Average Price Trend: {selected_product}",
    markers=True
)
st.plotly_chart(fig_trend, use_container_width=True)
