import streamlit as st
import plotly.express as px

from db import run_query
from queries import  (
    LATEST_DATES_QUERY,
    CATEGORY_LIST_QUERY,
    TOP_EXPENSIVE_QUERY,
    TOP_CHEAPEST_QUERY,
    PRICE_TREND_QUERY,
)

st.set_page_config(page_title="Trend Analysis", layout="wide")

st.title("Trend Analysis")

dates_df = run_query(LATEST_DATES_QUERY)
available_dates = dates_df["date"].tolist()

categories_df = run_query(CATEGORY_LIST_QUERY)
available_categories = categories_df["category_name"].dropna().tolist()

col1, col2 = st.columns(2)

with col1:
    selected_date = st.selectbox("Select date", available_dates)

with col2:
    selected_category = st.selectbox(
        "Filter by category",
        ["All"] + available_categories
    )

expensive_df = run_query(TOP_EXPENSIVE_QUERY, {"selected_date": selected_date})
cheap_df = run_query(TOP_CHEAPEST_QUERY, {"selected_date": selected_date})

if selected_category != "All":
    expensive_df = expensive_df[expensive_df["category_name"] == selected_category]
    cheap_df = cheap_df[cheap_df["category_name"] == selected_category]

c1, c2, c3 = st.columns(3)

with c1:
    st.metric("Selected Date", str(selected_date))

with c2:
    st.metric(
        "Most Expensive",
        expensive_df.iloc[0]["standardized_product_name"] if not expensive_df.empty else "-"
    )

with c3:
    st.metric(
        "Cheapest",
        cheap_df.iloc[0]["standardized_product_name"] if not cheap_df.empty else "-"
    )

st.subheader("Top 10 Most Expensive Products")
fig_expensive = px.bar(
    expensive_df,
    x="standardized_product_name",
    y="avg_price",
    hover_data=["category_name"],
)
st.plotly_chart(fig_expensive, use_container_width=True)

st.subheader("Top 10 Cheapest Products")
fig_cheap = px.bar(
    cheap_df,
    x="standardized_product_name",
    y="avg_price",
    hover_data=["category_name"],
)
st.plotly_chart(fig_cheap, use_container_width=True)

product_pool = sorted(
    list(set(expensive_df["standardized_product_name"].dropna().tolist()
    + cheap_df["standardized_product_name"].dropna().tolist()))
)

if product_pool:
    selected_product = st.selectbox("Select product for trend view", product_pool)
    trend_df = run_query(PRICE_TREND_QUERY, {"product_name": selected_product})

    st.subheader(f"Daily Trend — {selected_product}")
    fig_trend = px.line(
        trend_df,
        x="date",
        y="avg_price",
        markers=True,
    )
    st.plotly_chart(fig_trend, use_container_width=True)
