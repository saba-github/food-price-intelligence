import streamlit as st
import plotly.express as px

from app.db import run_query
from app.queries import TOP_MOVERS_QUERY, TOP_DECLINERS_QUERY

st.set_page_config(page_title="Top Movers", layout="wide")

st.title("Top Movers")

movers_df = run_query(TOP_MOVERS_QUERY)
decliners_df = run_query(TOP_DECLINERS_QUERY)

col1, col2 = st.columns(2)

with col1:
    st.subheader("Top Price Increases")
    st.dataframe(movers_df, use_container_width=True)

    if not movers_df.empty:
        fig_up = px.bar(
            movers_df,
            x="standardized_product_name",
            y="pct_change",
            hover_data=["latest_price", "previous_price", "category_name"],
            title="Top Price Increases (%)",
        )
        st.plotly_chart(fig_up, use_container_width=True)

with col2:
    st.subheader("Top Price Decreases")
    st.dataframe(decliners_df, use_container_width=True)

    if not decliners_df.empty:
        fig_down = px.bar(
            decliners_df,
            x="standardized_product_name",
            y="pct_change",
            hover_data=["latest_price", "previous_price", "category_name"],
            title="Top Price Decreases (%)",
        )
        st.plotly_chart(fig_down, use_container_width=True)
