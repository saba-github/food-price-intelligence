import streamlit as st
import plotly.graph_objects as go

from db import run_query
from queries import (
    HOME_TOP_MOVER_CARD_QUERY,
    HOME_TOP_DECLINER_CARD_QUERY,
    HOME_RECENT_TRENDS_QUERY,
)

st.set_page_config(page_title="Food Price Intelligence", layout="wide")
st.markdown("Real-time retail price intelligence powered by automated data pipelines.")

# -----------------------------
# Queries
# -----------------------------
HOME_SUMMARY_QUERY = """
select
    count(distinct standardized_product_name) as product_count,
    count(*) as observation_count,
    round(avg(price_per_unit)::numeric, 2) as avg_price_per_unit
from fact_price_observations
where price_per_unit is not null;
"""


TOTAL_RUNS_QUERY = """
select count(*) as total_runs
from scrape_runs;
"""




LATEST_RUN_QUERY = """
select
    run_id,
    status,
    finished_at,
    pipeline_health_status,
    total_checks,
    passed_checks,
    failed_checks
from mart_pipeline_health
order by run_id desc
limit 1;
"""



LATEST_DATE_QUERY = """
select max(date) as latest_date
from mart_daily_prices;
"""

# -----------------------------
# Data
# -----------------------------
summary_df = run_query(HOME_SUMMARY_QUERY)
latest_run_df = run_query(LATEST_RUN_QUERY)
latest_date_df = run_query(LATEST_DATE_QUERY)
total_runs_df = run_query(TOTAL_RUNS_QUERY)

product_count = None
observation_count = None
avg_price_per_unit = None
total_runs = None

if not summary_df.empty:
    product_count = int(summary_df.iloc[0]["product_count"])
    observation_count = int(summary_df.iloc[0]["observation_count"])
    avg_price_per_unit = summary_df.iloc[0]["avg_price_per_unit"]

if not total_runs_df.empty:
    total_runs = int(total_runs_df.iloc[0]["total_runs"])

latest_run = latest_run_df.iloc[0] if not latest_run_df.empty else None
latest_date = latest_date_df.iloc[0]["latest_date"] if not latest_date_df.empty else None

# -----------------------------
# Title
# -----------------------------
st.title("Food Price Intelligence System")
st.caption("A production-style data pipeline for monitoring retail price movements.")

if latest_date is not None:
    st.caption(f"Latest analytics date: {latest_date}")

# -----------------------------
# KPI Cards
# -----------------------------
c1, c2, c3, c4, c5 = st.columns(5)

c1.metric("Products", product_count)
c2.metric("Observations", observation_count)
c3.metric("Avg Price / Unit", avg_price_per_unit)
c4.metric("Total Runs", total_runs)

if latest_run is not None:
    c5.metric("Latest Run Health", latest_run["pipeline_health_status"])
    st.success(
        f"Latest pipeline run #{latest_run['run_id']} completed with "
        f"{latest_run['passed_checks']} / {latest_run['total_checks']} checks passed."
    )
else:
    c5.metric("Latest Run Health", "N/A")

# ---------------------------------
# Home insight cards
# ---------------------------------
movers_df = run_query(HOME_TOP_MOVER_CARD_QUERY)
decliners_df = run_query(HOME_TOP_DECLINER_CARD_QUERY)
trends_df = run_query(HOME_RECENT_TRENDS_QUERY)

left_col, mid_col, right_col = st.columns([1, 1, 1.3])

with left_col:
    st.markdown("### Price increases   ↑ top movers")

    if movers_df.empty:
        st.info("No mover data available.")
    else:
        for _, row in movers_df.iterrows():
            product = row["standardized_product_name"]
            category = row["category_name"] if row["category_name"] else ""
            pct = float(row["pct_change"]) if row["pct_change"] is not None else 0

            st.markdown(
                f"""
                **{product}**  
                <span style="color:#bfbfbf;">{category}</span>
                <span style="float:right;"><b>+{pct:.1f}%</b></span>
                """,
                unsafe_allow_html=True,
            )
            st.markdown("<br>", unsafe_allow_html=True)

with mid_col:
    st.markdown("### Price drops   ↓ top movers")

    if decliners_df.empty:
        st.info("No decliner data available.")
    else:
        for _, row in decliners_df.iterrows():
            product = row["standardized_product_name"]
            category = row["category_name"] if row["category_name"] else ""
            pct = float(row["pct_change"]) if row["pct_change"] is not None else 0

            st.markdown(
                f"""
                **{product}**  
                <span style="color:#bfbfbf;">{category}</span>
                <span style="float:right;"><b>{pct:.1f}%</b></span>
                """,
                unsafe_allow_html=True,
            )
            st.markdown("<br>", unsafe_allow_html=True)

with right_col:
    st.markdown("### Recent price trends")

    if trends_df.empty:
        st.info("No trend data available.")
    else:
        trend_products = trends_df["standardized_product_name"].dropna().unique().tolist()

        for product in trend_products:
            product_df = trends_df[trends_df["standardized_product_name"] == product].copy()
            product_df = product_df.sort_values("date")

            latest_price = float(product_df["avg_price"].iloc[-1])
            first_price = float(product_df["avg_price"].iloc[0])

            if first_price != 0:
                pct_change = ((latest_price - first_price) / first_price) * 100
            else:
                pct_change = 0

            spark_fig = go.Figure()
            spark_fig.add_trace(
                go.Scatter(
                    x=product_df["date"],
                    y=product_df["avg_price"],
                    mode="lines",
                    line=dict(
                        color="#63e6be" if pct_change > 0 else "#ff8c8c",
                        width=2,
                    ),
                    fill="tozeroy",
                )
            )

            spark_fig.update_layout(
                margin=dict(l=0, r=0, t=0, b=0),
                height=60,
                xaxis=dict(visible=False),
                yaxis=dict(visible=False),
                showlegend=False,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
            )

            row1, row2, row3 = st.columns([1.1, 1.4, 0.8])

            with row1:
                st.markdown(f"**{product}**")

            with row2:
                st.plotly_chart(spark_fig, use_container_width=True, config={"displayModeBar": False})

            with row3:
                sign = "+" if pct_change > 0 else ""
                st.markdown(f"₺{latest_price:.1f}  \n**{sign}{pct_change:.1f}%**")


