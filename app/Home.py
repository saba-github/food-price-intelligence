import streamlit as st
import plotly.graph_objects as go

from db import run_query
from queries import (
    HOME_TOP_MOVER_CARD_QUERY,
    HOME_TOP_DECLINER_CARD_QUERY,
    HOME_RECENT_TRENDS_QUERY,
)

# --------------------------------------------------
# Page config
# --------------------------------------------------
st.set_page_config(page_title="Food Price Intelligence", layout="wide")

st.title("Food Price Intelligence System")
st.caption("A production-style data pipeline for monitoring retail price movements.")
st.markdown("Real-time retail price intelligence powered by automated data pipelines.")


# --------------------------------------------------
# Queries
# --------------------------------------------------
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


# --------------------------------------------------
# Helpers
# --------------------------------------------------
def safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def build_sparkline(product_df, pct_change):
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=product_df["date"],
            y=product_df["avg_price"],
            mode="lines",
            line=dict(
                color="#63e6be" if pct_change >= 0 else "#ff8c8c",
                width=2,
            ),
            fill="tozeroy",
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        height=60,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        showlegend=False,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return fig


# --------------------------------------------------
# Load data
# --------------------------------------------------
summary_df = run_query(HOME_SUMMARY_QUERY)
total_runs_df = run_query(TOTAL_RUNS_QUERY)
latest_run_df = run_query(LATEST_RUN_QUERY)
latest_date_df = run_query(LATEST_DATE_QUERY)

movers_df = run_query(HOME_TOP_MOVER_CARD_QUERY)
decliners_df = run_query(HOME_TOP_DECLINER_CARD_QUERY)
trends_df = run_query(HOME_RECENT_TRENDS_QUERY)


# --------------------------------------------------
# Parse summary values
# --------------------------------------------------
product_count = None
observation_count = None
avg_price_per_unit = None
total_runs = None
latest_run = None
latest_date = None

if not summary_df.empty:
    product_count = safe_int(summary_df.iloc[0]["product_count"])
    observation_count = safe_int(summary_df.iloc[0]["observation_count"])
    avg_price_per_unit = safe_float(summary_df.iloc[0]["avg_price_per_unit"])

if not total_runs_df.empty:
    total_runs = safe_int(total_runs_df.iloc[0]["total_runs"])

if not latest_run_df.empty:
    latest_run = latest_run_df.iloc[0]

if not latest_date_df.empty:
    latest_date = latest_date_df.iloc[0]["latest_date"]


# --------------------------------------------------
# Header info
# --------------------------------------------------
if latest_date is not None:
    st.caption(f"Latest analytics date: {latest_date}")


# --------------------------------------------------
# KPI cards
# --------------------------------------------------
col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Products", product_count if product_count is not None else "N/A")
col2.metric("Observations", observation_count if observation_count is not None else "N/A")
col3.metric(
    "Avg Price / Unit",
    f"₺{avg_price_per_unit:.2f}" if avg_price_per_unit is not None else "N/A"
)
col4.metric("Total Runs", total_runs if total_runs is not None else "N/A")

if latest_run is not None:
    col5.metric("Latest Run Health", latest_run["pipeline_health_status"])

    total_checks = safe_int(latest_run["total_checks"])
    passed_checks = safe_int(latest_run["passed_checks"])

    st.success(
        f"Latest pipeline run #{latest_run['run_id']} completed successfully with "
        f"{passed_checks}/{total_checks} checks passed."
    )
else:
    col5.metric("Latest Run Health", "N/A")


# --------------------------------------------------
# Insight sections
# --------------------------------------------------
left_col, mid_col, right_col = st.columns([1, 1, 1.3])


# -------------------------
# Top movers
# -------------------------
with left_col:
    st.markdown("### Price increases ↑")

    if movers_df.empty:
        st.info("No mover data available.")
    else:
        for _, row in movers_df.iterrows():
            product = row["standardized_product_name"]
            category = row["category_name"] if row["category_name"] else "Unknown"
            pct_change = safe_float(row["pct_change"])

            st.markdown(
                f"""
                **{product}**  
                <span style="color:#9aa0a6;">{category}</span>
                <span style="float:right;"><b>+{pct_change:.1f}%</b></span>
                """,
                unsafe_allow_html=True,
            )
            st.markdown("<br>", unsafe_allow_html=True)


# -------------------------
# Top decliners
# -------------------------
with mid_col:
    st.markdown("### Price drops ↓")

    if decliners_df.empty:
        st.info("No decliner data available.")
    else:
        for _, row in decliners_df.iterrows():
            product = row["standardized_product_name"]
            category = row["category_name"] if row["category_name"] else "Unknown"
            pct_change = safe_float(row["pct_change"])

            st.markdown(
                f"""
                **{product}**  
                <span style="color:#9aa0a6;">{category}</span>
                <span style="float:right;"><b>{pct_change:.1f}%</b></span>
                """,
                unsafe_allow_html=True,
            )
            st.markdown("<br>", unsafe_allow_html=True)


# -------------------------
# Recent price trends
# -------------------------
with right_col:
    st.markdown("### Recent price trends")

    if trends_df.empty:
        st.info("No trend data available.")
    else:
        trend_products = (
            trends_df["standardized_product_name"]
            .dropna()
            .unique()
            .tolist()
        )

        for product in trend_products:
            product_df = trends_df[
                trends_df["standardized_product_name"] == product
            ].copy()

            product_df = product_df.sort_values("date")

            if product_df.empty:
                continue

            first_price = safe_float(product_df["avg_price"].iloc[0])
            latest_price = safe_float(product_df["avg_price"].iloc[-1])

            if first_price != 0:
                pct_change = ((latest_price - first_price) / first_price) * 100
            else:
                pct_change = 0.0

            spark_fig = build_sparkline(product_df, pct_change)

            row1, row2, row3 = st.columns([1.1, 1.4, 0.8])

            with row1:
                st.markdown(f"**{product}**")

            with row2:
                st.plotly_chart(
                    spark_fig,
                    use_container_width=True,
                    config={"displayModeBar": False},
                )

            with row3:
                sign = "+" if pct_change > 0 else ""
                st.markdown(
                    f"₺{latest_price:.1f}  \n**{sign}{pct_change:.1f}%**"
                )
