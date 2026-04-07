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

# --------------------------------------------------
# Custom CSS
# --------------------------------------------------
st.markdown("""
<style>
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        padding-left: 2rem;
        padding-right: 2rem;
    }

    .hero-title {
        font-size: 2.6rem;
        font-weight: 800;
        color: #ffffff;
        margin-bottom: 0.3rem;
    }

    .hero-subtitle {
        font-size: 1rem;
        color: #9aa4b2;
        margin-bottom: 1.5rem;
    }

    .section-card {
        background: rgba(17, 24, 39, 0.85);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 18px;
        padding: 20px 20px 14px 20px;
        box-shadow: 0 8px 24px rgba(0,0,0,0.18);
        margin-bottom: 18px;
    }

    .metric-card {
        background: linear-gradient(180deg, rgba(17,24,39,0.95), rgba(10,15,30,0.95));
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 18px;
        padding: 18px 18px 14px 18px;
        min-height: 115px;
        box-shadow: 0 10px 24px rgba(0,0,0,0.20);
    }

    .metric-label {
        font-size: 0.9rem;
        color: #9aa4b2;
        margin-bottom: 10px;
        font-weight: 500;
    }

    .metric-value {
        font-size: 2rem;
        font-weight: 800;
        color: #ffffff;
        line-height: 1.1;
    }

    .metric-sub {
        margin-top: 8px;
        font-size: 0.82rem;
        color: #7dd3fc;
    }

    .section-title {
        font-size: 1.45rem;
        font-weight: 750;
        color: #ffffff;
        margin-bottom: 14px;
    }

    .item-card {
        background: rgba(255,255,255,0.02);
        border: 1px solid rgba(255,255,255,0.05);
        border-radius: 14px;
        padding: 14px 14px 10px 14px;
        margin-bottom: 12px;
    }

    .item-title {
        font-size: 1rem;
        font-weight: 700;
        color: #ffffff;
        margin-bottom: 4px;
    }

    .item-sub {
        font-size: 0.85rem;
        color: #94a3b8;
    }

    .item-change-up {
        float: right;
        color: #22c55e;
        font-weight: 700;
    }

    .item-change-down {
        float: right;
        color: #ef4444;
        font-weight: 700;
    }

    .health-banner {
        background: linear-gradient(90deg, rgba(22,101,52,0.95), rgba(21,128,61,0.75));
        border: 1px solid rgba(34,197,94,0.25);
        color: #ecfdf5;
        padding: 14px 16px;
        border-radius: 14px;
        font-weight: 600;
        margin-top: 10px;
        margin-bottom: 20px;
    }

    .trend-price {
        font-size: 0.95rem;
        font-weight: 700;
        color: white;
        text-align: right;
    }

    .trend-change-up {
        color: #22c55e;
        font-weight: 700;
        text-align: right;
        font-size: 0.9rem;
    }

    .trend-change-down {
        color: #ef4444;
        font-weight: 700;
        text-align: right;
        font-size: 0.9rem;
    }
</style>
""", unsafe_allow_html=True)

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

def metric_card(label, value, subtext=""):
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-sub">{subtext}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def build_sparkline(product_df, pct_change):
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=product_df["date"],
            y=product_df["avg_price"],
            mode="lines",
            line=dict(
                color="#22c55e" if pct_change >= 0 else "#ef4444",
                width=2.5,
            ),
            fill="tozeroy",
        )
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        height=70,
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
# Parse values
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
# Hero
# --------------------------------------------------
st.markdown('<div class="hero-title">Food Price Intelligence System</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="hero-subtitle">Monitor retail price movements with a production-style analytics and pipeline monitoring dashboard.</div>',
    unsafe_allow_html=True,
)

if latest_date is not None:
    st.caption(f"Latest analytics date: {latest_date}")

# --------------------------------------------------
# KPI cards
# --------------------------------------------------
k1, k2, k3, k4, k5 = st.columns(5)

with k1:
    metric_card("Products", f"{product_count if product_count is not None else 'N/A'}", "Tracked distinct products")

with k2:
    metric_card("Observations", f"{observation_count if observation_count is not None else 'N/A'}", "Price observations collected")

with k3:
    metric_card("Avg Price / Unit", f"₺{avg_price_per_unit:.2f}" if avg_price_per_unit is not None else "N/A", "Average unit-normalized price")

with k4:
    metric_card("Total Runs", f"{total_runs if total_runs is not None else 'N/A'}", "Pipeline executions")

with k5:
    run_health = latest_run["pipeline_health_status"] if latest_run is not None else "N/A"
    metric_card("Latest Run Health", f"{run_health}", "Most recent pipeline status")

if latest_run is not None:
    total_checks = safe_int(latest_run["total_checks"])
    passed_checks = safe_int(latest_run["passed_checks"])
    st.markdown(
        f"""
        <div class="health-banner">
            Latest pipeline run <b>#{latest_run['run_id']}</b> completed successfully with
            <b>{passed_checks}/{total_checks}</b> checks passed.
        </div>
        """,
        unsafe_allow_html=True,
    )

# --------------------------------------------------
# Main sections
# --------------------------------------------------
left_col, mid_col, right_col = st.columns([1, 1, 1.25])

with left_col:
    st.markdown('<div class="section-title">Price increases ↑</div>', unsafe_allow_html=True)

    if movers_df.empty:
        st.info("No mover data available.")
    else:
        for _, row in movers_df.iterrows():
            product = row["standardized_product_name"]
            category = row["category_name"] if row["category_name"] else "Unknown"
            pct_change = safe_float(row["pct_change"])

            st.markdown(
                f"""
                <div class="item-card">
                    <div class="item-title">
                        {product}
                        <span class="item-change-up">+{pct_change:.1f}%</span>
                    </div>
                    <div class="item-sub">{category}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

with mid_col:
    st.markdown('<div class="section-title">Price drops ↓</div>', unsafe_allow_html=True)

    if decliners_df.empty:
        st.info("No decliner data available.")
    else:
        for _, row in decliners_df.iterrows():
            product = row["standardized_product_name"]
            category = row["category_name"] if row["category_name"] else "Unknown"
            pct_change = safe_float(row["pct_change"])

            st.markdown(
                f"""
                <div class="item-card">
                    <div class="item-title">
                        {product}
                        <span class="item-change-down">{pct_change:.1f}%</span>
                    </div>
                    <div class="item-sub">{category}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

with right_col:
    st.markdown('<div class="section-title">Recent price trends</div>', unsafe_allow_html=True)

    if trends_df.empty:
        st.info("No trend data available.")
    else:
        trend_products = trends_df["standardized_product_name"].dropna().unique().tolist()

        for product in trend_products:
            product_df = trends_df[trends_df["standardized_product_name"] == product].copy()
            product_df = product_df.sort_values("date")

            if product_df.empty:
                continue

            first_price = safe_float(product_df["avg_price"].iloc[0])
            latest_price = safe_float(product_df["avg_price"].iloc[-1])

            pct_change = ((latest_price - first_price) / first_price * 100) if first_price != 0 else 0.0
            spark_fig = build_sparkline(product_df, pct_change)

            st.markdown('<div class="item-card">', unsafe_allow_html=True)
            a, b, c = st.columns([1.05, 1.5, 0.7])

            with a:
                st.markdown(f"**{product}**")

            with b:
                st.plotly_chart(
                    spark_fig,
                    use_container_width=True,
                    config={"displayModeBar": False},
                )

            with c:
                trend_class = "trend-change-up" if pct_change >= 0 else "trend-change-down"
                sign = "+" if pct_change >= 0 else ""
                st.markdown(f'<div class="trend-price">₺{latest_price:.1f}</div>', unsafe_allow_html=True)
                st.markdown(f'<div class="{trend_class}">{sign}{pct_change:.1f}%</div>', unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)
