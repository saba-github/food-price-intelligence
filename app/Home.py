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
    .stApp {
        background-color: #0b0d10;
    }
    
        [data-testid="stSidebar"] {
        display: none;
    }

    .block-container {
        padding-top: 1.4rem;
        padding-bottom: 2rem;
        padding-left: 2rem;
        padding-right: 2rem;
        max-width: 1400px;
    }

    div[data-testid="stHorizontalBlock"] {
        gap: 0.9rem;
    }

    .app-shell {
        background: linear-gradient(180deg, #111315 0%, #0c0e11 100%);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 24px;
        padding: 1.4rem 1.4rem 1.2rem 1.4rem;
        box-shadow: 0 10px 30px rgba(0,0,0,0.24);
    }

    .topbar {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 1rem;
        margin-bottom: 0.6rem;
    }

    .eyebrow {
        color: #d4a857;
        font-size: 0.78rem;
        font-weight: 700;
        letter-spacing: 0.14em;
        text-transform: uppercase;
        margin-bottom: 0.3rem;
    }

    .hero-title {
        font-size: 2.15rem;
        font-weight: 800;
        color: #ffffff;
        line-height: 1.05;
        margin: 0;
    }

    .hero-subtitle {
        font-size: 1rem;
        color: #d1d5db;
        margin-top: 0.35rem;
        margin-bottom: 0;
    }

    .top-badges {
        display: flex;
        justify-content: flex-end;
        gap: 0.55rem;
        flex-wrap: wrap;
        margin-top: 0.15rem;
    }

    .badge {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        padding: 0.42rem 0.8rem;
        border-radius: 999px;
        font-size: 0.82rem;
        font-weight: 700;
        border: 1px solid rgba(255,255,255,0.08);
        background: #171a1f;
        color: #f3f4f6;
        white-space: nowrap;
    }

    .badge-health {
        background: #e8fff6;
        color: #0f8b62;
        border: 1px solid rgba(16,185,129,0.25);
    }

    .badge-run {
        background: #eef4ff;
        color: #2563eb;
        border: 1px solid rgba(37,99,235,0.15);
    }

    .badge-date {
        background: #1a1d22;
        color: #e5e7eb;
        border: 1px solid rgba(255,255,255,0.08);
    }

    .nav-row {
        display: flex;
        gap: 2rem;
        align-items: center;
        border-bottom: 1px solid rgba(255,255,255,0.10);
        padding-top: 1.15rem;
        padding-bottom: 0.85rem;
        margin-bottom: 1rem;
        overflow-x: auto;
    }

    .nav-item {
        color: #f3f4f6;
        font-size: 0.95rem;
        font-weight: 650;
        white-space: nowrap;
        opacity: 0.92;
    }

    .nav-item-active {
        position: relative;
    }

    .nav-item-active::after {
        content: "";
        position: absolute;
        left: 0;
        bottom: -0.87rem;
        width: 100%;
        height: 2px;
        background: #ffffff;
        border-radius: 999px;
    }

    .metric-card {
        background: linear-gradient(180deg, #262626 0%, #222222 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 16px;
        padding: 1rem 1rem 0.9rem 1rem;
        min-height: 132px;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.03);
    }

    .metric-label {
        font-size: 0.78rem;
        color: #b8bfc9;
        font-weight: 800;
        letter-spacing: 0.11em;
        text-transform: uppercase;
        margin-bottom: 0.7rem;
    }

    .metric-value {
        font-size: 2rem;
        font-weight: 800;
        color: #ffffff;
        line-height: 1;
        margin-bottom: 0.45rem;
    }

    .metric-sub {
        font-size: 0.9rem;
        color: #d1d5db;
        line-height: 1.3;
    }

    .health-pill {
        display: inline-block;
        padding: 0.24rem 0.72rem;
        border-radius: 999px;
        font-size: 0.95rem;
        font-weight: 800;
        background: #eafff4;
        color: #0f8b62;
        border: 1px solid rgba(16,185,129,0.22);
        margin-bottom: 0.4rem;
    }

    .health-banner {
        background: #dff3ea;
        border: 1px solid rgba(16,185,129,0.16);
        border-radius: 14px;
        padding: 0.95rem 1rem;
        color: #0f3d30;
        margin-top: 1rem;
        margin-bottom: 1rem;
        font-size: 0.98rem;
        font-weight: 650;
        display: flex;
        align-items: center;
        gap: 0.7rem;
        flex-wrap: wrap;
    }

    .health-dot {
        width: 10px;
        height: 10px;
        border-radius: 999px;
        background: #1aa56b;
        flex-shrink: 0;
    }

    .panel-card {
        background: linear-gradient(180deg, #262626 0%, #222222 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 18px;
        padding: 1rem;
        min-height: 100%;
    }

    .panel-title {
        font-size: 1.1rem;
        font-weight: 800;
        color: #ffffff;
        margin-bottom: 0.9rem;
        display: flex;
        align-items: center;
        gap: 0.45rem;
    }

    .dot-green, .dot-red, .dot-blue {
        width: 8px;
        height: 8px;
        border-radius: 999px;
        display: inline-block;
        flex-shrink: 0;
    }

    .dot-green { background: #22c55e; }
    .dot-red { background: #ef4444; }
    .dot-blue { background: #3b82f6; }

    .item-card {
        background: rgba(255,255,255,0.02);
        border: 1px solid rgba(255,255,255,0.07);
        border-radius: 12px;
        padding: 0.85rem 0.85rem 0.75rem 0.85rem;
        margin-bottom: 0.75rem;
    }

    .item-row {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 0.8rem;
    }

    .item-title {
        font-size: 1rem;
        font-weight: 750;
        color: #ffffff;
        line-height: 1.15;
        margin-bottom: 0.2rem;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }

    .item-sub {
        font-size: 0.86rem;
        color: #b5bcc8;
    }

    .item-change-up {
        color: #22c55e;
        font-weight: 800;
        font-size: 0.98rem;
        white-space: nowrap;
    }

    .item-change-down {
        color: #ef4444;
        font-weight: 800;
        font-size: 0.98rem;
        white-space: nowrap;
    }

    .trend-meta {
        text-align: right;
        min-width: 84px;
    }

    .trend-price {
        font-size: 0.98rem;
        font-weight: 800;
        color: #ffffff;
        line-height: 1.1;
    }

    .trend-change-up {
        color: #22c55e;
        font-weight: 800;
        font-size: 0.9rem;
        margin-top: 0.22rem;
    }

    .trend-change-down {
        color: #ef4444;
        font-weight: 800;
        font-size: 0.9rem;
        margin-top: 0.22rem;
    }
    .trend-date {
        font-size: 0.75rem;
        color: #94a3b8;
        margin-top: 0.2rem;
    }
    .empty-state {
        color: #cbd5e1;
        font-size: 0.95rem;
        padding: 0.4rem 0.1rem;
    }

    [data-testid="stPlotlyChart"] {
        margin-top: -0.3rem;
        margin-bottom: -0.8rem;
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

def format_date_badge(value):
    if value is None:
        return "No date"
    try:
        return value.strftime("%b %-d, %Y")
    except Exception:
        try:
            return value.strftime("%b %d, %Y").replace(" 0", " ")
        except Exception:
            return str(value)

def format_health_status(value):
    if not value:
        return "Unknown"
    return str(value).replace("_", " ").title()

def metric_card(label, value, subtext="", is_health=False):
    if is_health:
        value_html = f'<div class="health-pill">{value}</div>'
    else:
        value_html = f'<div class="metric-value">{value}</div>'

    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            {value_html}
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
                width=2.2,
            ),
            hoverinfo="skip",
        )
    )

    fig.update_layout(
        margin=dict(l=0, r=0, t=8, b=8),
        height=64,
        xaxis=dict(visible=False, fixedrange=True),
        yaxis=dict(visible=False, fixedrange=True),
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

latest_run_id = safe_int(latest_run["run_id"]) if latest_run is not None else None
health_status = format_health_status(
    latest_run["pipeline_health_status"] if latest_run is not None else None
)
total_checks = safe_int(latest_run["total_checks"]) if latest_run is not None else 0
passed_checks = safe_int(latest_run["passed_checks"]) if latest_run is not None else 0

# --------------------------------------------------
# Header shell
# --------------------------------------------------
st.markdown('<div class="app-shell">', unsafe_allow_html=True)

left_head, right_head = st.columns([1.65, 1])

with left_head:
    st.markdown('<div class="eyebrow">Migros · Price Intelligence</div>', unsafe_allow_html=True)
    st.markdown('<div class="hero-title">Market Overview</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="hero-subtitle">Retail price tracking & pipeline monitoring</div>',
        unsafe_allow_html=True,
    )

with right_head:
    st.markdown(
        f"""
        <div class="top-badges">
            <div class="badge badge-health">{health_status}</div>
            <div class="badge badge-run">Run #{latest_run_id if latest_run_id else "N/A"}</div>
            <div class="badge badge-date">{format_date_badge(latest_date)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown(
    """
    <div class="nav-row">
        <div class="nav-item nav-item-active">Overview</div>
        <div class="nav-item">Trend analysis</div>
        <div class="nav-item">Top movers</div>
        <div class="nav-item">Anomalies</div>
        <div class="nav-item">Pipeline health</div>
    </div>
    """,
    unsafe_allow_html=True,
)

# --------------------------------------------------
# KPI cards
# --------------------------------------------------
k1, k2, k3, k4, k5 = st.columns(5)

with k1:
    metric_card(
        "Products",
        f"{product_count if product_count is not None else 'N/A'}",
        "Distinct tracked items",
    )

with k2:
    metric_card(
        "Observations",
        f"{observation_count if observation_count is not None else 'N/A'}",
        "Price data points",
    )

with k3:
    metric_card(
        "Avg Price / Unit",
        f"₺{avg_price_per_unit:.2f}" if avg_price_per_unit is not None else "N/A",
        "Across trusted observations",
    )

with k4:
    metric_card(
        "Total Runs",
        f"{total_runs if total_runs is not None else 'N/A'}",
        "Pipeline executions",
    )

with k5:
    metric_card(
        "Latest Run Health",
        health_status,
        f"{passed_checks}/{total_checks} checks passed" if total_checks else "Most recent pipeline status",
        is_health=True,
    )

# --------------------------------------------------
# Health banner
# --------------------------------------------------
if latest_run is not None:
    banner_status_text = "completed successfully" if passed_checks == total_checks and total_checks > 0 else "completed"
    st.markdown(
        f"""
        <div class="health-banner">
            <span class="health-dot"></span>
            <span>Latest pipeline run</span>
            <strong>#{latest_run_id}</strong>
            <span>{banner_status_text} — </span>
            <strong>{passed_checks}/{total_checks}</strong>
            <span>quality checks passed. Materialized views refreshed.</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

# --------------------------------------------------
# Main content
# --------------------------------------------------
left_col, mid_col, right_col = st.columns([1, 1, 1.25])

with left_col:
    st.markdown('<div class="panel-card">', unsafe_allow_html=True)
    st.markdown(
        '<div class="panel-title"><span class="dot-green"></span>Top price increases</div>',
        unsafe_allow_html=True,
    )
    if movers_df.empty:
        st.markdown('<div class="empty-state">No mover data available.</div>', unsafe_allow_html=True)
    else:
        for _, row in movers_df.iterrows():
            product = row["standardized_product_name"]
            category = row["category_name"] if row["category_name"] else "Unknown"
            pct_change = safe_float(row["pct_change"])

            st.markdown(
                f"""
                <div class="item-card">
                    <div class="item-row">
                        <div>
                            <div class="item-title">{product}</div>
                            <div class="item-sub">{category}</div>
                        </div>
                        <div class="item-change-up">+{pct_change:.1f}%</div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown('</div>', unsafe_allow_html=True)

with mid_col:
    st.markdown('<div class="panel-card">', unsafe_allow_html=True)
    st.markdown(
        '<div class="panel-title"><span class="dot-red"></span>Top price drops</div>',
        unsafe_allow_html=True,
    )

    if decliners_df.empty:
        st.markdown('<div class="empty-state">No decliner data available.</div>', unsafe_allow_html=True)
    else:
        for _, row in decliners_df.iterrows():
            product = row["standardized_product_name"]
            category = row["category_name"] if row["category_name"] else "Unknown"
            pct_change = safe_float(row["pct_change"])

            st.markdown(
                f"""
                <div class="item-card">
                    <div class="item-row">
                        <div>
                            <div class="item-title">{product}</div>
                            <div class="item-sub">{category}</div>
                        </div>
                        <div class="item-change-down">{pct_change:.1f}%</div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown('</div>', unsafe_allow_html=True)

with right_col:
    st.markdown('<div class="panel-card">', unsafe_allow_html=True)
    st.markdown(
        '<div class="panel-title"><span class="dot-blue"></span>Recent price trends</div>',
        unsafe_allow_html=True,
    )

    if trends_df.empty:
        st.markdown('<div class="empty-state">No trend data available.</div>', unsafe_allow_html=True)
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
            last_date = product_df["date"].iloc[-1]
            last_date_str = last_date.strftime("%b %d").replace(" 0", " ")
            spark_fig = build_sparkline(product_df, pct_change)

            with st.container(border=True):
                a, b, c = st.columns([1.2, 1.35, 0.75])

                with a:
                    st.markdown(f'<div class="item-title">{product}</div>', unsafe_allow_html=True)

                with b:
                    st.plotly_chart(
                        spark_fig,
                        use_container_width=True,
                        config={"displayModeBar": False},
                    )

                with c:
                    trend_class = "trend-change-up" if pct_change >= 0 else "trend-change-down"
                    sign = "+" if pct_change >= 0 else ""
                    st.markdown(
                        f"""
                        <div class="trend-meta">
                            <div class="trend-price">₺{latest_price:.1f}</div>
                            <div class="{trend_class}">{sign}{pct_change:.1f}%</div>
                            <div class="trend-date">{last_date_str}</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

            st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)
