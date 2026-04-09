import streamlit as st
import plotly.express as px

from db import run_query
from queries import (
    LATEST_DATES_QUERY,
    CATEGORY_LIST_QUERY,
    TOP_EXPENSIVE_QUERY,
    TOP_CHEAPEST_QUERY,
    PRICE_TREND_QUERY,
)

st.set_page_config(page_title="Trend Analysis", layout="wide")

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

    .filter-card {
        background: linear-gradient(180deg, #262626 0%, #222222 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 16px;
        padding: 0.75rem 1rem 0.8rem 1rem;
        min-height: auto;
    }
    .filter-label {
        font-size: 0.78rem;
        color: #b8bfc9;
        font-weight: 800;
        letter-spacing: 0.11em;
        text-transform: uppercase;
        margin-bottom: 0.6rem;
    }

    .metric-card {
        background: linear-gradient(180deg, #262626 0%, #222222 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 16px;
        padding: 1rem 1rem 0.9rem 1rem;
        min-height: 128px;
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

    .panel-card {
        background: linear-gradient(180deg, #262626 0%, #222222 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 18px;
        padding: 1rem;
        margin-top: 1rem;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.03);
    }
    .panel-title {
        font-size: 1.1rem;
        font-weight: 800;
        color: #ffffff;
        margin-bottom: 0.9rem;
    }
    .panel-subtitle {
        font-size: 0.85rem;
        color: #94a3b8;
        margin-top: -0.35rem;
        margin-bottom: 0.8rem;
    }

    .trend-kpi-row {
        display: flex;
        gap: 0.8rem;
        flex-wrap: wrap;
        margin-bottom: 0.8rem;
    }

    .trend-kpi {
        background: rgba(255,255,255,0.03);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 12px;
        padding: 0.75rem 0.9rem;
        min-width: 150px;
    }

    .trend-kpi-label {
        font-size: 0.72rem;
        color: #94a3b8;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 0.35rem;
        font-weight: 700;
    }

    .trend-kpi-value {
        font-size: 1.1rem;
        color: #ffffff;
        font-weight: 800;
    }
    .empty-state {
        color: #cbd5e1;
        font-size: 0.95rem;
        padding: 0.4rem 0.1rem;
    }

    div[data-baseweb="select"] > div {
        background-color: #111315 !important;
        border-color: rgba(255,255,255,0.10) !important;
        color: white !important;
    }

    .stMarkdown, label, div {
        color: inherit;
    }
</style>
""", unsafe_allow_html=True)

# --------------------------------------------------
# Helpers
# --------------------------------------------------
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

# --------------------------------------------------
# Load filters
# --------------------------------------------------
dates_df = run_query(LATEST_DATES_QUERY)
available_dates = dates_df["date"].tolist() if not dates_df.empty else []

categories_df = run_query(CATEGORY_LIST_QUERY)
available_categories = categories_df["category_name"].dropna().tolist() if not categories_df.empty else []

selected_date = available_dates[0] if available_dates else None
selected_category = "All"

# --------------------------------------------------
# Header shell
# --------------------------------------------------
st.markdown('<div class="app-shell">', unsafe_allow_html=True)

st.markdown(
    """
    <div class="hero-card">
        <div class="eyebrow">Migros · Price Intelligence</div>
        <div class="hero-title">Trend Analysis</div>
        <div class="hero-subtitle">
            Explore daily pricing patterns, extremes, and product-level trends
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

nav1, nav2, nav3, nav4, nav5 = st.columns(5)

with nav1:
    st.page_link("Home.py", label="Overview")

with nav2:
    st.page_link("pages/1_Trend_Analysis.py", label="Trend analysis")

with nav3:
    st.page_link("pages/2_Top_Movers.py", label="Top movers")

with nav4:
    st.page_link("pages/3_Anomalies.py", label="Anomalies")

with nav5:
    st.page_link("pages/4_Pipeline_Health.py", label="Pipeline health")


st.markdown(
    """
    <div style="display:flex; justify-content:center; margin-top:-0.1rem; margin-bottom:0.6rem;">
        <div style="
            background: rgba(255,255,255,0.08);
            color: white;
            padding: 0.35rem 0.8rem;
            border-radius: 10px;
            font-size: 0.92rem;
            font-weight: 700;
        ">
            Trend analysis
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)
# --------------------------------------------------
# Filters
# --------------------------------------------------
st.markdown(
    """
    <div class="panel-subtitle">
        Choose a snapshot date and optionally narrow the analysis to a specific category.
    </div>
    """,
    unsafe_allow_html=True,
)

f1, f2 = st.columns(2)

with f1:
    st.markdown('<div class="filter-card">', unsafe_allow_html=True)
    st.markdown('<div class="filter-label">Selected Date</div>', unsafe_allow_html=True)
    if available_dates:
        selected_date = st.selectbox(
            "Selected Date",
            available_dates,
            label_visibility="collapsed",
        )
    else:
        st.markdown('<div class="empty-state">No dates available.</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

with f2:
    st.markdown('<div class="filter-card">', unsafe_allow_html=True)
    st.markdown('<div class="filter-label">Category Filter</div>', unsafe_allow_html=True)
    selected_category = st.selectbox(
        "Category Filter",
        ["All"] + available_categories,
        label_visibility="collapsed",
    )
    st.markdown('</div>', unsafe_allow_html=True)

# --------------------------------------------------
# Main data
# --------------------------------------------------
if selected_date is not None:
    expensive_df = run_query(TOP_EXPENSIVE_QUERY, {"selected_date": selected_date})
    cheap_df = run_query(TOP_CHEAPEST_QUERY, {"selected_date": selected_date})
else:
    expensive_df = run_query(TOP_EXPENSIVE_QUERY, {"selected_date": None}) if False else None
    cheap_df = run_query(TOP_CHEAPEST_QUERY, {"selected_date": None}) if False else None

if selected_date is None:
    expensive_df = None
    cheap_df = None

if expensive_df is not None and selected_category != "All":
    expensive_df = expensive_df[expensive_df["category_name"] == selected_category]

if cheap_df is not None and selected_category != "All":
    cheap_df = cheap_df[cheap_df["category_name"] == selected_category]

# --------------------------------------------------
# KPI cards
# --------------------------------------------------
k1, k2, k3 = st.columns(3)

with k1:
    metric_card(
        "Selected Date",
        format_date_badge(selected_date) if selected_date is not None else "N/A",
        "Current analysis snapshot",
    )

with k2:
    metric_card(
        "Most Expensive",
        expensive_df.iloc[0]["standardized_product_name"] if expensive_df is not None and not expensive_df.empty else "-",
        f"Category: {expensive_df.iloc[0]['category_name']}" if expensive_df is not None and not expensive_df.empty else "No data",
    )

with k3:
    metric_card(
        "Cheapest",
        cheap_df.iloc[0]["standardized_product_name"] if cheap_df is not None and not cheap_df.empty else "-",
        f"Category: {cheap_df.iloc[0]['category_name']}" if cheap_df is not None and not cheap_df.empty else "No data",
    )

# --------------------------------------------------
# Bar charts
# --------------------------------------------------
left_col, right_col = st.columns(2)

with left_col:
    st.markdown('<div class="panel-card">', unsafe_allow_html=True)
    st.markdown('<div class="panel-title">Top 10 Most Expensive Products</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel-subtitle">Highest average prices on the selected date</div>', unsafe_allow_html=True)

    if expensive_df is None or expensive_df.empty:
        st.markdown('<div class="empty-state">No expensive-product data available.</div>', unsafe_allow_html=True)
    else:
        fig_expensive = px.bar(
            expensive_df.sort_values("avg_price", ascending=True),
            x="avg_price",
            y="standardized_product_name",
            hover_data=["category_name"],
            orientation="h",
        )
        fig_expensive.update_traces(marker_color="#60a5fa")
        fig_expensive.update_layout(
            height=420,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Price",
            yaxis_title="",
            font=dict(color="white"),
        )
        st.plotly_chart(fig_expensive, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

with right_col:
    st.markdown('<div class="panel-card">', unsafe_allow_html=True)
    st.markdown('<div class="panel-title">Top 10 Cheapest Products</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel-subtitle">Lowest average prices on the selected date</div>', unsafe_allow_html=True)

    if cheap_df is None or cheap_df.empty:
        st.markdown('<div class="empty-state">No cheapest-product data available.</div>', unsafe_allow_html=True)
    else:
        fig_cheap = px.bar(
            cheap_df.sort_values("avg_price", ascending=True),
            x="avg_price",
            y="standardized_product_name",
            hover_data=["category_name"],
            orientation="h",
        )
        fig_cheap.update_traces(marker_color="#34d399")
        fig_cheap.update_layout(
            height=420,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Price",
            yaxis_title="",
            font=dict(color="white"),
        )
        st.plotly_chart(fig_cheap, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

# --------------------------------------------------
# Product trend view
# --------------------------------------------------
if expensive_df is not None and cheap_df is not None:
    product_pool = sorted(
        list(
            set(
                expensive_df["standardized_product_name"].dropna().tolist()
                + cheap_df["standardized_product_name"].dropna().tolist()
            )
        )
    )
else:
    product_pool = []

st.markdown('<div class="panel-card">', unsafe_allow_html=True)
st.markdown('<div class="panel-title">Daily Product Trend</div>', unsafe_allow_html=True)
st.markdown('<div class="panel-subtitle">Track how a selected product has moved over time</div>', unsafe_allow_html=True)

if product_pool:
    selected_product = st.selectbox(
        "Select product for trend view",
        product_pool,
        label_visibility="visible",
    )

    trend_df = run_query(PRICE_TREND_QUERY, {"product_name": selected_product})

    if not trend_df.empty:
        first_price = float(trend_df["avg_price"].iloc[0])
        latest_price = float(trend_df["avg_price"].iloc[-1])
        pct_change = ((latest_price - first_price) / first_price * 100) if first_price != 0 else 0.0
        first_date = format_date_badge(trend_df["date"].iloc[0])
        last_date = format_date_badge(trend_df["date"].iloc[-1])

        st.markdown(
            f"""
            <div class="trend-kpi-row">
                <div class="trend-kpi">
                    <div class="trend-kpi-label">First observed price</div>
                    <div class="trend-kpi-value">₺{first_price:.1f}</div>
                </div>
                <div class="trend-kpi">
                    <div class="trend-kpi-label">Latest price</div>
                    <div class="trend-kpi-value">₺{latest_price:.1f}</div>
                </div>
                <div class="trend-kpi">
                    <div class="trend-kpi-label">Total change</div>
                    <div class="trend-kpi-value">{pct_change:+.1f}%</div>
                </div>
                <div class="trend-kpi">
                    <div class="trend-kpi-label">Observed period</div>
                    <div class="trend-kpi-value">{first_date} → {last_date}</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    if trend_df.empty:
        st.markdown('<div class="empty-state">No trend data available for this product.</div>', unsafe_allow_html=True)
    else:
        fig_trend = px.line(
            trend_df,
            x="date",
            y="avg_price",
            markers=True,
        )
        fig_trend.update_traces(line=dict(width=3))
        fig_trend.update_layout(
            height=420,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Date",
            yaxis_title="Average Price",
            font=dict(color="white"),
        )
        st.plotly_chart(fig_trend, use_container_width=True)
else:
    st.markdown('<div class="empty-state">No products available for trend view.</div>', unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)
