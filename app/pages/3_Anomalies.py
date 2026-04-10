import streamlit as st
import plotly.express as px
import pandas as pd

from db import run_query
from queries import TOP_VOLATILE_QUERY

st.set_page_config(page_title="Anomalies", layout="wide")

# --------------------------------------------------
# Custom CSS
# --------------------------------------------------
st.markdown("""
<style>
    .stApp {
        background-color: #070b11;
    }

    [data-testid="stSidebar"] {
        display: none;
    }

    .block-container {
        padding-top: 1.3rem;
        padding-bottom: 2rem;
        padding-left: 1.8rem;
        padding-right: 1.8rem;
        max-width: 1450px;
    }

    div[data-testid="stHorizontalBlock"] {
        gap: 0.9rem;
    }

    .app-shell {
        background: linear-gradient(180deg, #0b0f16 0%, #070b11 100%);
        color: #f3f5f7;
        min-height: 100vh;
        border-radius: 22px;
    }

    .hero-card {
        background: linear-gradient(135deg, #0f2034 0%, #0b0f16 55%, #0a0d12 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 22px;
        padding: 1.4rem 1.5rem 1.2rem 1.5rem;
        margin-bottom: 1rem;
        box-shadow: 0 8px 24px rgba(0,0,0,0.22);
    }

    .eyebrow {
        color: #5ce1a8;
        font-size: 0.68rem;
        font-weight: 800;
        letter-spacing: 0.16em;
        text-transform: uppercase;
        margin-bottom: 0.35rem;
    }

    .hero-title {
        color: #ffffff;
        font-size: 2rem;
        font-weight: 800;
        line-height: 1.1;
        margin-bottom: 0.35rem;
    }

    .hero-subtitle {
        color: #c7d1db;
        font-size: 0.98rem;
        margin-bottom: 0;
    }

    .nav-row {
        display: flex;
        gap: 1rem;
        align-items: center;
        margin: 0.25rem 0 1rem 0;
        flex-wrap: wrap;
    }

    .nav-pill {
        color: #d8dee7;
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.07);
        border-radius: 999px;
        padding: 0.32rem 0.75rem;
        font-size: 0.82rem;
        font-weight: 600;
    }

    .nav-pill.active {
        background: #ffffff;
        color: #0b0f16;
        border-color: #ffffff;
    }

    .metric-card {
        background: linear-gradient(135deg, rgba(255,255,255,0.07) 0%, rgba(255,255,255,0.03) 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 18px;
        padding: 1rem 1rem 0.95rem 1rem;
        min-height: 126px;
        box-shadow: 0 8px 20px rgba(0,0,0,0.18);
    }

    .metric-label {
        color: #c4ccd6;
        font-size: 0.68rem;
        font-weight: 800;
        letter-spacing: 0.16em;
        text-transform: uppercase;
        margin-bottom: 0.45rem;
    }

    .metric-value {
        color: #ffffff;
        font-size: 2rem;
        font-weight: 800;
        line-height: 1.05;
        margin-bottom: 0.32rem;
    }

    .metric-help {
        color: #c7d1db;
        font-size: 0.88rem;
        line-height: 1.35;
    }

    .section-bar {
        margin-top: 1rem;
        margin-bottom: 0.85rem;
        height: 18px;
        border-radius: 999px;
        background: linear-gradient(90deg, rgba(255,255,255,0.10), rgba(255,255,255,0.04));
        border: 1px solid rgba(255,255,255,0.07);
    }

    .section-title {
        color: #ffffff;
        font-size: 1.02rem;
        font-weight: 800;
        margin-bottom: 0.2rem;
    }

    .section-subtitle {
        color: #b9c4cf;
        font-size: 0.85rem;
        margin-bottom: 0.9rem;
    }

    .info-banner {
        background: #dff3e8;
        color: #10231a;
        border-radius: 14px;
        padding: 0.8rem 1rem;
        border: 1px solid #c7e8d5;
        margin-bottom: 0.8rem;
        font-size: 0.94rem;
        font-weight: 600;
    }

    .stDataFrame, div[data-testid="stDataFrame"] {
        border-radius: 16px;
        overflow: hidden;
        border: 1px solid rgba(255,255,255,0.08);
    }

    div[data-testid="stMetric"] {
        background: transparent !important;
        border: none !important;
        box-shadow: none !important;
        padding: 0 !important;
    }

    .stPlotlyChart {
        background: linear-gradient(135deg, rgba(255,255,255,0.04), rgba(255,255,255,0.02));
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 18px;
        padding: 0.4rem 0.5rem 0.3rem 0.5rem;
    }

    .stSelectbox > div > div {
        background-color: #0f141b !important;
        color: white !important;
        border: 1px solid rgba(255,255,255,0.10) !important;
        border-radius: 10px !important;
    }

    .empty-box {
        background: linear-gradient(135deg, rgba(255,255,255,0.05), rgba(255,255,255,0.02));
        border: 1px dashed rgba(255,255,255,0.14);
        border-radius: 18px;
        padding: 1.4rem 1.2rem;
        color: #d1d8e0;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data(show_spinner=False, ttl=300)
def get_anomalies_data() -> pd.DataFrame:
    df = run_query(TOP_VOLATILE_QUERY)
    if df is None:
        return pd.DataFrame()
    return df


def safe_metric(value, default="-"):
    if value is None:
        return default
    if isinstance(value, float):
        return f"{value:.2f}"
    return value


df = get_anomalies_data()

st.markdown('<div class="app-shell">', unsafe_allow_html=True)

# --------------------------------------------------
# Hero
# --------------------------------------------------
st.markdown("""
<div class="hero-card">
    <div class="eyebrow">MIGROS • PRICE INTELLIGENCE</div>
    <div class="hero-title">Anomalies</div>
    <div class="hero-subtitle">Monitor volatile products, unusual price movements, and potential outliers in trusted price observations.</div>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="nav-row">
    <div class="nav-pill">Overview</div>
    <div class="nav-pill">Trend analysis</div>
    <div class="nav-pill">Top movers</div>
    <div class="nav-pill active">Anomalies</div>
    <div class="nav-pill">Pipeline health</div>
</div>
""", unsafe_allow_html=True)

# --------------------------------------------------
# Empty state
# --------------------------------------------------
if df.empty:
    st.markdown("""
    <div class="empty-box">
        <div class="section-title">No anomaly data available yet</div>
        <div class="section-subtitle">Run the pipeline and refresh marts to populate volatility analysis.</div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)
    st.stop()

# --------------------------------------------------
# Filters
# --------------------------------------------------
category_options = ["All"] + sorted(df["category_name"].dropna().astype(str).unique().tolist())
selected_category = st.selectbox("Category filter", category_options, index=0)

filtered_df = df.copy()
if selected_category != "All":
    filtered_df = filtered_df[filtered_df["category_name"] == selected_category]

if filtered_df.empty:
    st.markdown("""
    <div class="empty-box">
        <div class="section-title">No rows match this filter</div>
        <div class="section-subtitle">Try another category to inspect product volatility.</div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)
    st.stop()

# --------------------------------------------------
# Summary metrics
# --------------------------------------------------
top_row = filtered_df.sort_values("volatility", ascending=False).iloc[0]
volatile_count = len(filtered_df)
high_count = int((filtered_df["volatility_level"] == "high").sum()) if "volatility_level" in filtered_df.columns else 0
avg_volatility = float(filtered_df["volatility"].mean()) if "volatility" in filtered_df.columns else None

m1, m2, m3, m4 = st.columns(4)

with m1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Volatile Products</div>
        <div class="metric-value">{volatile_count}</div>
        <div class="metric-help">Products with enough observations for anomaly analysis</div>
    </div>
    """, unsafe_allow_html=True)

with m2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Highest Volatility</div>
        <div class="metric-value">₺{safe_metric(top_row.get("volatility"), "0.00")}</div>
        <div class="metric-help">{top_row.get("standardized_product_name", "-")}</div>
    </div>
    """, unsafe_allow_html=True)

with m3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Average Volatility</div>
        <div class="metric-value">₺{safe_metric(avg_volatility, "0.00")}</div>
        <div class="metric-help">Average standard deviation across filtered products</div>
    </div>
    """, unsafe_allow_html=True)

with m4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">High Risk Group</div>
        <div class="metric-value">{high_count}</div>
        <div class="metric-help">Products classified as high volatility</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown(
    f"""
    <div class="info-banner">
        Latest anomaly highlight: <strong>{top_row.get("standardized_product_name", "-")}</strong>
        in <strong>{top_row.get("category_name", "-")}</strong> with volatility level
        <strong>{top_row.get("volatility_level", "-")}</strong>.
    </div>
    """,
    unsafe_allow_html=True,
)

# --------------------------------------------------
# Section divider
# --------------------------------------------------
st.markdown('<div class="section-bar"></div>', unsafe_allow_html=True)

# --------------------------------------------------
# Charts
# --------------------------------------------------
left_col, right_col = st.columns([1.2, 1])

with left_col:
    st.markdown('<div class="section-title">Most Volatile Products</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Top products ranked by price volatility</div>', unsafe_allow_html=True)

    top_n = filtered_df.sort_values("volatility", ascending=False).head(10).copy()

    fig_bar = px.bar(
        top_n,
        x="volatility",
        y="standardized_product_name",
        color="volatility_level",
        orientation="h",
        hover_data=["avg_price_per_unit", "observation_count", "category_name"],
    )
    fig_bar.update_layout(
        height=430,
        plot_bgcolor="#0b0f16",
        paper_bgcolor="#0b0f16",
        font=dict(color="white"),
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis_title="Volatility",
        yaxis_title="",
        legend_title_text="Risk Level",
        yaxis=dict(categoryorder="total ascending"),
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with right_col:
    st.markdown('<div class="section-title">Volatility Distribution</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">How products are distributed across anomaly severity levels</div>', unsafe_allow_html=True)

    level_counts = (
        filtered_df["volatility_level"]
        .fillna("unknown")
        .value_counts()
        .reset_index()
    )
    level_counts.columns = ["volatility_level", "count"]

    fig_pie = px.pie(
        level_counts,
        names="volatility_level",
        values="count",
        hole=0.45,
    )
    fig_pie.update_layout(
        height=430,
        plot_bgcolor="#0b0f16",
        paper_bgcolor="#0b0f16",
        font=dict(color="white"),
        margin=dict(l=10, r=10, t=10, b=10),
        legend_title_text="Level",
    )
    st.plotly_chart(fig_pie, use_container_width=True)

# --------------------------------------------------
# Section divider
# --------------------------------------------------
st.markdown('<div class="section-bar"></div>', unsafe_allow_html=True)

# --------------------------------------------------
# Detail table
# --------------------------------------------------
st.markdown('<div class="section-title">Detailed Anomaly Table</div>', unsafe_allow_html=True)
st.markdown('<div class="section-subtitle">Analytics-ready anomaly output for deeper inspection and validation</div>', unsafe_allow_html=True)

display_df = filtered_df.sort_values("volatility", ascending=False).copy()

preferred_cols = [
    "standardized_product_name",
    "category_name",
    "avg_price_per_unit",
    "volatility",
    "observation_count",
    "volatility_level",
]

existing_cols = [col for col in preferred_cols if col in display_df.columns]
display_df = display_df[existing_cols]

st.dataframe(display_df, use_container_width=True, hide_index=True)

st.markdown("</div>", unsafe_allow_html=True)
