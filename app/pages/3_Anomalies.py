import streamlit as st
import plotly.express as px
import pandas as pd

from db import run_query
from queries import TOP_VOLATILE_QUERY, GLOBAL_FRESHNESS_QUERY

st.set_page_config(page_title="Anomalies", layout="wide")

# --------------------------------------------------
# CSS
# --------------------------------------------------
st.markdown("""
<style>
    .stApp {
        background-color: #070b11;
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

    .hero-card {
        background: linear-gradient(135deg, #10233c 0%, #0b1017 58%, #090d12 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 22px;
        padding: 1.3rem 1.5rem 1.15rem 1.5rem;
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
        color: #c8d2dd;
        font-size: 0.96rem;
        line-height: 1.45;
        margin-bottom: 0;
    }

    .metric-card {
        background: linear-gradient(135deg, rgba(255,255,255,0.08) 0%, rgba(255,255,255,0.03) 100%);
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
        margin-bottom: 0.3rem;
    }

    .metric-help {
        color: #c7d1db;
        font-size: 0.88rem;
        line-height: 1.35;
    }

    .info-banner {
        background: #dff1e8;
        color: #10231a;
        border-radius: 14px;
        padding: 0.82rem 1rem;
        border: 1px solid #c7e6d7;
        margin-bottom: 0.9rem;
        font-size: 0.94rem;
        font-weight: 600;
    }

    .section-bar {
        margin-top: 0.9rem;
        margin-bottom: 0.9rem;
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
</style>
""", unsafe_allow_html=True)

# --------------------------------------------------
# Helpers
# --------------------------------------------------
@st.cache_data(show_spinner=False, ttl=300)
def get_anomalies_data() -> pd.DataFrame:
    try:
        df = run_query(TOP_VOLATILE_QUERY)
        if df is None:
            return pd.DataFrame()
        return df
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()


def fmt_num(x, digits=2):
    if pd.isna(x):
        return "-"
    return f"{float(x):,.{digits}f}"


def build_dark_figure(fig, height=420):
    fig.update_layout(
        height=height,
        plot_bgcolor="#0b0f16",
        paper_bgcolor="#0b0f16",
        font=dict(color="white"),
        margin=dict(l=10, r=10, t=20, b=10),
        legend_title_text="",
    )
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(showgrid=False, zeroline=False)
    return fig

# --------------------------------------------------
# Data
# --------------------------------------------------
df = get_anomalies_data()
freshness_df = run_query(GLOBAL_FRESHNESS_QUERY)
freshness_text = None
if not freshness_df.empty:
    freshness_row = freshness_df.iloc[0]
    freshness_text = (
        f"Data freshness: {freshness_row.get('latest_data_date')} | "
        f"Last successful run: {freshness_row.get('latest_success_started_at')}"
    )

if df.empty:
    st.markdown("""
    <div class="hero-card">
        <div class="eyebrow">MIGROS • PRICE INTELLIGENCE</div>
        <div class="hero-title">Anomalies</div>
        <div class="hero-subtitle">No anomaly data available yet.</div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

df = df.copy()
df["volatility"] = pd.to_numeric(df["volatility"], errors="coerce")
df["avg_price_per_unit"] = pd.to_numeric(df["avg_price_per_unit"], errors="coerce")
df["observation_count"] = pd.to_numeric(df["observation_count"], errors="coerce")

# --------------------------------------------------
# Hero
# --------------------------------------------------
st.markdown("""
<div class="hero-card">
    <div class="eyebrow">MIGROS • PRICE INTELLIGENCE</div>
    <div class="hero-title">Anomalies</div>
    <div class="hero-subtitle">
        Detect unusually volatile products using price variability across trusted observations.
    </div>
</div>
""", unsafe_allow_html=True)

if freshness_text:
    st.caption(freshness_text)

# --------------------------------------------------
# Filter
# --------------------------------------------------
category_options = ["All"] + sorted(df["category_name"].dropna().astype(str).unique().tolist())
selected_category = st.selectbox("Category filter", category_options, index=0)

filtered_df = df.copy()
if selected_category != "All":
    filtered_df = filtered_df[filtered_df["category_name"] == selected_category]

if filtered_df.empty:
    st.warning("No rows match this filter.")
    st.stop()

top_row = filtered_df.sort_values("volatility", ascending=False).iloc[0]

# --------------------------------------------------
# KPI Cards
# --------------------------------------------------
c1, c2, c3, c4 = st.columns(4)

with c1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Volatile Products</div>
        <div class="metric-value">{len(filtered_df)}</div>
        <div class="metric-help">Products included in anomaly analysis</div>
    </div>
    """, unsafe_allow_html=True)

with c2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Highest Volatility</div>
        <div class="metric-value">₺{fmt_num(top_row["volatility"], 1)}</div>
        <div class="metric-help">{top_row["standardized_product_name"]}</div>
    </div>
    """, unsafe_allow_html=True)

with c3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Average Volatility</div>
        <div class="metric-value">₺{fmt_num(filtered_df["volatility"].mean(), 1)}</div>
        <div class="metric-help">Mean volatility across filtered products</div>
    </div>
    """, unsafe_allow_html=True)

with c4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">High Risk Group</div>
        <div class="metric-value">{int((filtered_df["volatility_level"] == "high").sum())}</div>
        <div class="metric-help">Products classified as high volatility</div>
    </div>
    """, unsafe_allow_html=True)

# --------------------------------------------------
# Insight Banner
# --------------------------------------------------
st.markdown(
    f"""
    <div class="info-banner">
        Most volatile product: <strong>{top_row["standardized_product_name"]}</strong>
        in <strong>{top_row["category_name"]}</strong> with
        <strong>{top_row["volatility_level"]}</strong> level and
        volatility score of <strong>₺{fmt_num(top_row["volatility"], 1)}</strong>.
    </div>
    """,
    unsafe_allow_html=True,
)

# --------------------------------------------------
# Charts
# --------------------------------------------------
st.markdown('<div class="section-bar"></div>', unsafe_allow_html=True)

left_col, right_col = st.columns([1.25, 1])

with left_col:
    st.markdown('<div class="section-title">Most Volatile Products</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Top products ranked by volatility score</div>', unsafe_allow_html=True)

    top10 = filtered_df.sort_values("volatility", ascending=False).head(10).copy()

    fig_bar = px.bar(
        top10.sort_values("volatility", ascending=True),
        x="volatility",
        y="standardized_product_name",
        color="volatility_level",
        orientation="h",
        hover_data=["avg_price_per_unit", "observation_count", "category_name"],
        color_discrete_map={
            "high": "#7cb5ec",
            "medium": "#2f7ed8",
            "low": "#f7a6a6",
        },
    )
    fig_bar = build_dark_figure(fig_bar, height=470)
    fig_bar.update_layout(xaxis_title="Volatility", yaxis_title="")
    st.plotly_chart(fig_bar, use_container_width=True)

with right_col:
    st.markdown('<div class="section-title">Volatility Level Distribution</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Breakdown of anomaly severity groups</div>', unsafe_allow_html=True)

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
        hole=0.55,
        color="volatility_level",
        color_discrete_map={
            "high": "#7cb5ec",
            "medium": "#2f7ed8",
            "low": "#f7a6a6",
            "unknown": "#bfc7d5",
        },
    )
    fig_pie = build_dark_figure(fig_pie, height=470)
    st.plotly_chart(fig_pie, use_container_width=True)

# --------------------------------------------------
# Table
# --------------------------------------------------
st.markdown('<div class="section-bar"></div>', unsafe_allow_html=True)
st.markdown('<div class="section-title">Detailed Anomaly Table</div>', unsafe_allow_html=True)
st.markdown('<div class="section-subtitle">Detailed volatility output for inspection and validation</div>', unsafe_allow_html=True)

display_df = filtered_df.sort_values("volatility", ascending=False).copy()
display_df = display_df[
    [
        "standardized_product_name",
        "category_name",
        "avg_price_per_unit",
        "volatility",
        "observation_count",
        "volatility_level",
    ]
].copy()

display_df["avg_price_per_unit"] = display_df["avg_price_per_unit"].map(lambda x: f"₺{fmt_num(x, 2)}")
display_df["volatility"] = display_df["volatility"].map(lambda x: f"₺{fmt_num(x, 2)}")

st.dataframe(display_df, use_container_width=True, hide_index=True)
