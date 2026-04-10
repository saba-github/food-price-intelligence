import streamlit as st
import plotly.express as px
import pandas as pd

from db import run_query
from queries import TOP_MOVERS_QUERY, TOP_DECLINERS_QUERY

st.set_page_config(page_title="Top Movers", layout="wide")

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

    .stDataFrame, div[data-testid="stDataFrame"] {
        border-radius: 16px;
        overflow: hidden;
        border: 1px solid rgba(255,255,255,0.08);
    }

    .stPlotlyChart {
        background: linear-gradient(135deg, rgba(255,255,255,0.04), rgba(255,255,255,0.02));
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 18px;
        padding: 0.35rem 0.45rem 0.2rem 0.45rem;
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

# --------------------------------------------------
# Helpers
# --------------------------------------------------
@st.cache_data(show_spinner=False, ttl=300)
def get_top_movers_data():
    try:
        movers = run_query(TOP_MOVERS_QUERY)
        decliners = run_query(TOP_DECLINERS_QUERY)

        if movers is None:
            movers = pd.DataFrame()
        if decliners is None:
            decliners = pd.DataFrame()

        return movers, decliners
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame(), pd.DataFrame()


def fmt_num(x, digits=2):
    if pd.isna(x):
        return "-"
    return f"{float(x):,.{digits}f}"


def fmt_pct(x, digits=1):
    if pd.isna(x):
        return "-"
    sign = "+" if float(x) > 0 else ""
    return f"{sign}{float(x):.{digits}f}%"


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
movers_df, decliners_df = get_top_movers_data()

if movers_df.empty and decliners_df.empty:
    st.markdown("""
    <div class="hero-card">
        <div class="eyebrow">MIGROS • PRICE INTELLIGENCE</div>
        <div class="hero-title">Top Movers</div>
        <div class="hero-subtitle">No price movement data available yet.</div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

if not movers_df.empty:
    movers_df = movers_df.copy()
    movers_df["pct_change"] = pd.to_numeric(movers_df["pct_change"], errors="coerce")
    movers_df["latest_price"] = pd.to_numeric(movers_df["latest_price"], errors="coerce")
    movers_df["previous_price"] = pd.to_numeric(movers_df["previous_price"], errors="coerce")

if not decliners_df.empty:
    decliners_df = decliners_df.copy()
    decliners_df["pct_change"] = pd.to_numeric(decliners_df["pct_change"], errors="coerce")
    decliners_df["latest_price"] = pd.to_numeric(decliners_df["latest_price"], errors="coerce")
    decliners_df["previous_price"] = pd.to_numeric(decliners_df["previous_price"], errors="coerce")

top_gainer = movers_df.sort_values("pct_change", ascending=False).iloc[0] if not movers_df.empty else None
top_loser = decliners_df.sort_values("pct_change", ascending=True).iloc[0] if not decliners_df.empty else None

largest_increase = top_gainer["pct_change"] if top_gainer is not None else None
largest_drop = top_loser["pct_change"] if top_loser is not None else None

total_movers = len(movers_df) + len(decliners_df)
latest_snapshot = "Apr 10, 2026"

# --------------------------------------------------
# Hero
# --------------------------------------------------
st.markdown(f"""
<div class="hero-card">
    <div class="eyebrow">MIGROS • PRICE INTELLIGENCE</div>
    <div class="hero-title">Top Movers</div>
    <div class="hero-subtitle">
        Track the strongest price increases and declines across products — {latest_snapshot}
    </div>
</div>
""", unsafe_allow_html=True)

# --------------------------------------------------
# KPI Cards
# --------------------------------------------------
c1, c2, c3, c4 = st.columns(4)

with c1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Tracked Movers</div>
        <div class="metric-value">{total_movers}</div>
        <div class="metric-help">Products with meaningful price movement</div>
    </div>
    """, unsafe_allow_html=True)

with c2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Top Increase</div>
        <div class="metric-value">{fmt_pct(largest_increase, 1)}</div>
        <div class="metric-help">{top_gainer['standardized_product_name'] if top_gainer is not None else '-'}</div>
    </div>
    """, unsafe_allow_html=True)

with c3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Top Drop</div>
        <div class="metric-value">{fmt_pct(largest_drop, 1)}</div>
        <div class="metric-help">{top_loser['standardized_product_name'] if top_loser is not None else '-'}</div>
    </div>
    """, unsafe_allow_html=True)

with c4:
    avg_abs_change = None
    pct_vals = []
    if not movers_df.empty:
        pct_vals.extend(movers_df["pct_change"].dropna().tolist())
    if not decliners_df.empty:
        pct_vals.extend([abs(x) for x in decliners_df["pct_change"].dropna().tolist()])
    if pct_vals:
        avg_abs_change = sum(pct_vals) / len(pct_vals)

    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Avg Absolute Change</div>
        <div class="metric-value">{fmt_pct(avg_abs_change, 1)}</div>
        <div class="metric-help">Average magnitude across top movers</div>
    </div>
    """, unsafe_allow_html=True)

# --------------------------------------------------
# Insight Banner
# --------------------------------------------------
if top_gainer is not None and top_loser is not None:
    st.markdown(
        f"""
        <div class="info-banner">
            Biggest increase: <strong>{top_gainer['standardized_product_name']}</strong>
            ({fmt_pct(top_gainer['pct_change'], 1)}), biggest drop:
            <strong>{top_loser['standardized_product_name']}</strong>
            ({fmt_pct(top_loser['pct_change'], 1)}).
        </div>
        """,
        unsafe_allow_html=True,
    )

# --------------------------------------------------
# Charts
# --------------------------------------------------
st.markdown('<div class="section-bar"></div>', unsafe_allow_html=True)

left_col, right_col = st.columns(2)

with left_col:
    st.markdown('<div class="section-title">Top Price Increases</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Products with the strongest positive price change</div>', unsafe_allow_html=True)

    if movers_df.empty:
        st.markdown("""
        <div class="empty-box">
            No increasing products available.
        </div>
        """, unsafe_allow_html=True)
    else:
        movers_top = movers_df.sort_values("pct_change", ascending=False).head(10).copy()

        fig_up = px.bar(
            movers_top.sort_values("pct_change", ascending=True),
            x="pct_change",
            y="standardized_product_name",
            orientation="h",
            hover_data=["latest_price", "previous_price", "category_name"],
            color_discrete_sequence=["#2bd67b"],
        )
        fig_up = build_dark_figure(fig_up, height=470)
        fig_up.update_layout(xaxis_title="% Change", yaxis_title="", showlegend=False)
        st.plotly_chart(fig_up, use_container_width=True)

with right_col:
    st.markdown('<div class="section-title">Top Price Drops</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Products with the strongest negative price change</div>', unsafe_allow_html=True)

    if decliners_df.empty:
        st.markdown("""
        <div class="empty-box">
            No declining products available.
        </div>
        """, unsafe_allow_html=True)
    else:
        decliners_top = decliners_df.sort_values("pct_change", ascending=True).head(10).copy()

        fig_down = px.bar(
            decliners_top.sort_values("pct_change", ascending=False),
            x="pct_change",
            y="standardized_product_name",
            orientation="h",
            hover_data=["latest_price", "previous_price", "category_name"],
            color_discrete_sequence=["#ff5a67"],
        )
        fig_down = build_dark_figure(fig_down, height=470)
        fig_down.update_layout(xaxis_title="% Change", yaxis_title="", showlegend=False)
        st.plotly_chart(fig_down, use_container_width=True)

# --------------------------------------------------
# Tables
# --------------------------------------------------
st.markdown('<div class="section-bar"></div>', unsafe_allow_html=True)

t1, t2 = st.columns(2)

with t1:
    st.markdown('<div class="section-title">Increase Details</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Detailed output for top gainers</div>', unsafe_allow_html=True)

    if not movers_df.empty:
        movers_table = movers_df.sort_values("pct_change", ascending=False).copy()
        movers_table = movers_table[
            ["standardized_product_name", "category_name", "previous_price", "latest_price", "pct_change"]
        ].copy()
        movers_table["previous_price"] = movers_table["previous_price"].map(lambda x: f"₺{fmt_num(x, 2)}")
        movers_table["latest_price"] = movers_table["latest_price"].map(lambda x: f"₺{fmt_num(x, 2)}")
        movers_table["pct_change"] = movers_table["pct_change"].map(lambda x: fmt_pct(x, 2))
        st.dataframe(movers_table, use_container_width=True, hide_index=True)

with t2:
    st.markdown('<div class="section-title">Decline Details</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Detailed output for top decliners</div>', unsafe_allow_html=True)

    if not decliners_df.empty:
        decliners_table = decliners_df.sort_values("pct_change", ascending=True).copy()
        decliners_table = decliners_table[
            ["standardized_product_name", "category_name", "previous_price", "latest_price", "pct_change"]
        ].copy()
        decliners_table["previous_price"] = decliners_table["previous_price"].map(lambda x: f"₺{fmt_num(x, 2)}")
        decliners_table["latest_price"] = decliners_table["latest_price"].map(lambda x: f"₺{fmt_num(x, 2)}")
        decliners_table["pct_change"] = decliners_table["pct_change"].map(lambda x: fmt_pct(x, 2))
        st.dataframe(decliners_table, use_container_width=True, hide_index=True)
