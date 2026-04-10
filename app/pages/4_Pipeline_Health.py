import streamlit as st
import plotly.express as px
import pandas as pd

from db import run_query
from queries import PIPELINE_HEALTH_QUERY, QUALITY_RESULTS_QUERY

st.set_page_config(page_title="Pipeline Health", layout="wide")

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

    .alert-banner {
        background: #fde8e8;
        color: #601818;
        border-radius: 14px;
        padding: 0.82rem 1rem;
        border: 1px solid #f5c8c8;
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

    .detail-card {
        background: linear-gradient(135deg, rgba(255,255,255,0.05), rgba(255,255,255,0.02));
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 18px;
        padding: 1rem 1rem 0.9rem 1rem;
        min-height: 100%;
    }

    .detail-label {
        color: #c4ccd6;
        font-size: 0.7rem;
        font-weight: 800;
        letter-spacing: 0.14em;
        text-transform: uppercase;
        margin-bottom: 0.35rem;
    }

    .detail-value {
        color: #ffffff;
        font-size: 1.1rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
    }

    .detail-text {
        color: #c7d1db;
        font-size: 0.9rem;
        line-height: 1.45;
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
</style>
""", unsafe_allow_html=True)

# --------------------------------------------------
# Helpers
# --------------------------------------------------
@st.cache_data(show_spinner=False, ttl=300)
def get_pipeline_data():
    try:
        health = run_query(PIPELINE_HEALTH_QUERY)
        quality = run_query(QUALITY_RESULTS_QUERY)

        if health is None:
            health = pd.DataFrame()
        if quality is None:
            quality = pd.DataFrame()

        return health, quality
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame(), pd.DataFrame()


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
health_df, quality_df = get_pipeline_data()

if health_df.empty:
    st.markdown("""
    <div class="hero-card">
        <div class="eyebrow">MIGROS • PRICE INTELLIGENCE</div>
        <div class="hero-title">Pipeline Health</div>
        <div class="hero-subtitle">No pipeline health data available yet.</div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

health_df = health_df.copy()

numeric_cols = [
    "run_id",
    "records_fact",
    "records_raw",
    "records_stg",
    "records_suspicious",
    "records_failed",
    "run_duration_seconds",
    "total_checks",
    "passed_checks",
    "failed_checks",
]
for col in numeric_cols:
    if col in health_df.columns:
        health_df[col] = pd.to_numeric(health_df[col], errors="coerce")

latest = health_df.iloc[0]

healthy_count = int((health_df["pipeline_health_status"] == "healthy").sum()) if "pipeline_health_status" in health_df.columns else 0
running_count = int((health_df["pipeline_health_status"] == "running").sum()) if "pipeline_health_status" in health_df.columns else 0
failed_count = int((health_df["pipeline_health_status"] == "failed").sum()) if "pipeline_health_status" in health_df.columns else 0
warning_count = int((health_df["pipeline_health_status"] == "warning").sum()) if "pipeline_health_status" in health_df.columns else 0

latest_run_id = latest.get("run_id")
latest_status = latest.get("status")
latest_fact = latest.get("records_fact")
latest_duration = latest.get("run_duration_seconds")
latest_failed_check = latest.get("last_failed_check_name")
latest_failed_details = latest.get("last_failed_check_details")

latest_snapshot = "Apr 10, 2026"

# --------------------------------------------------
# Hero
# --------------------------------------------------
st.markdown(f"""
<div class="hero-card">
    <div class="eyebrow">MIGROS • PRICE INTELLIGENCE</div>
    <div class="hero-title">Pipeline Health</div>
    <div class="hero-subtitle">
        Monitor ETL runs, quality checks, failure signals, and operational health across the pricing pipeline — {latest_snapshot}
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
        <div class="metric-label">Latest Run ID</div>
        <div class="metric-value">{int(latest_run_id) if pd.notna(latest_run_id) else '-'}</div>
        <div class="metric-help">Most recent tracked pipeline execution</div>
    </div>
    """, unsafe_allow_html=True)

with c2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Latest Status</div>
        <div class="metric-value">{latest_status if pd.notna(latest_status) else '-'}</div>
        <div class="metric-help">Current outcome of the latest run</div>
    </div>
    """, unsafe_allow_html=True)

with c3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Records Fact</div>
        <div class="metric-value">{int(latest_fact) if pd.notna(latest_fact) else '-'}</div>
        <div class="metric-help">Trusted rows loaded into fact layer</div>
    </div>
    """, unsafe_allow_html=True)

with c4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Run Duration (s)</div>
        <div class="metric-value">{fmt_num(latest_duration, 1)}</div>
        <div class="metric-help">Elapsed time for the latest completed run</div>
    </div>
    """, unsafe_allow_html=True)

c5, c6, c7, c8 = st.columns(4)

with c5:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Healthy Runs</div>
        <div class="metric-value">{healthy_count}</div>
        <div class="metric-help">Runs completed without quality issues</div>
    </div>
    """, unsafe_allow_html=True)

with c6:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Running Runs</div>
        <div class="metric-value">{running_count}</div>
        <div class="metric-help">Runs currently in progress</div>
    </div>
    """, unsafe_allow_html=True)

with c7:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Warning Runs</div>
        <div class="metric-value">{warning_count}</div>
        <div class="metric-help">Runs completed with quality warnings</div>
    </div>
    """, unsafe_allow_html=True)

with c8:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Failed Runs</div>
        <div class="metric-value">{failed_count}</div>
        <div class="metric-help">Runs marked as failed</div>
    </div>
    """, unsafe_allow_html=True)

# --------------------------------------------------
# Insight Banner
# --------------------------------------------------
if pd.notna(latest_failed_check) and str(latest_failed_check).strip():
    st.markdown(
        f"""
        <div class="alert-banner">
            Latest run issue detected: <strong>{latest_failed_check}</strong>.
            {latest_failed_details if pd.notna(latest_failed_details) and str(latest_failed_details).strip() else 'No extra details available.'}
        </div>
        """,
        unsafe_allow_html=True,
    )
else:
    passed_checks = latest.get("passed_checks")
    total_checks = latest.get("total_checks")
    st.markdown(
        f"""
        <div class="info-banner">
            Latest pipeline run <strong>#{int(latest_run_id) if pd.notna(latest_run_id) else '-'}</strong>
            completed with status <strong>{latest_status if pd.notna(latest_status) else '-'}</strong> —
            <strong>{int(passed_checks) if pd.notna(passed_checks) else 0}/{int(total_checks) if pd.notna(total_checks) else 0}</strong>
            checks passed.
        </div>
        """,
        unsafe_allow_html=True,
    )

# --------------------------------------------------
# Latest Run Details
# --------------------------------------------------
st.markdown('<div class="section-bar"></div>', unsafe_allow_html=True)

d1, d2 = st.columns([1.35, 1])

with d1:
    st.markdown('<div class="section-title">Latest Run Details</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Operational summary of the latest pipeline execution</div>', unsafe_allow_html=True)

    latest_cols = [
        col for col in [
            "run_id",
            "status",
            "pipeline_health_status",
            "records_raw",
            "records_stg",
            "records_fact",
            "records_suspicious",
            "records_failed",
            "run_duration_seconds",
            "total_checks",
            "passed_checks",
            "failed_checks",
        ] if col in health_df.columns
    ]

    latest_df = health_df.head(1)[latest_cols].copy()
    if "run_duration_seconds" in latest_df.columns:
        latest_df["run_duration_seconds"] = latest_df["run_duration_seconds"].map(lambda x: fmt_num(x, 2))
    st.dataframe(latest_df, use_container_width=True, hide_index=True)

with d2:
    st.markdown('<div class="section-title">Latest Failure Signal</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Most recent failed check and details</div>', unsafe_allow_html=True)

    if pd.notna(latest_failed_check) and str(latest_failed_check).strip():
        st.markdown(f"""
        <div class="detail-card">
            <div class="detail-label">Failed Check</div>
            <div class="detail-value">{latest_failed_check}</div>
            <div class="detail-text">
                {latest_failed_details if pd.notna(latest_failed_details) and str(latest_failed_details).strip() else "No details available."}
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="detail-card">
            <div class="detail-label">Failed Check</div>
            <div class="detail-value">None</div>
            <div class="detail-text">No failed checks for the latest run.</div>
        </div>
        """, unsafe_allow_html=True)

# --------------------------------------------------
# Charts
# --------------------------------------------------
st.markdown('<div class="section-bar"></div>', unsafe_allow_html=True)

left_col, right_col = st.columns(2)

with left_col:
    st.markdown('<div class="section-title">Pipeline Health Status Distribution</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Distribution of run health outcomes</div>', unsafe_allow_html=True)

    status_counts = health_df["pipeline_health_status"].fillna("unknown").value_counts().reset_index()
    status_counts.columns = ["pipeline_health_status", "count"]

    fig_pie = px.pie(
        status_counts,
        names="pipeline_health_status",
        values="count",
        hole=0.55,
        color="pipeline_health_status",
        color_discrete_map={
            "healthy": "#2bd67b",
            "running": "#4da3ff",
            "warning": "#ffb84d",
            "failed": "#ff5a67",
            "unknown": "#bfc7d5",
        },
    )
    fig_pie = build_dark_figure(fig_pie, height=450)
    st.plotly_chart(fig_pie, use_container_width=True)

with right_col:
    st.markdown('<div class="section-title">Run Duration Trend</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Elapsed seconds by run ID</div>', unsafe_allow_html=True)

    duration_df = health_df.dropna(subset=["run_duration_seconds"]).copy()

    if duration_df.empty:
        st.warning("No run duration data available.")
    else:
        duration_df = duration_df.sort_values("run_id")
        fig_line = px.line(
            duration_df,
            x="run_id",
            y="run_duration_seconds",
            markers=True,
        )
        fig_line = build_dark_figure(fig_line, height=450)
        fig_line.update_traces(line=dict(color="#7cb5ec", width=3))
        fig_line.update_layout(xaxis_title="Run ID", yaxis_title="Duration (s)", showlegend=False)
        st.plotly_chart(fig_line, use_container_width=True)

# --------------------------------------------------
# Tables
# --------------------------------------------------
st.markdown('<div class="section-bar"></div>', unsafe_allow_html=True)

t1, t2 = st.columns(2)

with t1:
    st.markdown('<div class="section-title">Recent Pipeline Runs</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Recent operational history of pipeline executions</div>', unsafe_allow_html=True)

    runs_df = health_df.copy()
    if "run_duration_seconds" in runs_df.columns:
        runs_df["run_duration_seconds"] = runs_df["run_duration_seconds"].map(lambda x: fmt_num(x, 2))
    st.dataframe(runs_df, use_container_width=True, hide_index=True)

with t2:
    st.markdown('<div class="section-title">Recent Data Quality Results</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Latest logged data quality outcomes</div>', unsafe_allow_html=True)

    if quality_df.empty:
        st.warning("No recent quality results available.")
    else:
        quality_display = quality_df.copy()
        st.dataframe(quality_display, use_container_width=True, hide_index=True)
