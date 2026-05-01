import logging
from time import perf_counter

import pandas as pd
import streamlit as st

from database.connection import get_connection as get_shared_connection

logger = logging.getLogger(__name__)


def get_connection():
    connection_started_at = perf_counter()
    streamlit_database_url = None
    try:
        streamlit_database_url = st.secrets.get("DATABASE_URL")
    except Exception:
        streamlit_database_url = None

    conn = get_shared_connection(
        fallback_url=streamlit_database_url,
        application_name="streamlit-app",
    )
    logger.info(
        "db.get_connection completed in %.3fs",
        perf_counter() - connection_started_at,
    )
    return conn


def run_query(query: str, params=None) -> pd.DataFrame:
    query_started_at = perf_counter()
    conn = get_connection()
    try:
        query_df = pd.read_sql_query(query, conn, params=params)
        logger.info(
            "db.run_query completed in %.3fs rows=%s sql=%s",
            perf_counter() - query_started_at,
            len(query_df),
            " ".join(query.strip().split())[:120],
        )
        return query_df
    finally:
        conn.close()
