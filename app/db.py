import os
import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        database_url = st.secrets.get("DATABASE_URL")

    if not database_url:
        raise ValueError("DATABASE_URL is not set in environment variables or Streamlit secrets.")

    return psycopg2.connect(database_url)


def run_query(query: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()
