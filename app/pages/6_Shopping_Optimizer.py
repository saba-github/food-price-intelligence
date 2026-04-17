import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from pipeline.optimizer.engine import optimize_basket
from db import run_query
from queries import GLOBAL_FRESHNESS_QUERY


st.set_page_config(page_title="Shopping Optimizer", layout="wide")

st.title("Shopping Optimizer")
st.caption("Enter a shopping list to compare split basket pricing against single market options.")

freshness_df = run_query(GLOBAL_FRESHNESS_QUERY)
if not freshness_df.empty:
    freshness_row = freshness_df.iloc[0]
    st.caption(
        f"Data freshness: {freshness_row.get('latest_data_date')} | "
        f"Last successful run: {freshness_row.get('latest_success_started_at')}"
    )

user_text = st.text_area(
    "Shopping List",
    placeholder="domates\nmuz\nlimon",
    height=180,
)

if st.button("Optimize Basket"):
    user_inputs = [
        line.strip()
        for line in user_text.splitlines()
        if line.strip()
    ]

    if not user_inputs:
        st.warning("Please enter at least one product.")
    else:
        conn = None
        cursor = None

        try:
            database_url = os.getenv("DATABASE_URL")

            if not database_url:
                database_url = st.secrets.get("DATABASE_URL")

            if not database_url:
                raise ValueError("DATABASE_URL is not set.")

            conn = psycopg2.connect(database_url)
            cursor = conn.cursor()

            result = optimize_basket(cursor, user_inputs)
            split_basket = result["split_basket"]
            split_basket_total = split_basket["total_price"]
            single_market_options = result["single_market_options"]

            st.subheader("Entered Products")
            st.write(result["input"])

            st.subheader("Matched Products")
            matched_df = pd.DataFrame(result["matched_products"])

            if not matched_df.empty and "found" in matched_df.columns:
                missing_df = matched_df[matched_df["found"] == False]
                if not missing_df.empty:
                    missing_inputs = ", ".join(missing_df["input"].astype(str).tolist())
                    st.warning(f"Could not match: {missing_inputs}")

            st.dataframe(matched_df, use_container_width=True)

            st.subheader("Split Basket Items")
            st.dataframe(pd.DataFrame(split_basket["items"]), use_container_width=True)

            st.subheader("Split Basket Total")
            st.metric("Split Basket Total", f"₺{split_basket_total:,.2f}")

            st.subheader("Single Market Options")

            if single_market_options:
                st.dataframe(pd.DataFrame(single_market_options), use_container_width=True)

                cheapest_single_market_total = min(
                    option["total_price"]
                    for option in single_market_options
                )
                savings = cheapest_single_market_total - split_basket_total

                st.metric("Savings vs Cheapest Single Market", f"₺{savings:,.2f}")
            else:
                st.info("No single market can fulfill all matched products.")

        except Exception as exc:
            st.error(str(exc))

        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()
