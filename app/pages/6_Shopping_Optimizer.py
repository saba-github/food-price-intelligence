import os

import psycopg2
import streamlit as st

from pipeline.optimizer.engine import optimize_basket


st.set_page_config(page_title="Shopping Optimizer", layout="wide")

st.title("Shopping Optimizer")
st.caption("Enter a shopping list to compare split basket pricing against single market options.")

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
            conn = psycopg2.connect(database_url)
            cursor = conn.cursor()

            result = optimize_basket(cursor, user_inputs)
            split_basket = result["split_basket"]
            split_basket_total = split_basket["total_price"]
            single_market_options = result["single_market_options"]

            st.subheader("Entered Products")
            st.write(result["input"])

            st.subheader("Matched Products")
            st.dataframe(result["matched_products"], width="stretch")

            st.subheader("Split Basket Items")
            st.dataframe(split_basket["items"], width="stretch")

            st.subheader("Split Basket Total")
            st.metric("Split Basket Total", split_basket_total)

            st.subheader("Single Market Options")

            if single_market_options:
                st.dataframe(single_market_options, width="stretch")

                cheapest_single_market_total = min(
                    option["total_price"]
                    for option in single_market_options
                )
                savings = cheapest_single_market_total - split_basket_total

                st.metric("Savings vs Cheapest Single Market", savings)
            else:
                st.info("No single market can fulfill all matched products.")

        except Exception as exc:
            st.error(str(exc))

        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()
