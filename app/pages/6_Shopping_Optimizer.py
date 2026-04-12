import os

import psycopg2
import streamlit as st

from pipeline.optimizer.engine import optimize_basket


st.set_page_config(page_title="Shopping Optimizer", layout="wide")

st.title("Shopping Optimizer")
st.caption("Enter a shopping list and compare the cheapest basket distribution across markets.")

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

            st.subheader("Entered Products")
            st.write(result["input"])

            st.subheader("Matched Products")
            st.dataframe(result["matched_products"], use_container_width=True)

            st.subheader("Basket Items")
            st.dataframe(result["basket"]["items"], use_container_width=True)

            st.subheader("Total Price")
            st.metric("Total Price", result["basket"]["total_price"])

        except Exception as exc:
            st.error(str(exc))

        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()
