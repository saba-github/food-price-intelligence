import os
import sys
from pathlib import Path
from collections import defaultdict

import pandas as pd
import psycopg2
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from pipeline.optimizer.engine import optimize_basket
from db import run_query
from queries import GLOBAL_FRESHNESS_QUERY


st.set_page_config(page_title="Sepet Tasarrufu", layout="centered")

st.title("Sepet Tasarrufu")
st.caption("Ürünlerini ekle, en ucuz market kombinasyonunu ve tahmini tasarrufunu gör.")

freshness_df = run_query(GLOBAL_FRESHNESS_QUERY)
if not freshness_df.empty:
    freshness_row = freshness_df.iloc[0]
    st.info(
        f"Veri güncelliği: {freshness_row.get('latest_data_date')} | "
        f"Son başarılı güncelleme: {freshness_row.get('latest_success_started_at')}"
    )

st.markdown("### Hazır Sepetler")
b1, b2, b3 = st.columns(3)

with b1:
    if st.button("Kahvaltılık"):
        st.session_state["preset_basket"] = ["süt", "yumurta", "peynir", "ekmek", "domates"]
        st.rerun()

with b2:
    if st.button("Sebze-Meyve"):
        st.session_state["preset_basket"] = ["domates", "salatalık", "muz", "elma", "limon"]
        st.rerun()

with b3:
    if st.button("Öğrenci"):
        st.session_state["preset_basket"] = ["süt", "yoğurt", "ekmek", "muz"]
        st.rerun()

preset_items = st.session_state.get("preset_basket", [])
default_text = "\n".join(preset_items) if preset_items else ""

user_text = st.text_area(
    "Alışveriş Listen",
    value=default_text,
    placeholder="domates\nyumurta\nsüt",
    height=180,
)

if st.button("Sepeti Hesapla", use_container_width=True):
    user_inputs = [line.strip() for line in user_text.splitlines() if line.strip()]

    if not user_inputs:
        st.warning("Lütfen en az bir ürün gir.")
    else:
        conn = None
        cursor = None

        try:
            database_url = os.getenv("DATABASE_URL")

            if not database_url:
                database_url = st.secrets.get("DATABASE_URL")

            if not database_url:
                raise ValueError("DATABASE_URL ayarlı değil.")

            conn = psycopg2.connect(database_url)
            cursor = conn.cursor()

            result = optimize_basket(cursor, user_inputs)

            matched_df = pd.DataFrame(result["matched_products"])
            split_basket = result["split_basket"]
            split_basket_total = split_basket["total_price"]
            single_market_options = result["single_market_options"]

            found_count = int(matched_df["found"].sum()) if not matched_df.empty and "found" in matched_df.columns else 0
            missing_count = len(matched_df) - found_count if not matched_df.empty else 0

            cheapest_single_market_total = None
            savings = 0

            if single_market_options:
                cheapest_single_market_total = min(
                    option["total_price"] for option in single_market_options
                )
                savings = cheapest_single_market_total - split_basket_total

            st.markdown("---")
            st.markdown("## Sonuç")

            c1, c2, c3 = st.columns(3)

            with c1:
                st.metric("Bölünmüş Sepet", f"₺{split_basket_total:,.2f}")

            with c2:
                if cheapest_single_market_total is not None:
                    st.metric("En Ucuz Tek Market", f"₺{cheapest_single_market_total:,.2f}")
                else:
                    st.metric("En Ucuz Tek Market", "-")

            with c3:
                st.metric("Tahmini Tasarruf", f"₺{savings:,.2f}")

            st.info(f"{len(user_inputs)} ürünün {found_count} tanesi eşleşti.")

            if missing_count > 0:
                st.warning(
                    f"{missing_count} ürün eşleşmedi. "
                    "Bu ürünler tasarruf hesabına dahil edilmedi."
                )

            st.markdown("### Girilen Ürünler")
            st.write(", ".join(user_inputs))

            if not matched_df.empty and "found" in matched_df.columns:
                missing_df = matched_df[matched_df["found"] == False]
                if not missing_df.empty:
                    st.markdown("### Bulunamayan Ürünler")
                    st.dataframe(
                        missing_df[["input"]],
                        use_container_width=True,
                        hide_index=True,
                    )

            st.markdown("### Bölünmüş Sepet Detayı")
            split_items = split_basket.get("items", [])

            if split_items:
                grouped = defaultdict(list)
                for item in split_items:
                    grouped[item["market"]].append(item)

                for market, items in grouped.items():
                    st.markdown(f"#### {market.upper()}")
                    market_rows = []
                    for item in items:
                        market_rows.append(
                            {
                                "product_id": item.get("product_id"),
                                "selected_price": item.get("selected_price"),
                                "price_per_unit": item.get("price_per_unit"),
                            }
                        )
                    st.dataframe(
                        pd.DataFrame(market_rows),
                        use_container_width=True,
                        hide_index=True,
                    )
            else:
                st.info("Bölünmüş sepet için yeterli veri bulunamadı.")

            st.markdown("### Tek Market Alternatifleri")
            if single_market_options:
                st.dataframe(
                    pd.DataFrame(single_market_options),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info("Hiçbir tek market tüm eşleşen ürünleri karşılamıyor.")

            st.markdown("### Eşleşme Detayı")
            st.dataframe(matched_df, use_container_width=True, hide_index=True)

        except Exception as exc:
            st.error(f"Hata: {exc}")

        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()
