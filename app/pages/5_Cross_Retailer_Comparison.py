import sys
from pathlib import Path

import streamlit as st
import pandas as pd
import plotly.express as px

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from pipeline.optimizer.measurement import (
    get_comparison_status_label,
    get_measurement_mismatch_label,
    format_measurement_label,
)

from db import run_query
from queries import (
    CROSS_RETAILER_PRODUCTS_QUERY,
    CROSS_RETAILER_COMPARISON_QUERY,
    CHEAPEST_RETAILER_TODAY_QUERY,
    GLOBAL_FRESHNESS_QUERY,
)

st.set_page_config(page_title="Marketler Arasi Karsilastirma", layout="wide")

st.title("Marketler Arasi Karsilastirma")
st.caption("Ayni urunu Migros ve A101 arasinda karsilastir.")


def add_measurement_display_fields(products_df: pd.DataFrame) -> pd.DataFrame:
    products_df = products_df.copy()
    products_df["a101_measurement_label"] = products_df.apply(
        lambda row: format_measurement_label(
            row.get("a101_normalized_quantity"),
            row.get("a101_normalized_unit"),
            row.get("a101_source_product_name"),
        ),
        axis=1,
    )
    products_df["migros_measurement_label"] = products_df.apply(
        lambda row: format_measurement_label(
            row.get("migros_normalized_quantity"),
            row.get("migros_normalized_unit"),
            row.get("migros_source_product_name"),
        ),
        axis=1,
    )
    products_df["comparison_status_label"] = products_df.apply(
        lambda row: get_comparison_status_label(row.to_dict()),
        axis=1,
    )
    products_df["measurement_mismatch_label"] = products_df.apply(
        lambda row: get_measurement_mismatch_label(row.to_dict()),
        axis=1,
    )
    return products_df


def format_product_option(product_name: str) -> str:
    row = products_df.loc[products_df["standardized_product_name"] == product_name]
    if row.empty:
        return product_name

    confidence = row.iloc[0].get("comparison_confidence")
    measurement_summary = (
        f"A101: {row.iloc[0].get('a101_measurement_label')} / "
        f"Migros: {row.iloc[0].get('migros_measurement_label')}"
    )
    status = row.iloc[0].get("comparison_status_label")
    if confidence == "high":
        return f"{product_name} ({status} | {measurement_summary})"

    return f"{product_name} ({status} | {measurement_summary})"


freshness_df = run_query(GLOBAL_FRESHNESS_QUERY)
if not freshness_df.empty:
    freshness_row = freshness_df.iloc[0]
    st.caption(
        f"Veri guncelligi: {freshness_row.get('latest_data_date')} | "
        f"Son basarili calisma: {freshness_row.get('latest_success_started_at')}"
    )

products_df = run_query(CROSS_RETAILER_PRODUCTS_QUERY)

if products_df.empty:
    st.warning("mart_cross_compare icinde urun verisi bulunamadi.")
    st.stop()

products_df = add_measurement_display_fields(products_df)
product_list = products_df["standardized_product_name"].dropna().tolist()

selected_product = st.selectbox(
    "Urun sec",
    product_list,
    format_func=format_product_option,
)

selected_safety = products_df.loc[
    products_df["standardized_product_name"] == selected_product
].iloc[0]
if selected_safety.get("comparison_confidence") == "high":
    st.caption(
        "Yuksek guvenli karsilastirma: son A101 ve Migros fiyatlari ayni birim "
        "ve miktari kullaniyor."
    )
else:
    st.warning(
        "Bu karsilastirma inceleme gerektiriyor; son A101 ve Migros fiyatlari "
        "ayni birim ve miktari paylasmiyor. Inceleme icin gosterilir, ancak "
        "en ucuz market siralamasindan cikarilir."
    )

measurement_df = pd.DataFrame(
    [
        {
            "A101 Olcu": selected_safety.get("a101_measurement_label"),
            "Migros Olcu": selected_safety.get("migros_measurement_label"),
            "Durum": selected_safety.get("comparison_status_label"),
            "Olcu Notu": selected_safety.get("measurement_mismatch_label") or "-",
        }
    ]
)
st.dataframe(measurement_df, use_container_width=True, hide_index=True)

comparison_df = run_query(
    CROSS_RETAILER_COMPARISON_QUERY,
    params=(selected_product,),
)

st.subheader(f"Fiyat trendi: {selected_product}")

if comparison_df.empty:
    st.info("Bu urun icin karsilastirma verisi yok.")
else:
    fig = px.line(
        comparison_df,
        x="date",
        y="avg_price",
        color="source_name",
        markers=True,
        title=f"Zamana gore ortalama fiyat - {selected_product}",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        comparison_df.sort_values(["date", "source_name"]),
        use_container_width=True,
    )

st.subheader("Son tarihte en ucuz market")
st.caption(
    "Bu tablo yalnizca yuksek guvenli, ayni birim ve ayni miktar "
    "karsilastirmalarini icerir."
)

cheapest_df = run_query(CHEAPEST_RETAILER_TODAY_QUERY)

if cheapest_df.empty:
    st.info("En ucuz market verisi yok.")
else:
    st.dataframe(cheapest_df, use_container_width=True)
