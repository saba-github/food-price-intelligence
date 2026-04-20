import streamlit as st
from db import run_query
from queries import GLOBAL_FRESHNESS_QUERY

st.set_page_config(page_title="Market Sepet Tasarrufu", layout="centered")

st.title("Market Sepet Tasarrufu")
st.subheader("Bu hafta alışverişinde ne kadar tasarruf edebileceğini gör")

freshness_df = run_query(GLOBAL_FRESHNESS_QUERY)
if not freshness_df.empty:
    row = freshness_df.iloc[0]
    latest_data_date = row.get("latest_data_date")
    latest_success_started_at = row.get("latest_success_started_at")

    st.info(
        f"Veri güncelliği: {latest_data_date} | "
        f"Son başarılı güncelleme: {latest_success_started_at}"
    )

st.markdown("### Nasıl çalışır?")
st.write(
    "Ürünlerini seç, marketler arasında karşılaştır, "
    "en ucuz sepet kombinasyonunu ve tahmini tasarrufunu gör."
)

st.markdown("### Hazır Sepetler")
c1, c2, c3 = st.columns(3)

with c1:
    if st.button("Kahvaltılık Sepet"):
        st.session_state["preset_basket"] = [
            "süt",
            "yumurta",
            "peynir",
            "ekmek",
            "domates",
        ]
        st.switch_page("pages/6_Shopping_Optimizer.py")

with c2:
    if st.button("Haftalık Sebze-Meyve"):
        st.session_state["preset_basket"] = [
            "domates",
            "salatalık",
            "muz",
            "elma",
            "limon",
        ]
        st.switch_page("pages/6_Shopping_Optimizer.py")

with c3:
    if st.button("Öğrenci Sepeti"):
        st.session_state["preset_basket"] = [
            "süt",
            "yoğurt",
            "ekmek",
            "muz",
        ]
        st.switch_page("pages/6_Shopping_Optimizer.py")

st.markdown("---")

if st.button("Sepetimi Oluştur", use_container_width=True):
    st.switch_page("pages/6_Shopping_Optimizer.py")
