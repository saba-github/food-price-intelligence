import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from pipeline.optimizer.engine import optimize_basket
from pipeline.optimizer.measurement import (
    get_comparison_status_label,
    get_measurement_mismatch_label,
    format_measurement_label,
)
from pipeline.optimizer.product_search import search_product_catalog
from pipeline.optimizer.product_search import (
    build_optimizer_input_from_group,
    build_product_family_groups,
    format_product_family_group,
)
from db import get_connection, run_query
from queries import GLOBAL_FRESHNESS_QUERY


PRODUCT_CATALOG_QUERY = """
WITH latest AS (
    SELECT
        standardized_product_name,
        source_name,
        source_product_name,
        normalized_unit,
        normalized_quantity,
        ROW_NUMBER() OVER (
            PARTITION BY standardized_product_name, source_name
            ORDER BY observed_at DESC, price_observation_id DESC
        ) AS rn
    FROM price_history
    WHERE standardized_product_name IS NOT NULL
      AND price IS NOT NULL
),
coverage AS (
    SELECT
        standardized_product_name,
        COUNT(DISTINCT source_name) AS source_count,
        STRING_AGG(DISTINCT source_name, ', ' ORDER BY source_name) AS available_retailers,
        MAX(CASE WHEN source_name = 'a101' THEN source_product_name END) AS a101_source_product_name,
        MAX(CASE WHEN source_name = 'migros' THEN source_product_name END) AS migros_source_product_name,
        MAX(CASE WHEN source_name = 'a101' THEN normalized_unit END) AS a101_normalized_unit,
        MAX(CASE WHEN source_name = 'migros' THEN normalized_unit END) AS migros_normalized_unit,
        MAX(CASE WHEN source_name = 'a101' THEN normalized_quantity END) AS a101_normalized_quantity,
        MAX(CASE WHEN source_name = 'migros' THEN normalized_quantity END) AS migros_normalized_quantity
    FROM latest
    WHERE rn = 1
    GROUP BY standardized_product_name
),
safety AS (
    SELECT
        standardized_product_name,
        (
            COUNT(DISTINCT source_name) = 2
            AND MAX(CASE WHEN source_name = 'a101' THEN normalized_unit END) IS NOT NULL
            AND MAX(CASE WHEN source_name = 'migros' THEN normalized_unit END) IS NOT NULL
            AND MAX(CASE WHEN source_name = 'a101' THEN normalized_unit END)
                = MAX(CASE WHEN source_name = 'migros' THEN normalized_unit END)
        ) AS same_unit_flag,
        (
            COUNT(DISTINCT source_name) = 2
            AND MAX(CASE WHEN source_name = 'a101' THEN normalized_quantity END) IS NOT NULL
            AND MAX(CASE WHEN source_name = 'migros' THEN normalized_quantity END) IS NOT NULL
            AND MAX(CASE WHEN source_name = 'a101' THEN normalized_quantity END)
                = MAX(CASE WHEN source_name = 'migros' THEN normalized_quantity END)
        ) AS same_quantity_flag
    FROM latest
    WHERE rn = 1
    GROUP BY standardized_product_name
)
SELECT
    c.standardized_product_name,
    c.source_count,
    c.available_retailers,
    c.a101_source_product_name,
    c.migros_source_product_name,
    c.a101_normalized_unit,
    c.migros_normalized_unit,
    c.a101_normalized_quantity,
    c.migros_normalized_quantity,
    s.same_unit_flag,
    s.same_quantity_flag,
    CASE
        WHEN c.source_count < 2 THEN 'single_source'
        WHEN s.same_unit_flag AND s.same_quantity_flag THEN 'high'
        WHEN s.same_unit_flag THEN 'medium'
        ELSE 'low'
    END AS comparison_confidence,
    CASE
        WHEN c.source_count = 2 AND s.same_unit_flag AND s.same_quantity_flag THEN 'comparable'
        WHEN c.source_count = 2 THEN 'comparison_review_required'
        WHEN c.source_count = 1 AND c.available_retailers = 'a101' THEN 'only_a101'
        WHEN c.source_count = 1 AND c.available_retailers = 'migros' THEN 'only_migros'
        ELSE 'unavailable'
    END AS coverage_status
FROM coverage c
LEFT JOIN safety s
    ON c.standardized_product_name = s.standardized_product_name
ORDER BY standardized_product_name;
"""

COVERAGE_LABELS = {
    "comparable": "Karsilastirilabilir",
    "comparison_review_required": "Inceleme gerekli",
    "only_a101": "Sadece A101",
    "only_migros": "Sadece Migros",
    "only_available_at_a101": "Sadece A101",
    "only_available_at_migros": "Sadece Migros",
    "unavailable": "Uygun degil",
}
RETAILER_LABELS = {
    "a101": "A101",
    "migros": "Migros",
}


def format_metric_value(value):
    if value is None:
        return "-"

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(value, date):
        return value.isoformat()

    try:
        if pd.isna(value):
            return "-"
    except (TypeError, ValueError):
        pass

    if isinstance(value, (int, float, str)):
        return value

    return str(value)


def get_catalog_subset(catalog_df: pd.DataFrame, filter_key: str) -> pd.DataFrame:
    if filter_key == "comparable":
        return catalog_df[catalog_df["coverage_status"] == "comparable"]

    if filter_key == "needs_review":
        return catalog_df[
            catalog_df["coverage_status"] == "comparison_review_required"
        ]

    if filter_key == "only_a101":
        return catalog_df[catalog_df["coverage_status"] == "only_a101"]

    if filter_key == "only_migros":
        return catalog_df[catalog_df["coverage_status"] == "only_migros"]

    return catalog_df


def format_coverage(value) -> str:
    return COVERAGE_LABELS.get(str(value), str(value))


def add_catalog_display_fields(catalog_df: pd.DataFrame) -> pd.DataFrame:
    catalog_df = catalog_df.copy()
    catalog_df["a101_measurement_label"] = catalog_df.apply(
        lambda row: format_measurement_label(
            row.get("a101_normalized_quantity"),
            row.get("a101_normalized_unit"),
            row.get("a101_source_product_name"),
        ),
        axis=1,
    )
    catalog_df["migros_measurement_label"] = catalog_df.apply(
        lambda row: format_measurement_label(
            row.get("migros_normalized_quantity"),
            row.get("migros_normalized_unit"),
            row.get("migros_source_product_name"),
        ),
        axis=1,
    )
    catalog_df["comparison_status_label"] = catalog_df.apply(
        lambda row: get_comparison_status_label(row.to_dict()),
        axis=1,
    )
    return catalog_df


def format_measurement_summary(row) -> str:
    parts = []
    if row.get("a101_measurement_label") != "-":
        parts.append(f"A101: {row.get('a101_measurement_label')}")
    if row.get("migros_measurement_label") != "-":
        parts.append(f"Migros: {row.get('migros_measurement_label')}")
    return " / ".join(parts)


def selected_measurement_label(row: dict) -> str:
    retailer = row.get("recommended_retailer") or row.get("market")
    if retailer == "a101":
        return row.get("a101_measurement_label") or "-"
    if retailer == "migros":
        return row.get("migros_measurement_label") or "-"
    return "-"


def display_basket_item_name(item: dict) -> str:
    family_label = item.get("family_label")
    selected_product = (
        item.get("selected_family_product_name")
        or item.get("standardized_product_name")
        or item.get("product_name")
    )
    if family_label and selected_product:
        return f"{family_label} -> {selected_product}"
    if selected_product:
        return selected_product
    return ""


st.set_page_config(page_title="Sepet Optimizasyonu", layout="wide")

st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1.5rem;
        padding-left: 1.5rem;
        padding-right: 1.5rem;
        max-width: 1180px;
    }
    div[data-testid="stMetric"] {
        border: 1px solid rgba(120, 130, 145, 0.36);
        border-radius: 8px;
        padding: 14px 16px;
        background: var(--secondary-background-color);
        color: var(--text-color);
        box-shadow: 0 1px 2px rgba(15, 23, 42, 0.08);
    }
    div[data-testid="stMetricLabel"] p {
        color: var(--text-color);
        opacity: 0.72;
    }
    div[data-testid="stMetricValue"] {
        color: var(--text-color);
    }
    div.stButton > button[kind="primary"],
    button[data-testid="stBaseButton-primary"] {
        background: #0f766e;
        border-color: #0f766e;
        color: #ffffff;
    }
    div.stButton > button[kind="primary"]:hover,
    button[data-testid="stBaseButton-primary"]:hover {
        background: #115e59;
        border-color: #115e59;
        color: #ffffff;
    }
    div.stButton > button[kind="primary"]:focus,
    button[data-testid="stBaseButton-primary"]:focus {
        box-shadow: 0 0 0 0.2rem rgba(15, 118, 110, 0.25);
    }
    div.stDownloadButton > button {
        border-color: rgba(15, 118, 110, 0.45);
        color: var(--text-color);
    }
    .savings-hero {
        margin: 1.25rem 0 1rem 0;
        padding: 1.35rem 1.5rem;
        border: 1px solid rgba(15, 118, 110, 0.34);
        border-radius: 8px;
        background:
            linear-gradient(135deg, rgba(15, 118, 110, 0.16), rgba(22, 163, 74, 0.10)),
            var(--secondary-background-color);
        color: var(--text-color);
    }
    .savings-hero__eyebrow {
        margin: 0 0 0.35rem 0;
        color: var(--text-color);
        opacity: 0.72;
        font-size: 0.88rem;
        font-weight: 600;
        text-transform: uppercase;
    }
    .savings-hero__value {
        margin: 0;
        color: var(--text-color);
        font-size: 2.5rem;
        line-height: 1.1;
        font-weight: 760;
    }
    .savings-hero__detail {
        margin: 0.55rem 0 0 0;
        color: var(--text-color);
        opacity: 0.78;
        font-size: 1rem;
    }
    @media (max-width: 700px) {
        .block-container {
            padding-top: 1rem;
            padding-left: 0.85rem;
            padding-right: 0.85rem;
        }
        .savings-hero {
            padding: 1rem;
        }
        .savings-hero__value {
            font-size: 2rem;
        }
        div[data-testid="column"] {
            min-width: 100%;
        }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

header_left, header_right = st.columns([2, 1])
with header_left:
    st.title("Sepet Optimizasyonu")
    st.caption("Standart urunlerden sepet olustur ve market kapsamini karsilastir.")

with header_right:
    freshness_df = run_query(GLOBAL_FRESHNESS_QUERY)
    if not freshness_df.empty:
        freshness_row = freshness_df.iloc[0]
        st.metric(
            "Son Fiyat Tarihi",
            format_metric_value(freshness_row.get("latest_data_date")),
        )


@st.cache_data(ttl=300)
def load_product_catalog() -> pd.DataFrame:
    return run_query(PRODUCT_CATALOG_QUERY)


def format_money(value) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):,.2f} TL"
    except (TypeError, ValueError):
        return "-"


def format_retailer(value) -> str:
    return RETAILER_LABELS.get(str(value), str(value))


def format_retailer_case(value) -> str:
    retailer = format_retailer(value)
    if retailer == "A101":
        return "A101'e"
    if retailer == "Migros":
        return "Migros'a"
    return f"{retailer}'e"


def get_next_best_single_market(single_market_options: list[dict]) -> dict | None:
    if len(single_market_options) < 2:
        return None
    return single_market_options[1]


def build_savings_display(
    savings_amount,
    single_market_options: list[dict],
) -> dict:
    if savings_amount is not None and savings_amount > 0:
        return {
            "label": "Ek Tasarruf",
            "amount": savings_amount,
            "headline": f"Ek Tasarruf: {format_money(savings_amount)}",
        }

    if single_market_options:
        best_market = single_market_options[0]
        next_best_market = get_next_best_single_market(single_market_options)
        if next_best_market is not None:
            advantage = (
                next_best_market["total_price"]
                - best_market["total_price"]
            )
            return {
                "label": "En iyi seçim avantajı",
                "amount": advantage,
                "headline": (
                    f"En iyi seçim avantajı: {format_money(advantage)}"
                ),
            }

    return {
        "label": "Sepet avantajı",
        "amount": savings_amount,
        "headline": f"Sepet avantajı: {format_money(savings_amount)}",
    }


def build_single_product_messages(recommendations: list[dict]) -> list[str]:
    if len(recommendations) != 1:
        return []

    recommendation = recommendations[0]
    retailer = recommendation.get("recommended_retailer")
    recommended_price = recommendation.get("recommended_price")
    if not retailer or recommended_price is None:
        return []

    other_retailer = "migros" if retailer == "a101" else "a101"
    other_price = recommendation.get(f"{other_retailer}_price")
    if other_price is None:
        return [f"En ucuz mağaza: {format_retailer(retailer)}"]

    advantage = other_price - recommended_price
    return [
        f"En ucuz mağaza: {format_retailer(retailer)}",
        f"{format_retailer_case(other_retailer)} göre avantaj: {format_money(advantage)}",
    ]


def build_export_rows(items):
    rows = []
    for item in items:
        rows.append(
            {
                "Urun": display_basket_item_name(item),
                "Secilen Secenek": item.get("selected_family_product_name", ""),
                "Mağaza": item.get("recommended_retailer", ""),
                "Fiyat": item.get("selected_price"),
                "Olcu": selected_measurement_label(item),
                "Kapsam": item.get("coverage_status", ""),
            }
        )
    return rows


def build_shopping_list_text(
    items,
    total_price,
    advantage_label,
    advantage_amount,
) -> str:
    lines = [
        "Sepet Optimizasyonu Listesi",
        f"Karma sepet toplami: {format_money(total_price)}",
        f"{advantage_label}: {format_money(advantage_amount)}",
        "",
    ]

    rows = build_export_rows(items)
    retailers = sorted({row["Mağaza"] or "bilinmiyor" for row in rows})
    for retailer in retailers:
        lines.append(str(retailer).upper())
        for row in rows:
            if (row["Mağaza"] or "bilinmiyor") != retailer:
                continue
            lines.append(
                f"- {row['Urun']} | {format_money(row['Fiyat'])} | {row['Olcu']} | {row['Kapsam']}"
            )
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def format_product_option(product_name: str, catalog_df: pd.DataFrame) -> str:
    row = catalog_df.loc[catalog_df["standardized_product_name"] == product_name]
    if row.empty:
        return product_name

    coverage = format_coverage(row.iloc[0]["coverage_status"])
    measurement_summary = format_measurement_summary(row.iloc[0])
    status = row.iloc[0].get("comparison_status_label")
    details = " | ".join(
        detail for detail in [coverage, status, measurement_summary] if detail
    )
    return f"{product_name} ({details})"


def merge_selection_group(existing_group: dict, incoming_group: dict) -> dict:
    product_names = list(existing_group.get("product_names") or [])
    for product_name in incoming_group.get("product_names") or []:
        if product_name not in product_names:
            product_names.append(product_name)

    return {
        **existing_group,
        "product_names": product_names,
    }


def format_selection_option(
    selection_id: str,
    group_lookup: dict[str, dict],
    catalog_df: pd.DataFrame,
) -> str:
    group = group_lookup.get(selection_id)
    if not group:
        return selection_id

    if group.get("selection_type") == "product_family":
        return format_product_family_group(group)

    product_names = group.get("product_names") or []
    if not product_names:
        return selection_id

    return format_product_option(product_names[0], catalog_df)


catalog_df = load_product_catalog()

if catalog_df.empty:
    st.warning("price_history icinde fiyatlanmis urun bulunamadi.")
    st.stop()

catalog_df = add_catalog_display_fields(catalog_df)
catalog_df["coverage"] = catalog_df["coverage_status"].apply(format_coverage)

st.markdown("### Sepeti Olustur")
selection_panel, summary_panel = st.columns([2, 1])

filter_options = [
    ("Tumu", "all", "Fiyatlanmis tum urunler"),
    ("Karsilastirilabilir", "comparable", "Iki markette de ayni birim ve miktarda olan urunler"),
    ("Inceleme Gerekli", "needs_review", "Iki markette var ama birim veya miktar farkli"),
    ("Sadece A101", "only_a101", "Su anda yalnizca A101 tarafinda olan urunler"),
    ("Sadece Migros", "only_migros", "Su anda yalnizca Migros tarafinda olan urunler"),
]
filter_counts = {
    filter_key: len(get_catalog_subset(catalog_df, filter_key))
    for _, filter_key, _ in filter_options
}
selection_keys = [
    f"selected_basket_products_{filter_key}"
    for _, filter_key, _ in filter_options
]
has_filter_state = any(key in st.session_state for key in selection_keys)
legacy_selected_products = (
    st.session_state.get("selected_basket_products", [])
    if not has_filter_state
    else []
)

with selection_panel:
    search_text = st.text_input(
        "Urun ara",
        placeholder="domates, avokado, limon gibi bir urun yaz",
    ).strip().lower()

    all_groups = build_product_family_groups(catalog_df)
    all_group_lookup = {
        group["selection_id"]: group
        for group in all_groups
    }
    product_to_selection_id = {
        product_name: group["selection_id"]
        for group in all_groups
        for product_name in group.get("product_names", [])
    }
    all_selection_ids = set(all_group_lookup)
    filter_tabs = st.tabs(
        [
            f"{label} ({filter_counts[filter_key]})"
            for label, filter_key, _ in filter_options
        ]
    )
    selected_by_filter = []
    selected_group_lookup = {}

    for tab, (label, filter_key, description) in zip(filter_tabs, filter_options):
        with tab:
            filtered_df = get_catalog_subset(catalog_df, filter_key)
            filtered_df = search_product_catalog(filtered_df, search_text)
            filtered_groups = build_product_family_groups(filtered_df)
            filtered_group_lookup = {
                group["selection_id"]: group
                for group in filtered_groups
            }
            display_lookup = {
                **all_group_lookup,
                **filtered_group_lookup,
            }

            session_key = f"selected_basket_products_{filter_key}"
            raw_saved_products = st.session_state.get(session_key, [])
            if filter_key == "all" and legacy_selected_products:
                raw_saved_products = legacy_selected_products

            saved_selection_ids = []
            for saved_product in raw_saved_products:
                if saved_product in all_selection_ids:
                    selection_id = saved_product
                else:
                    selection_id = product_to_selection_id.get(saved_product)

                if selection_id and selection_id not in saved_selection_ids:
                    saved_selection_ids.append(selection_id)

            if raw_saved_products != saved_selection_ids:
                st.session_state[session_key] = saved_selection_ids

            options = [group["selection_id"] for group in filtered_groups]
            display_options = list(dict.fromkeys(saved_selection_ids + options))

            st.caption(
                f"{description}. Aramayla eslesen secim sayisi: {len(options)}."
            )

            if display_options:
                selected = st.multiselect(
                    f"{label} listesinden urun sec",
                    options=display_options,
                    default=saved_selection_ids,
                    format_func=lambda selection_id: format_selection_option(
                        selection_id,
                        display_lookup,
                        catalog_df,
                    ),
                    placeholder="Urun sec",
                    key=session_key,
                )
            else:
                selected = []
                st.info("Bu filtreyle eslesen urun yok.")

            for selection_id in selected:
                group = filtered_group_lookup.get(selection_id) or all_group_lookup.get(
                    selection_id
                )
                if not group:
                    continue

                if selection_id in all_group_lookup:
                    group = merge_selection_group(all_group_lookup[selection_id], group)

                if selection_id in display_lookup:
                    if selection_id in selected_group_lookup:
                        selected_group_lookup[selection_id] = merge_selection_group(
                            selected_group_lookup[selection_id],
                            group,
                        )
                    else:
                        selected_group_lookup[selection_id] = group

            selected_by_filter.extend(selected)

    selected_selection_ids = list(dict.fromkeys(selected_by_filter))
    selected_groups = [
        selected_group_lookup.get(selection_id) or all_group_lookup.get(selection_id)
        for selection_id in selected_selection_ids
    ]
    selected_groups = [group for group in selected_groups if group]
    optimizer_inputs = [
        build_optimizer_input_from_group(group)
        for group in selected_groups
    ]
    st.session_state["selected_basket_products"] = selected_selection_ids

    selected_family_groups = [
        group
        for group in selected_groups
        if group.get("selection_type") == "product_family"
    ]
    if selected_family_groups:
        st.caption(
            "Urun ailesi secildiginde sepete tum es anlamli urunler eklenmez; "
            "optimizasyon aile icinden tek ve en ucuz gecerli secenegi onerir."
        )
        family_rows = [
            {
                "Urun ailesi": group["family_label"],
                "Secenek sayisi": len(group.get("product_names") or []),
                "Secenekler": ", ".join(group.get("product_names") or []),
            }
            for group in selected_family_groups
        ]
        st.dataframe(
            pd.DataFrame(family_rows),
            use_container_width=True,
            hide_index=True,
        )

with summary_panel:
    a101_visible_count = int(
        catalog_df["available_retailers"].fillna("").str.contains("a101").sum()
    )
    st.metric("Katalog Urunleri", len(catalog_df))
    st.metric("A101'de Gorunen", a101_visible_count)
    st.metric("Karsilastirilabilir", filter_counts["comparable"])
    st.metric("Sadece A101", filter_counts["only_a101"])
    st.metric("Sadece Migros", filter_counts["only_migros"])
    st.metric("Inceleme Gerekli", filter_counts["needs_review"])
    st.metric("Secilen Urun", len(optimizer_inputs))
    st.caption(
        "Katalog urunleri, son fiyati olan tekil standart urunlerdir. "
        "A101'de Gorunen sayisi, A101'in tek basina veya Migros ile birlikte sundugu urunleri kapsar. "
        "Karsilastirilabilir urunler A101 ve Migros'ta ayni birim ve miktarla eslesenlerdir. "
        "Sadece A101 ve Sadece Migros sayilari tek markette gorunen urunleri gosterir. "
        "Bu sayilar ham scrape satirlarindan daha dusuk olabilir; tekrar eden gozlemler "
        "ve SKU'lar son urun kapsamina indirgenir."
    )

optimize_clicked = st.button(
    "Sepeti Optimize Et",
    type="primary",
    use_container_width=True,
    disabled=not optimizer_inputs,
)

if not optimize_clicked:
    st.info("Bir veya daha fazla urun sec, sonra sepeti optimize et.")
    st.stop()

with get_connection() as conn:
    with conn.cursor() as cursor:
        result = optimize_basket(cursor, optimizer_inputs)

mixed_basket = result.get("cheapest_mixed_basket", {})
single_basket = result.get("cheapest_single_retailer_basket")
recommendations = result.get("per_product_recommendations", [])
single_source_products = result.get("single_source_only_products", [])
unavailable_products = result.get("unavailable_products", [])
comparable_products = result.get("comparable_products", [])
suspicious_comparison_products = result.get("suspicious_comparison_products", [])
savings_amount = result.get("savings_amount")

mixed_total = mixed_basket.get("total_price", 0)
single_market_options = result.get("single_market_options", [])
single_total = single_basket.get("total_price") if single_basket else None
single_retailer = single_basket.get("retailer") if single_basket else None
smart_savings = build_savings_display(savings_amount, single_market_options)
single_product_messages = build_single_product_messages(recommendations)
single_retailer_detail = (
    f"En iyi tek mağaza: {format_retailer(single_retailer)} ({format_money(single_total)})"
    if single_basket
    else "Bu sepet icin tek mağaza karsilastirmasi yapilamiyor"
)
hero_detail_lines = single_product_messages or [
    f"Karma sepet toplami: {format_money(mixed_total)}.",
    single_retailer_detail,
]
hero_detail_html = "".join(
    f'<p class="savings-hero__detail">{line}</p>'
    for line in hero_detail_lines
)

st.markdown(
    f"""
    <div class="savings-hero">
        <p class="savings-hero__eyebrow">Sepet Avantajı</p>
        <h2 class="savings-hero__value">{smart_savings["headline"]}</h2>
        {hero_detail_html}
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown("### Sepet Ozeti")
kpi1, kpi2, kpi3 = st.columns(3)
with kpi1:
    label = (
        f"En Ucuz Tek Mağaza ({format_retailer(single_retailer)})"
        if single_retailer
        else "En Ucuz Tek Mağaza"
    )
    st.metric(label, format_money(single_total))

with kpi2:
    st.metric("En Ucuz Karma Sepet", format_money(mixed_total))

with kpi3:
    st.metric(smart_savings["label"], format_money(smart_savings["amount"]))

coverage_cols = st.columns(4)
with coverage_cols[0]:
    st.metric("Karsilastirilabilir", len(comparable_products))
with coverage_cols[1]:
    st.metric("Tek Mağaza", len(single_source_products))
with coverage_cols[2]:
    st.metric("Inceleme Gerekli", len(suspicious_comparison_products))
with coverage_cols[3]:
    st.metric("Uygun Degil", len(unavailable_products))

if single_source_products:
    names = ", ".join(
        f"{item['standardized_product_name']} ({item['retailer']})"
        for item in single_source_products
    )
    st.warning(
        "Bazi urunler yalnizca tek mağazada var. Bu urunler karma sepete dahil edilir, "
        f"ama tek mağaza tasarruf karsilastirmasindan cikarilir: {names}."
    )

if suspicious_comparison_products:
    st.warning(
        "Bazi secili urunler iki markette de fiyatli, ancak birim veya miktar farkli. "
        "Paket/olcu karsilastirmasi guvenli olmadigi icin tasarruf hesabindan cikarilir."
    )
    suspicious_df = pd.DataFrame(suspicious_comparison_products)
    suspicious_df["comparison_status_label"] = suspicious_df.apply(
        lambda row: get_comparison_status_label(row.to_dict()),
        axis=1,
    )
    suspicious_df["measurement_mismatch_label"] = suspicious_df.apply(
        lambda row: get_measurement_mismatch_label(row.to_dict()),
        axis=1,
    )
    suspicious_df = suspicious_df.rename(
        columns={
            "standardized_product_name": "Urun",
            "family_label": "Ürün Ailesi",
            "selected_family_product_name": "Seçilen Seçenek",
            "family_option_count": "Seçenek Sayısı",
            "a101_price": "A101 Fiyati",
            "migros_price": "Migros Fiyati",
            "a101_measurement_label": "A101 Olcu",
            "migros_measurement_label": "Migros Olcu",
            "comparison_status_label": "Durum",
            "measurement_mismatch_label": "Olcu Farki",
            "same_unit_flag": "Ayni Birim",
            "same_quantity_flag": "Ayni Miktar",
            "comparison_confidence": "Guven",
            "comparison_review_reason": "Inceleme Nedeni",
        }
    )
    for column in ["A101 Fiyati", "Migros Fiyati"]:
        if column in suspicious_df.columns:
            suspicious_df[column] = suspicious_df[column].apply(format_money)
    st.dataframe(
        suspicious_df[
            [
                "Urun",
                "Ürün Ailesi",
                "Seçilen Seçenek",
                "Seçenek Sayısı",
                "A101 Fiyati",
                "A101 Olcu",
                "Migros Fiyati",
                "Migros Olcu",
                "Durum",
                "Olcu Farki",
                "Ayni Birim",
                "Ayni Miktar",
                "Inceleme Nedeni",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

if unavailable_products:
    names = ", ".join(
        item["standardized_product_name"]
        for item in unavailable_products
    )
    st.error(
        "Bazi urunlerin guncel fiyati yok ve toplamlardan cikarildi: "
        f"{names}."
    )

st.markdown("### Oneri Tablosu")
if recommendations:
    recommendation_df = pd.DataFrame(recommendations)
    recommendation_df["comparison_status_label"] = recommendation_df.apply(
        lambda row: get_comparison_status_label(row.to_dict()),
        axis=1,
    )
    recommendation_df["measurement_mismatch_label"] = recommendation_df.apply(
        lambda row: get_measurement_mismatch_label(row.to_dict()),
        axis=1,
    )
    recommendation_df["measurement_mismatch_label"] = recommendation_df[
        "measurement_mismatch_label"
    ].fillna("-")
    recommendation_df = recommendation_df.rename(
        columns={
            "standardized_product_name": "Urun",
            "family_label": "Ürün Ailesi",
            "selected_family_product_name": "Seçilen Seçenek",
            "family_option_count": "Seçenek Sayısı",
            "coverage_status": "Kapsam",
            "recommended_retailer": "Onerilen Mağaza",
            "recommended_price": "Onerilen Fiyat",
            "a101_price": "A101 Fiyati",
            "migros_price": "Migros Fiyati",
            "a101_measurement_label": "A101 Olcu",
            "migros_measurement_label": "Migros Olcu",
            "availability_status": "Uygunluk",
            "comparison_status_label": "Durum",
            "measurement_mismatch_label": "Olcu Notu",
            "comparison_confidence": "Guven",
            "comparison_review_reason": "Inceleme Nedeni",
        }
    )

    for column in ["Onerilen Fiyat", "A101 Fiyati", "Migros Fiyati"]:
        if column in recommendation_df.columns:
            recommendation_df[column] = recommendation_df[column].apply(format_money)
    if "Kapsam" in recommendation_df.columns:
        recommendation_df["Kapsam"] = recommendation_df["Kapsam"].apply(format_coverage)
    if "Uygunluk" in recommendation_df.columns:
        recommendation_df["Uygunluk"] = recommendation_df["Uygunluk"].apply(
            format_coverage
        )

    visible_columns = [
        column
        for column in [
            "Urun",
            "Ürün Ailesi",
            "Seçilen Seçenek",
            "Seçenek Sayısı",
            "Kapsam",
            "Uygunluk",
            "Onerilen Mağaza",
            "Onerilen Fiyat",
            "A101 Fiyati",
            "A101 Olcu",
            "Migros Fiyati",
            "Migros Olcu",
            "Durum",
            "Olcu Notu",
            "Inceleme Nedeni",
        ]
        if column in recommendation_df.columns
    ]

    st.dataframe(
        recommendation_df[visible_columns],
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("Secili urunler icin oneriler hazirlanamadi.")

items = mixed_basket.get("items", [])

st.markdown("### Alisveris Listesini Disa Aktar")
if items:
    export_rows = build_export_rows(items)
    export_df = pd.DataFrame(export_rows)
    csv_data = export_df.to_csv(index=False).encode("utf-8")
    text_data = build_shopping_list_text(
        items,
        mixed_total,
        smart_savings["label"],
        smart_savings["amount"],
    )

    export_col1, export_col2 = st.columns(2)
    with export_col1:
        st.download_button(
            "CSV indir",
            data=csv_data,
            file_name="shopping_optimizer_list.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with export_col2:
        st.download_button(
            "Metin listesi indir",
            data=text_data,
            file_name="shopping_optimizer_list.txt",
            mime="text/plain",
            use_container_width=True,
        )
else:
    st.info("Disa aktarilacak fiyatli sepet urunu yok.")

st.markdown("### Karma Sepet Plani")
if items:
    item_df = pd.DataFrame(items)
    item_df["selected_measurement_label"] = item_df.apply(
        lambda row: selected_measurement_label(row.to_dict()),
        axis=1,
    )
    item_df["comparison_status_label"] = item_df.apply(
        lambda row: get_comparison_status_label(row.to_dict()),
        axis=1,
    )
    item_df = item_df.rename(
        columns={
            "standardized_product_name": "Urun",
            "family_label": "Ürün Ailesi",
            "selected_family_product_name": "Seçilen Seçenek",
            "family_option_count": "Seçenek Sayısı",
            "recommended_retailer": "Mağaza",
            "selected_price": "Fiyat",
            "selected_measurement_label": "Olcu",
            "coverage_status": "Kapsam",
            "comparison_status_label": "Durum",
        }
    )
    if "Fiyat" in item_df.columns:
        item_df["Fiyat"] = item_df["Fiyat"].apply(format_money)
    if "Kapsam" in item_df.columns:
        item_df["Kapsam"] = item_df["Kapsam"].apply(format_coverage)

    visible_columns = [
        column
        for column in [
            "Urun",
            "Ürün Ailesi",
            "Seçilen Seçenek",
            "Seçenek Sayısı",
            "Mağaza",
            "Fiyat",
            "Olcu",
            "Kapsam",
            "Durum",
        ]
        if column in item_df.columns
    ]
    st.dataframe(item_df[visible_columns], use_container_width=True, hide_index=True)
else:
    st.info("Fiyatlanabilen sepet urunu bulunamadi.")
