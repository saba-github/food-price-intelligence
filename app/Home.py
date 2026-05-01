import base64
import html
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from time import perf_counter

import pandas as pd
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[1]
APP_DIR = Path(__file__).resolve().parent
PRIMARY_LOGO_PATH = APP_DIR / "assets" / "ucuzsepet_logo_primary.svg"
LOGO_CANDIDATE_PATHS = (
    PRIMARY_LOGO_PATH,
    APP_DIR / "assets" / "ucuzsepet_icon.svg",
)
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from db import get_connection, run_query
from pipeline.optimizer.engine import optimize_basket
from pipeline.optimizer.measurement import (
    format_measurement_label,
    get_comparison_status_label,
    get_measurement_mismatch_label,
)
from pipeline.optimizer.paper_products import augment_catalog_with_paper_towel_rows
from pipeline.optimizer.public_compare import build_public_result_display
from pipeline.optimizer.product_search import (
    build_search_group_sections,
    build_optimizer_input_from_group,
    build_product_family_groups,
    format_product_family_group,
    search_product_catalog,
)
from queries import GLOBAL_FRESHNESS_QUERY, PUBLIC_PRODUCT_CATALOG_QUERY


COVERAGE_LABELS = {
    "comparable": "Karşılaştırılabilir",
    "comparison_review_required": "Kontrol gerekli",
    "only_a101": "Sadece A101",
    "only_migros": "Sadece Migros",
    "only_available_at_a101": "Sadece A101",
    "only_available_at_migros": "Sadece Migros",
    "unavailable": "Uygun değil",
}
RETAILER_LABELS = {
    "a101": "A101",
    "migros": "Migros",
}
SEARCH_RESULT_LIMIT = 8
SUBTYPE_SELECTION_STATE_KEY = "selected_subtype"
SUBTYPE_SELECTION_QUERY_STATE_KEY = "selected_subtype_query"
logger = logging.getLogger(__name__)


def get_logo_data_uri() -> str | None:
    for logo_path in LOGO_CANDIDATE_PATHS:
        if not logo_path.exists() or not logo_path.is_file():
            continue

        try:
            if logo_path == PRIMARY_LOGO_PATH:
                svg_text = logo_path.read_text(encoding="utf-8")
                svg_text = svg_text.replace(
                    '<rect width="420" height="146" rx="18" fill="#F4F1EA"/>',
                    "",
                    1,
                )
                encoded_logo = base64.b64encode(svg_text.encode("utf-8")).decode("ascii")
            else:
                encoded_logo = base64.b64encode(logo_path.read_bytes()).decode("ascii")
        except OSError:
            continue

        mime_type = "image/svg+xml"
        if logo_path.suffix.lower() == ".png":
            mime_type = "image/png"
        elif logo_path.suffix.lower() in {".jpg", ".jpeg"}:
            mime_type = "image/jpeg"
        elif logo_path.suffix.lower() == ".webp":
            mime_type = "image/webp"

        return f"data:{mime_type};base64,{encoded_logo}"

    return None

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

    return str(value)


def build_catalog_cache_key(freshness_row: dict | None) -> str | None:
    if not freshness_row:
        return None

    latest_success = freshness_row.get("latest_success_started_at")
    if latest_success is None:
        return None

    if hasattr(latest_success, "isoformat"):
        return latest_success.isoformat()

    return str(latest_success)


def format_money(value) -> str:
    if value is None:
        return "-"

    try:
        return f"{float(value):,.2f} TL"
    except (TypeError, ValueError):
        return "-"


def format_retailer(value) -> str:
    return RETAILER_LABELS.get(str(value), str(value))


def format_coverage(value) -> str:
    return COVERAGE_LABELS.get(str(value), str(value))


def status_theme(label: str) -> str:
    if label == "Güvenli karşılaştırma":
        return "status-safe"
    if label == "Tek markette var":
        return "status-single"
    return "status-review"


def result_title(group: dict, recommendation: dict) -> str:
    family_label = recommendation.get("family_label") or group.get("family_label")
    if family_label:
        return family_label
    return recommendation.get("standardized_product_name") or "Ürün"


def chosen_option_label(recommendation: dict) -> str:
    selected_family_product_name = recommendation.get("selected_family_product_name")
    if selected_family_product_name:
        return selected_family_product_name
    coverage_status = recommendation.get("coverage_status")
    if coverage_status in {"only_a101", "only_available_at_a101"}:
        return recommendation.get("a101_source_product_name") or "-"
    if coverage_status in {"only_migros", "only_available_at_migros"}:
        return recommendation.get("migros_source_product_name") or "-"
    return recommendation.get("standardized_product_name") or "-"


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
    catalog_df["coverage_label"] = catalog_df["coverage_status"].apply(format_coverage)
    return catalog_df


def display_product_name(row: pd.Series, fallback_name: str) -> str:
    coverage_status = row.get("coverage_status")
    if coverage_status in {"only_a101", "only_available_at_a101"}:
        return row.get("a101_source_product_name") or fallback_name
    if coverage_status in {"only_migros", "only_available_at_migros"}:
        return row.get("migros_source_product_name") or fallback_name
    return fallback_name


def format_search_option(group: dict, catalog_df: pd.DataFrame) -> str:
    if group.get("selection_type") == "product_family":
        return format_product_family_group(group)

    product_names = group.get("product_names") or []
    if not product_names:
        return group.get("selection_id", "Ürün")

    product_name = product_names[0]
    row = catalog_df.loc[catalog_df["standardized_product_name"] == product_name]
    if row.empty:
        return product_name

    display_name = display_product_name(row.iloc[0], product_name)
    status_label = row.iloc[0].get("comparison_status_label")
    coverage_label = row.iloc[0].get("coverage_label")
    return f"{display_name} • {status_label} • {coverage_label}"


def format_related_group_option(group: dict, catalog_df: pd.DataFrame) -> str:
    if group.get("selection_type") == "product_family":
        return group.get("family_label") or format_product_family_group(group)
    return format_search_option(group, catalog_df)


def sync_subtype_selection_state(
    search_text: str,
    safe_groups: list[dict],
    related_groups: list[dict],
) -> None:
    previous_query = st.session_state.get(SUBTYPE_SELECTION_QUERY_STATE_KEY)
    if previous_query != search_text:
        st.session_state[SUBTYPE_SELECTION_QUERY_STATE_KEY] = search_text
        st.session_state[SUBTYPE_SELECTION_STATE_KEY] = None

    if safe_groups or not related_groups:
        st.session_state[SUBTYPE_SELECTION_STATE_KEY] = None
        return

    valid_selection_ids = {group["selection_id"] for group in related_groups}
    if st.session_state.get(SUBTYPE_SELECTION_STATE_KEY) not in valid_selection_ids:
        st.session_state[SUBTYPE_SELECTION_STATE_KEY] = None


def render_subtype_options(group_lookup: dict[str, dict], catalog_df: pd.DataFrame) -> None:
    for selection_id, group in group_lookup.items():
        if st.button(
            format_related_group_option(group, catalog_df),
            key=f"subtype_option_{selection_id}",
            use_container_width=True,
            type="secondary",
        ):
            st.session_state[SUBTYPE_SELECTION_STATE_KEY] = selection_id
            st.rerun()


def build_search_groups(catalog_df: pd.DataFrame, search_text: str) -> list[dict]:
    search_results = search_product_catalog(catalog_df, search_text)
    groups = build_product_family_groups(search_results)
    return groups[:SEARCH_RESULT_LIMIT]


def format_available_retailers(value) -> str:
    if value is None:
        return "-"

    retailers = []
    for retailer in str(value).split(","):
        retailer = retailer.strip()
        if retailer:
            retailers.append(format_retailer(retailer))

    return ", ".join(retailers) if retailers else "-"


def related_result_reason(row: pd.Series) -> str:
    base_reason = (
        "Paket gramajı/tür farklı olabilir, bu yüzden otomatik fiyat karşılaştırmasına alınmadı."
    )
    if row.get("coverage_status") == "comparison_review_required":
        mismatch_label = get_measurement_mismatch_label(row.to_dict())
        if mismatch_label:
            return f"{mismatch_label}. {base_reason}"
    return base_reason


def related_section_title(search_text: str) -> str:
    if "mantar" in search_text:
        return "Diğer mantar seçenekleri"
    return "Benzer ürünler"


def build_related_results_df(groups: list[dict], catalog_df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for group in groups:
        product_names = group.get("product_names") or []
        if not product_names:
            continue

        product_name = product_names[0]
        row = catalog_df.loc[catalog_df["standardized_product_name"] == product_name]
        if row.empty:
            continue

        row = row.iloc[0]
        measurement_parts = []
        if row.get("a101_measurement_label") and row.get("a101_measurement_label") != "-":
            measurement_parts.append(f"A101: {row.get('a101_measurement_label')}")
        if row.get("migros_measurement_label") and row.get("migros_measurement_label") != "-":
            measurement_parts.append(f"Migros: {row.get('migros_measurement_label')}")

        rows.append(
            {
                "Ürün": (
                    format_product_family_group(group)
                    if group.get("selection_type") == "product_family"
                    else display_product_name(row, product_name)
                ),
                "Durum": row.get("comparison_status_label"),
                "Marketler": format_available_retailers(row.get("available_retailers")),
                "Ölçü": " / ".join(measurement_parts) if measurement_parts else "-",
                "Not": related_result_reason(row),
            }
        )

    return pd.DataFrame(rows)


def catalog_row_for_product(
    catalog_df: pd.DataFrame,
    standardized_product_name: str | None,
) -> dict | None:
    if not standardized_product_name:
        return None

    row = catalog_df.loc[
        catalog_df["standardized_product_name"] == standardized_product_name
    ]
    if row.empty:
        return None

    return row.iloc[0].to_dict()


def price_metric_label(retailer: str, display_recommendation: dict) -> str:
    price_display_unit = display_recommendation.get("price_display_unit")
    if price_display_unit:
        return f"{retailer} {price_display_unit} fiyatı"
    return f"{retailer} fiyatı"


def format_price_with_unit(value, unit: str | None) -> str:
    money = format_money(value)
    if money == "-" or not unit:
        return money
    return f"{money}/{unit}"


def normalization_explanation(
    recommendation: dict,
    display_recommendation: dict,
) -> str | None:
    comparison_unit = display_recommendation.get("price_display_unit")
    if not comparison_unit:
        return None

    a101_package = recommendation.get("a101_measurement_label")
    migros_package = recommendation.get("migros_measurement_label")
    if a101_package and a101_package != "-" and migros_package and migros_package != "-":
        return (
            f"{a101_package} ve {migros_package} paketlerini adil karşılaştırmak için "
            f"{comparison_unit} fiyatına çevirdik."
        )

    return f"Farklı paketleri adil karşılaştırmak için {comparison_unit} fiyatına çevirdik."


def winner_badge_text(
    recommendation: dict,
    display_recommendation: dict,
) -> str | None:
    retailer = display_recommendation.get("display_recommended_retailer")
    if recommendation.get("comparison_confidence") == "single_source":
        return None
    if retailer == "same":
        return "En ucuz: Aynı fiyat"
    if retailer is None:
        return None
    return f"En ucuz: {format_retailer(retailer)}"


def compare_note(recommendation: dict, display_recommendation: dict) -> str:
    retailer = display_recommendation.get("display_recommended_retailer")
    if recommendation.get("comparison_confidence") == "single_source" and retailer:
        return f"Bu ürün şu anda yalnızca {format_retailer(retailer)} markette mevcut."
    if retailer is None:
        mismatch_label = get_measurement_mismatch_label(recommendation)
        if mismatch_label:
            return f"Bu tür için otomatik en ucuz sonucu göstermeden önce kontrol gerekiyor: {mismatch_label}."
        return "Bu tür için otomatik en ucuz sonucu göstermeden önce kontrol gerekiyor."

    other_retailer = "migros" if retailer == "a101" else "a101"
    recommended_price = display_recommendation.get("display_recommended_price")
    other_price = display_recommendation.get(f"{other_retailer}_display_price")

    if recommended_price is None or other_price is None:
        return f"Şu anda en düşük görünen fiyat {format_retailer(retailer)} tarafında."

    price_advantage = float(other_price) - float(recommended_price)
    if abs(price_advantage) < 0.01:
        return "İki markette de görünen fiyat aynı."

    comparison_unit = display_recommendation.get("price_display_unit")
    if comparison_unit:
        return (
            f"{format_retailer(retailer)}, {format_retailer(other_retailer)}'a göre "
            f"{comparison_unit} başına {format_money(price_advantage)} daha ucuz."
        )

    return (
        f"{format_retailer(retailer)} seçimi şu anda "
        f"{format_money(price_advantage)} avantaj sağlıyor."
    )


def build_advantage_display(display_recommendation: dict) -> tuple[str, str]:
    retailer = display_recommendation.get("display_recommended_retailer")
    a101_price = display_recommendation.get("a101_display_price")
    migros_price = display_recommendation.get("migros_display_price")

    if retailer is None or a101_price is None or migros_price is None:
        if display_recommendation.get("comparison_confidence") == "single_source":
            return "-", "Tek markette fiyat var"
        return "-", "Güvenli fark gösterilmiyor"

    advantage = abs(float(a101_price) - float(migros_price))
    if advantage < 0.01:
        return format_money(advantage), "Fiyat farkı yok"

    more_expensive_retailer = "a101" if float(a101_price) > float(migros_price) else "migros"
    return format_money(advantage), f"{format_retailer(more_expensive_retailer)} fiyatına göre"


def render_result_card(
    group: dict,
    recommendation: dict,
    display_recommendation: dict,
    freshness_row: dict | None,
) -> None:
    status_label = get_comparison_status_label(recommendation)
    status_class = status_theme(status_label)
    product_title = result_title(group, recommendation)
    chosen_option = chosen_option_label(recommendation)
    freshness_text = "-"
    if freshness_row:
        freshness_text = format_metric_value(freshness_row.get("latest_success_started_at"))
    explanation_text = normalization_explanation(
        recommendation,
        display_recommendation,
    )
    comparison_unit = display_recommendation.get("price_display_unit")
    comparison_label = (
        "Kg fiyatı" if comparison_unit == "kg" else "Karşılaştırma fiyatı"
    )
    winning_retailer = display_recommendation.get("display_recommended_retailer")
    single_source_retailer = None
    if recommendation.get("comparison_confidence") == "single_source":
        if recommendation.get("coverage_status") in {"only_a101", "only_available_at_a101"}:
            single_source_retailer = "a101"
        elif recommendation.get("coverage_status") in {"only_migros", "only_available_at_migros"}:
            single_source_retailer = "migros"

    topline_text = "En ucuz nerede?"
    if single_source_retailer:
        topline_text = (
            f"Bu ürün şu anda yalnızca {format_retailer(single_source_retailer)} markette mevcut"
        )
    elif winning_retailer is None:
        topline_text = "Karşılaştırma özeti"

    market_cards_html_parts = []
    for retailer in ["a101", "migros"]:
        card_classes = ["market-price-card"]
        if winning_retailer == retailer:
            card_classes.append("market-price-card--winner")
        elif winning_retailer not in {None, "same"}:
            card_classes.append("market-price-card--loser")

        market_cards_html_parts.append(
            (
                f'<div class="{" ".join(card_classes)}">'
                f'<div class="market-price-card__title">{format_retailer(retailer)}</div>'
                '<div class="market-price-card__row">'
                '<span>Raf fiyatı</span>'
                f'<strong>{format_money(display_recommendation.get(f"{retailer}_shelf_price"))}</strong>'
                '</div>'
                '<div class="market-price-card__row">'
                '<span>Paket</span>'
                f'<strong>{recommendation.get(f"{retailer}_measurement_label") or "-"}</strong>'
                '</div>'
                '<div class="market-price-card__row">'
                f"<span>{comparison_label}</span>"
                f'<strong>{format_price_with_unit(display_recommendation.get(f"{retailer}_display_price"), comparison_unit)}</strong>'
                '</div>'
                '</div>'
            )
        )

    explanation_html = ""
    if explanation_text:
        explanation_html = (
            f'<p class="result-card__explanation">{explanation_text}</p>'
        )

    market_cards_html = "".join(market_cards_html_parts)
    winner_badge = winner_badge_text(recommendation, display_recommendation)
    winner_badge_html = (
        f'<div class="winner-pill">{winner_badge}</div>'
        if winner_badge
        else ""
    )
    card_html = (
        '<div class="result-card">'
        f'<div class="result-card__topline">{topline_text}</div>'
        f'<h2 class="result-card__title">{product_title}</h2>'
        f'<p class="result-card__subtitle">Gösterilen ürün: {chosen_option}</p>'
        f'{explanation_html}'
        '<div class="market-price-grid">'
        f'{market_cards_html}'
        '</div>'
        '<div class="result-card__winner-row">'
        f'{winner_badge_html}'
        f'<div class="result-card__difference">{compare_note(recommendation, display_recommendation)}</div>'
        '</div>'
        '<div class="result-card__meta">'
        '<div>'
        '<div class="result-card__meta-label">Karşılaştırma durumu</div>'
        f'<div class="status-pill {status_class}">{status_label}</div>'
        '</div>'
        '<div>'
        '<div class="result-card__meta-label">Veri tazeliği</div>'
        f'<div class="result-card__meta-value">{freshness_text}</div>'
        '</div>'
        '</div>'
        '</div>'
    )

    st.markdown(card_html, unsafe_allow_html=True)


def render_price_rows(recommendation: dict, display_recommendation: dict) -> None:
    comparison_label = (
        "Kg fiyatı"
        if display_recommendation.get("price_display_unit") == "kg"
        else "Karşılaştırma fiyatı"
    )
    rows = [
        {
            "Mağaza": "A101",
            "Raf fiyatı": format_money(display_recommendation.get("a101_shelf_price")),
            "Paket": recommendation.get("a101_measurement_label") or "-",
            comparison_label: format_price_with_unit(
                display_recommendation.get("a101_display_price"),
                display_recommendation.get("price_display_unit"),
            ),
        },
        {
            "Mağaza": "Migros",
            "Raf fiyatı": format_money(display_recommendation.get("migros_shelf_price")),
            "Paket": recommendation.get("migros_measurement_label") or "-",
            comparison_label: format_price_with_unit(
                display_recommendation.get("migros_display_price"),
                display_recommendation.get("price_display_unit"),
            ),
        },
    ]
    render_brand_table(pd.DataFrame(rows), "brand-table--summary")


def render_brand_table(dataframe: pd.DataFrame, modifier_class: str = "") -> None:
    if dataframe.empty:
        return

    table_class = "brand-table"
    if modifier_class:
        table_class = f"{table_class} {modifier_class}"

    table_html = dataframe.to_html(
        index=False,
        escape=True,
        border=0,
        classes=table_class,
    )
    st.markdown(
        f'<div class="brand-table-shell">{table_html}</div>',
        unsafe_allow_html=True,
    )


def render_related_results_table(dataframe: pd.DataFrame) -> None:
    if dataframe.empty:
        return

    rows_html = []
    for _, row in dataframe.iterrows():
        status_value = str(row.get("Durum", "-"))
        status_class = "status-chip"
        if status_value == "Tek markette var":
            status_class += " status-chip--single"
        elif status_value == "Kontrol gerekli":
            status_class += " status-chip--review"

        rows_html.append(
            "<tr>"
            f"<td>{html.escape(str(row.get('Ürün', '-')))}</td>"
            f"<td><span class=\"{status_class}\">{html.escape(status_value)}</span></td>"
            f"<td>{html.escape(str(row.get('Marketler', '-')))}</td>"
            f"<td>{html.escape(str(row.get('Ölçü', '-')))}</td>"
            f"<td>{html.escape(str(row.get('Not', '-')))}</td>"
            "</tr>"
        )

    table_html = (
        '<div class="brand-table-shell">'
        '<table class="brand-table brand-table--related">'
        "<thead><tr>"
        "<th>Ürün</th><th>Durum</th><th>Marketler</th><th>Ölçü</th><th>Not</th>"
        "</tr></thead>"
        f"<tbody>{''.join(rows_html)}</tbody>"
        "</table>"
        "</div>"
    )
    st.markdown(table_html, unsafe_allow_html=True)


def render_inline_message(text: str, tone: str = "muted") -> None:
    st.markdown(
        (
            f'<div class="inline-message inline-message--{tone}">'
            f"{html.escape(text)}"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def render_empty_state() -> None:
    st.markdown(
        """
        <div class="empty-state">
            <div class="empty-state__icon" aria-hidden="true">
                <svg viewBox="0 0 24 24" fill="none">
                    <circle cx="11" cy="11" r="6.5"></circle>
                    <path d="M16.2 16.2L21 21"></path>
                </svg>
            </div>
            <div class="empty-state__title">Aramaya başlamak için ürün adı yaz</div>
            <div class="empty-state__body">Domates, süt, mantar veya salatalık gibi bir ürün arayabilirsin.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_freshness_card(freshness_row: dict) -> None:
    latest_run_status = freshness_row.get("latest_run_status")
    latest_run_source_name = freshness_row.get("latest_run_source_name")
    latest_run_started_at = freshness_row.get("latest_run_started_at")
    latest_run_error_message = freshness_row.get("latest_run_error_message")
    is_stale = latest_run_status == "failed"
    status_class = (
        "freshness-card__status freshness-card__status--warning"
        if is_stale
        else "freshness-card__status"
    )
    status_text = "Son kayıt" if is_stale else "Canlı"

    st.markdown(
        f"""
        <div class="freshness-card">
            <div class="freshness-card__topline">Veri güncelliği</div>
            <div class="freshness-card__header">
                <div class="freshness-card__details">
                    <p class="freshness-card__line">
                        <span>Son fiyat tarihi</span>
                        <strong>{format_metric_value(freshness_row.get("latest_data_date"))}</strong>
                    </p>
                    <p class="freshness-card__line">
                        <span>Son başarılı güncelleme</span>
                        <strong>{format_metric_value(freshness_row.get("latest_success_started_at"))}</strong>
                    </p>
                </div>
                <div class="{status_class}">
                    <span class="freshness-card__status-dot" aria-hidden="true"></span>
                    <span>{status_text}</span>
                </div>
            </div>
            <p class="freshness-card__note">
                Veri canlı değildir; son başarılı güncelleme zamanına göre gösterilir.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if is_stale:
        failure_detail = ""
        if latest_run_error_message:
            failure_detail = f" ({str(latest_run_error_message)[:180]})"
        render_inline_message(
            "Veri güncellenemedi; gösterilen fiyat son başarılı kayıttandır. "
            f"Son deneme: {format_retailer(latest_run_source_name)} - "
            f"{format_metric_value(latest_run_started_at)}{failure_detail}",
            tone="warning",
        )


def resolve_selected_group(
    group_lookup: dict[str, dict],
    state_key: str,
) -> tuple[str, dict, list[str]]:
    selection_ids = list(group_lookup)
    selected_selection_id = st.session_state.get(state_key)
    if selected_selection_id not in group_lookup:
        selected_selection_id = selection_ids[0]
    return selected_selection_id, group_lookup[selected_selection_id], selection_ids


def render_selected_group_summary(
    selected_group: dict,
    catalog_df: pd.DataFrame,
    freshness_row: dict | None,
) -> dict:
    optimizer_input = build_optimizer_input_from_group(selected_group)

    with st.spinner("En uygun sonuç hazırlanıyor..."):
        with get_connection() as conn:
            with conn.cursor() as cursor:
                result = optimize_basket(cursor, [optimizer_input])

    recommendations = result.get("per_product_recommendations", [])
    if not recommendations:
        st.error("Bu ürün için gösterilebilir sonuç hazırlanamadı.")
        st.stop()

    recommendation = recommendations[0]
    catalog_row = catalog_row_for_product(
        catalog_df,
        recommendation.get("standardized_product_name"),
    )
    display_recommendation = build_public_result_display(
        recommendation,
        catalog_row,
    )
    render_result_card(
        selected_group,
        recommendation,
        display_recommendation,
        freshness_row,
    )

    st.markdown("### Fiyat özeti")
    render_price_rows(recommendation, display_recommendation)

    return recommendation


@st.cache_data(ttl=300)
def load_public_catalog(_freshness_key: str | None = None) -> pd.DataFrame:
    cache_started_at = perf_counter()
    logger.info(
        "load_public_catalog started freshness_key=%s",
        _freshness_key,
    )
    query_started_at = perf_counter()
    catalog_df = run_query(PUBLIC_PRODUCT_CATALOG_QUERY)
    logger.info(
        "load_public_catalog query finished in %.3fs rows=%s",
        perf_counter() - query_started_at,
        len(catalog_df),
    )
    augmentation_started_at = perf_counter()
    catalog_df = augment_catalog_with_paper_towel_rows(catalog_df)
    logger.info(
        "load_public_catalog paper towel augmentation finished in %.3fs rows=%s",
        perf_counter() - augmentation_started_at,
        len(catalog_df),
    )
    display_started_at = perf_counter()
    catalog_df = add_catalog_display_fields(catalog_df)
    logger.info(
        "load_public_catalog display fields finished in %.3fs total=%.3fs",
        perf_counter() - display_started_at,
        perf_counter() - cache_started_at,
    )
    return catalog_df


st.set_page_config(
    page_title="UcuzSepet | En Ucuz Nerede?",
    layout="centered",
    initial_sidebar_state="collapsed",
)

freshness_df = run_query(GLOBAL_FRESHNESS_QUERY)
freshness_row = freshness_df.iloc[0].to_dict() if not freshness_df.empty else None
latest_update_display = (
    format_metric_value(freshness_row.get("latest_success_started_at"))
    if freshness_row
    else "-"
)
logo_data_uri = get_logo_data_uri()

brand_lockup_html = (
    (
        '<div class="brand-header__logo-wrap">'
        f'<img src="{logo_data_uri}" class="brand-header__logo" alt="UcuzSepet logo" />'
        "</div>"
    )
    if logo_data_uri
    else (
        '<div class="brand-header__brand">'
        '<span class="brand-header__brand-ucuz">Ucuz</span>'
        '<span class="brand-header__brand-sepet">Sepet</span>'
        "</div>"
    )
)
st.markdown(
    """
    <style>
    :root {
        --green-900: #1B3D08;
        --green-700: #27500A;
        --green-400: #3B6D11;
        --green-100: #C0DD97;
        --green-50: #EAF3DE;
        --cream: #F4F3EF;
        --text-900: #1C1C1A;
        --text-500: #5F5E5A;
        --text-300: #9B9A96;
        --border: #E2E0D8;
        --brand-primary: var(--green-700);
        --brand-accent: var(--green-100);
        --brand-bg: var(--cream);
        --brand-surface: #FFFFFF;
        --brand-text: var(--text-900);
        --brand-border: var(--border);
        --brand-border-strong: var(--border);
        --brand-muted: var(--text-500);
        --brand-warning: #6E5216;
    }
    html, body, .stApp {
        background: var(--brand-bg);
        color: var(--brand-text);
        font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        font-size: 15px;
        line-height: 1.6;
    }
    p, div, span, label, input, textarea, button, table {
        color: var(--brand-text);
        font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        font-size: 15px;
        line-height: 1.6;
    }
    h1, h2, h3, h4, h5, h6 {
        font-family: Georgia, "Times New Roman", serif;
        color: var(--brand-text);
        letter-spacing: 0;
        font-weight: 700;
    }
    h3 {
        margin-top: 28px;
        margin-bottom: 12px;
        font-size: 1.1rem;
    }
    a, a:visited {
        color: var(--brand-primary) !important;
    }
    [data-testid="stSidebar"],
    [data-testid="stSidebarCollapsedControl"] {
        display: none;
    }
    .block-container {
        max-width: 680px;
        padding-top: 0;
        padding-bottom: 40px;
        padding-left: 24px;
        padding-right: 24px;
    }
    .brand-header {
        margin: 24px 0 0 0;
        padding: 28px 0 0;
        background: transparent;
        border-bottom: 1px solid var(--brand-border);
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 0.75rem;
        flex-wrap: wrap;
    }
    .brand-header__left {
        display: flex;
        align-items: center;
        min-width: 0;
        flex: 0 1 auto;
        margin-left: -12px;
    }
    .brand-header__logo-wrap {
        display: flex;
        align-items: center;
        min-width: 0;
    }
    .brand-header__logo {
        display: block;
        width: 326px;
        height: auto;
        max-width: 100%;
        object-fit: contain;
    }
    .brand-header__brand {
        display: inline-flex;
        align-items: center;
        gap: 0.2rem;
        font-family: Georgia, "Times New Roman", serif;
        font-size: 22px;
        font-weight: 700;
        line-height: 1;
        white-space: nowrap;
    }
    .brand-header__brand-ucuz {
        color: #27500A;
    }
    .brand-header__brand-sepet {
        color: #1C1C1A;
    }
    .last-update-note {
        font-size: 12px;
        color: #9B9A96;
        line-height: 1.4;
        text-align: right;
        margin-top: 8px;
        white-space: normal;
    }
    .hero-section {
        padding-top: 32px;
        margin-top: 0;
        display: flex;
        flex-direction: column;
        gap: 0;
    }
    .hero-section > * {
        margin-block-start: 0 !important;
        margin-block-end: 0 !important;
    }
    div.hero-section__eyebrow {
        margin: 0 0 4px !important;
        color: #27500A;
        font-size: 12px;
        line-height: 1.2;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        font-weight: 700;
    }
    h1.hero-section__title {
        margin: 0 !important;
        font-size: 34px;
        line-height: 0.92;
        color: #1C1C1A;
    }
    .hero-section__title br {
        display: block;
        content: "";
        margin: 0;
    }
    p.hero-section__subtitle {
        margin: 6px 0 10px !important;
        max-width: 36rem;
        color: var(--text-500);
        font-size: 15px;
        line-height: 1.15;
    }
    .freshness-card {
        border: 1px solid var(--brand-border);
        border-radius: 12px;
        padding: 16px 18px;
        margin-top: 32px;
        background: var(--brand-surface);
    }
    .freshness-card__topline {
        margin: 0 0 10px;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: var(--text-300);
        font-weight: 700;
    }
    .freshness-card__header {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 1rem;
        flex-wrap: wrap;
    }
    .freshness-card__details {
        display: grid;
        gap: 0.35rem;
        min-width: 0;
    }
    .freshness-card__line {
        margin: 0;
        color: var(--text-500);
        font-size: 13px;
        line-height: 1.5;
    }
    .freshness-card__line span {
        color: var(--text-500);
    }
    .freshness-card__line strong {
        color: var(--text-900);
        font-weight: 600;
    }
    .freshness-card__status {
        display: inline-flex;
        align-items: center;
        gap: 0.4rem;
        padding: 0.28rem 0.65rem;
        border-radius: 999px;
        border: 1px solid rgba(39, 80, 10, 0.14);
        background: rgba(192, 221, 151, 0.2);
        font-size: 12px;
        font-weight: 700;
        color: var(--brand-primary);
    }
    .freshness-card__status--warning {
        border-color: rgba(130, 109, 40, 0.18);
        background: rgba(255, 249, 232, 0.85);
        color: #7B611A;
    }
    .freshness-card__status-dot {
        width: 0.55rem;
        height: 0.55rem;
        border-radius: 50%;
        background: #4C8D16;
        animation: freshnessPulse 1.8s infinite;
    }
    .freshness-card__note {
        margin: 10px 0 0;
        font-size: 13px;
        line-height: 1.6;
        color: var(--text-500);
    }
    .inline-message {
        border: 1px solid var(--brand-border);
        border-radius: 12px;
        padding: 0.95rem 1rem;
        margin: 0.9rem 0 0;
        background: rgba(255, 255, 255, 0.76);
        line-height: 1.5;
    }
    .inline-message--warning {
        border-color: rgba(130, 109, 40, 0.32);
        background: rgba(255, 249, 232, 0.82);
    }
    .inline-message--info {
        border-color: rgba(39, 80, 10, 0.22);
        background: rgba(192, 221, 151, 0.18);
    }
    .inline-message--muted {
        color: var(--brand-muted);
    }
    .search-panel {
        margin: 8px 0 0;
        padding: 18px 20px 12px;
        background: var(--brand-surface);
        border: 1px solid var(--brand-border);
        border-bottom: none;
        border-radius: 16px 16px 0 0;
    }
    .search-panel__label {
        margin: 0 0 1.1rem;
        font-size: 11px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: var(--text-300);
    }
    div[data-testid="stTextInput"] {
        margin-top: -1px;
        margin-bottom: 0;
        padding: 0 20px 18px;
        background: var(--brand-surface);
        border: 1px solid var(--brand-border);
        border-top: none;
        border-radius: 0 0 16px 16px;
    }
    div[data-testid="stTextInput"] [data-baseweb="base-input"] {
        position: relative;
        border-radius: 12px !important;
        border: 1.5px solid var(--border) !important;
        background: var(--brand-surface) !important;
        box-shadow: none !important;
        transition: border-color 0.2s ease;
        overflow: visible !important;
    }
    div[data-testid="stTextInput"] [data-baseweb="base-input"]:hover,
    div[data-testid="stTextInput"] [data-baseweb="input"]:hover,
    div[data-testid="stTextInput"] [data-baseweb="base-input"][aria-invalid="true"],
    div[data-testid="stTextInput"] [data-baseweb="input"][aria-invalid="true"],
    div[data-testid="stTextInput"] [data-baseweb="base-input"][data-invalid="true"],
    div[data-testid="stTextInput"] [data-baseweb="input"][data-invalid="true"],
    div[data-testid="stTextInput"][aria-invalid="true"] [data-baseweb="base-input"],
    div[data-testid="stTextInput"][aria-invalid="true"] [data-baseweb="input"] {
        border-color: #E2E0D8 !important;
        outline: none !important;
        box-shadow: none !important;
    }
    div[data-testid="stTextInput"] [data-baseweb="base-input"]:hover,
    div[data-testid="stTextInput"] [data-baseweb="input"]:hover {
        border-color: #C0DD97 !important;
    }
    div[data-testid="stTextInput"] [data-baseweb="base-input"]:focus,
    div[data-testid="stTextInput"] [data-baseweb="base-input"]:focus-visible,
    div[data-testid="stTextInput"] [data-baseweb="base-input"]:focus-within,
    div[data-testid="stTextInput"] [data-baseweb="input"]:focus,
    div[data-testid="stTextInput"] [data-baseweb="input"]:focus-visible,
    div[data-testid="stTextInput"] [data-baseweb="input"]:focus-within,
    div[data-testid="stTextInput"] [data-baseweb="base-input"][aria-invalid="true"]:focus-within,
    div[data-testid="stTextInput"] [data-baseweb="input"][aria-invalid="true"]:focus-within,
    div[data-testid="stTextInput"] [data-baseweb="base-input"][data-invalid="true"]:focus-within,
    div[data-testid="stTextInput"] [data-baseweb="input"][data-invalid="true"]:focus-within,
    div[data-testid="stTextInput"][aria-invalid="true"] [data-baseweb="base-input"]:focus-within,
    div[data-testid="stTextInput"][aria-invalid="true"] [data-baseweb="input"]:focus-within {
        border-color: #27500A !important;
        outline: none !important;
        box-shadow: none !important;
    }
    div[data-testid="stTextInput"] [data-baseweb="base-input"] > div {
        overflow: visible !important;
    }
    div[data-testid="stTextInput"] input,
    .stTextInput input {
        color: var(--brand-text) !important;
        background: transparent !important;
        caret-color: var(--brand-primary) !important;
        margin: 0 !important;
        overflow: visible !important;
        appearance: none !important;
        -webkit-appearance: none !important;
        -webkit-tap-highlight-color: transparent !important;
        border: none !important;
        box-shadow: none !important;
        font-family: inherit !important;
    }
    div[data-testid="stTextInput"] input:focus,
    div[data-testid="stTextInput"] input:focus-visible,
    div[data-testid="stTextInput"] input:active,
    div[data-testid="stTextInput"] input:invalid,
    div[data-testid="stTextInput"] input[aria-invalid="true"],
    div[data-testid="stTextInput"] input[data-invalid="true"] {
        outline: none !important;
        box-shadow: none !important;
        border: none !important;
        color: var(--brand-text) !important;
        -webkit-text-fill-color: var(--brand-text) !important;
    }
    div[data-testid="stTextInput"] input::placeholder {
        color: var(--text-300) !important;
        opacity: 1;
    }
    div[data-testid="stTextInput"] [data-testid="InputInstructions"],
    div[data-testid="stTextInput"] + [data-testid="InputInstructions"],
    div[data-testid="stTextInput"] [aria-live="polite"] {
        display: none !important;
        height: 0 !important;
        margin: 0 !important;
        padding: 0 !important;
        overflow: hidden !important;
    }
    div[data-testid="stTextInput"]:focus-within [data-baseweb="base-input"],
    div[data-testid="stTextInput"]:focus-within [data-baseweb="input"],
    div[data-testid="stTextInput"][aria-invalid="true"]:focus-within [data-baseweb="base-input"],
    div[data-testid="stTextInput"][aria-invalid="true"]:focus-within [data-baseweb="input"] {
        border-color: var(--green-700) !important;
        outline: none !important;
        box-shadow: none !important;
    }
    .empty-state {
        margin: 12px 0 0;
        padding: 1.1rem 1rem;
        border: 1px solid var(--brand-border);
        border-radius: 12px;
        background: rgba(255, 255, 255, 0.82);
        text-align: center;
    }
    .empty-state__icon {
        width: 2.8rem;
        height: 2.8rem;
        margin: 0 auto 0.7rem;
        border-radius: 999px;
        display: flex;
        align-items: center;
        justify-content: center;
        background: rgba(192, 221, 151, 0.26);
        border: 1px solid rgba(39, 80, 10, 0.12);
    }
    .empty-state__icon svg {
        width: 1.6rem;
        height: 1.6rem;
        stroke: var(--brand-primary);
        stroke-width: 1.8;
        stroke-linecap: round;
    }
    .empty-state__title {
        font-family: Iowan Old Style, Palatino Linotype, Book Antiqua, Georgia, serif;
        font-size: 1.1rem;
        font-weight: 700;
    }
    .empty-state__body {
        margin-top: 0.25rem;
        color: var(--brand-muted);
        line-height: 1.5;
        font-size: 14px;
    }
    .result-card {
        border: 1px solid var(--brand-border);
        border-radius: 12px;
        padding: 20px 24px;
        margin-top: 18px;
        background: var(--brand-surface);
    }
    .result-card__topline {
        font-size: 11px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: var(--text-300);
        margin-bottom: 6px;
    }
    .result-card__title {
        margin: 0;
        font-size: 28px;
        line-height: 1.15;
    }
    .result-card__subtitle {
        margin: 8px 0 0 0;
        color: var(--text-500);
        font-size: 14px;
    }
    .result-card__explanation {
        margin: 12px 0 0 0;
        line-height: 1.6;
        color: var(--text-500);
        font-size: 14px;
    }
    .market-price-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 12px;
        margin-top: 18px;
    }
    .market-price-card {
        border: 1px solid var(--brand-border);
        border-radius: 12px;
        padding: 16px;
        background: #FFFFFF;
    }
    .market-price-card--winner {
        background: var(--green-50);
        border-color: var(--green-700);
    }
    .market-price-card--winner .market-price-card__row strong {
        font-weight: 700;
    }
    .market-price-card__title {
        font-size: 14px;
        font-weight: 700;
        margin-bottom: 12px;
        color: var(--green-900);
    }
    .market-price-card__row {
        display: flex;
        justify-content: space-between;
        gap: 0.75rem;
        align-items: flex-start;
        margin-top: 0.45rem;
        line-height: 1.4;
    }
    .market-price-card__row span {
        color: var(--text-500);
        font-size: 13px;
    }
    .market-price-card__row strong {
        text-align: right;
        font-size: 15px;
        font-weight: 600;
    }
    .result-card__winner-row {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        align-items: flex-start;
        margin-top: 16px;
    }
    .winner-pill {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 13px;
        font-weight: 700;
        background: var(--green-700);
        color: #FFFFFF;
        border: none;
    }
    .result-card__difference {
        font-size: 14px;
        line-height: 1.6;
        font-weight: 500;
        color: var(--text-500);
    }
    .result-card__meta {
        display: flex;
        justify-content: space-between;
        gap: 1rem;
        align-items: flex-end;
        margin-top: 16px;
        flex-wrap: wrap;
    }
    .result-card__meta-label {
        font-size: 11px;
        font-weight: 700;
        color: var(--text-300);
        margin-bottom: 4px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }
    .result-card__meta-value {
        font-size: 13px;
        line-height: 1.6;
        font-weight: 500;
        color: var(--text-500);
    }
    .status-pill {
        display: inline-block;
        padding: 0.35rem 0.65rem;
        border-radius: 999px;
        font-size: 0.82rem;
        font-weight: 700;
        border: 1px solid transparent;
    }
    .status-safe {
        background: rgba(192, 221, 151, 0.42);
        color: var(--brand-primary);
        border-color: rgba(39, 80, 10, 0.12);
    }
    .status-review {
        background: rgba(255, 249, 232, 0.84);
        color: var(--brand-warning);
        border-color: rgba(130, 109, 40, 0.18);
    }
    .status-single {
        background: rgba(39, 80, 10, 0.08);
        color: var(--brand-primary);
        border-color: rgba(39, 80, 10, 0.12);
    }
    .brand-table-shell {
        margin-top: 0.15rem;
        overflow-x: auto;
    }
    .brand-table {
        width: 100%;
        border-collapse: collapse;
        background: var(--brand-surface);
        font-size: 15px;
    }
    .brand-table th {
        background: transparent;
        color: var(--text-300);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        text-align: left;
        padding: 0 0 10px 0;
        border-bottom: 1px solid var(--brand-border);
    }
    .brand-table td {
        padding: 12px 0;
        border-bottom: 1px solid var(--brand-border);
        line-height: 1.6;
        vertical-align: top;
        background: var(--brand-surface);
    }
    .brand-table tr:last-child td {
        border-bottom: none;
    }
    div[data-testid="stRadio"] {
        margin-top: 0.35rem;
    }
    div[data-testid="stRadio"] > div {
        gap: 0.65rem;
    }
    div[data-testid="stRadio"] label {
        background: var(--brand-surface);
        border: 1px solid var(--brand-border);
        border-radius: 12px;
        padding: 0.72rem 0.9rem;
        align-items: flex-start !important;
    }
    div[data-testid="stRadio"] label:has(input:checked) {
        border-color: var(--brand-primary);
        background: rgba(192, 221, 151, 0.2);
    }
    div[data-testid="stButton"] {
        margin-top: 0.35rem;
    }
    div[data-testid="stButton"] > button {
        width: 100%;
        min-height: auto !important;
        padding: 12px 16px !important;
        border-radius: 12px !important;
        border: 1px solid var(--green-100) !important;
        background: var(--green-50) !important;
        color: var(--green-700) !important;
        font-weight: 700 !important;
        line-height: 1.35 !important;
        box-shadow: none !important;
        transition: background-color 0.18s ease, border-color 0.18s ease, color 0.18s ease;
    }
    div[data-testid="stButton"] > button:hover {
        background: var(--green-100) !important;
        border-color: var(--green-700) !important;
        color: var(--green-700) !important;
    }
    div[data-testid="stButton"] > button:focus,
    div[data-testid="stButton"] > button:focus-visible,
    div[data-testid="stButton"] > button:active {
        outline: none !important;
        box-shadow: none !important;
        border-color: var(--green-700) !important;
        background: var(--green-100) !important;
        color: var(--green-700) !important;
    }
    div[data-testid="stButton"] > button[kind="primary"] {
        background: var(--green-700) !important;
        border-color: var(--green-700) !important;
        color: #FFFFFF !important;
    }
    div[data-testid="stButton"] > button[kind="primary"]:hover,
    div[data-testid="stButton"] > button[kind="primary"]:focus,
    div[data-testid="stButton"] > button[kind="primary"]:focus-visible,
    div[data-testid="stButton"] > button[kind="primary"]:active {
        background: var(--green-900) !important;
        border-color: var(--green-900) !important;
        color: #FFFFFF !important;
    }
    div[data-baseweb="notification"] {
        border-radius: 12px !important;
        border: 1px solid var(--brand-border) !important;
        box-shadow: none !important;
        background: rgba(255, 255, 255, 0.82) !important;
    }
    .status-chip {
        display: inline-flex;
        align-items: center;
        border-radius: 999px;
        padding: 4px 10px;
        font-size: 12px;
        line-height: 1.2;
        font-weight: 600;
        background: #F5F5F1;
        color: var(--text-500);
    }
    .status-chip--single {
        background: #FFF3CC;
        color: #6E5216;
    }
    .status-chip--review {
        background: rgba(255, 249, 232, 0.95);
        color: #7B611A;
    }
    .app-note {
        margin-top: 0.35rem;
        padding: 14px 16px;
        border: none;
        border-left: 3px solid var(--green-700);
        border-radius: 0 12px 12px 0;
        background: var(--green-50);
        line-height: 1.6;
        color: var(--text-500);
        font-size: 14px;
    }
    @keyframes freshnessPulse {
        0% {
            opacity: 0.55;
            transform: scale(0.92);
        }
        50% {
            opacity: 1;
            transform: scale(1.08);
        }
        100% {
            opacity: 0.55;
            transform: scale(0.92);
        }
    }
    @media (max-width: 640px) {
        .block-container {
            padding-left: 24px;
            padding-right: 24px;
        }
        .last-update-note {
            text-align: left;
            margin-top: 10px;
        }
        .hero-section {
            padding-top: 28px;
            margin-top: 0;
        }
        h1.hero-section__title {
            font-size: 30px;
        }
        .search-panel {
            padding: 16px 16px 10px;
        }
        div[data-testid="stTextInput"] {
            padding: 0 16px 16px;
        }
        .result-card__title {
            font-size: 28px;
        }
        .market-price-grid {
            grid-template-columns: 1fr;
        }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    f"""
    <div class="brand-header">
        <div class="brand-header__left">{brand_lockup_html}</div>
    </div>
    <div class="hero-section">
        <div class="hero-section__eyebrow">EN UCUZ NEREDEN ALIRIM?</div>
        <h1 class="hero-section__title">Binlerce ürün,<br>tek arama.</h1>
        <p class="hero-section__subtitle">
            Marketleri karşılaştır, en iyi fiyatı seç.
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)

catalog_df = load_public_catalog(build_catalog_cache_key(freshness_row))
if catalog_df.empty:
    render_inline_message("Şu anda gösterilebilir ürün bulunamadı.", tone="warning")
    st.stop()

st.markdown(
    """
    <div class="search-panel">
        <div class="search-panel__label">Ürün ara</div>
    </div>
    """,
    unsafe_allow_html=True,
)
search_text = st.text_input(
    "Ürün ara",
    key="public_product_search",
    placeholder="domates, süt, salatalık, muz gibi bir ürün yaz",
    autocomplete="off",
    label_visibility="collapsed",
).strip().lower()
st.markdown(
    f'<div class="last-update-note">Son güncelleme: {latest_update_display}</div>',
    unsafe_allow_html=True,
)
if not search_text:
    render_empty_state()
    st.stop()

if len(search_text) < 2:
    render_inline_message("Daha iyi sonuç için en az 2 karakter yaz.", tone="info")
    st.stop()

search_grouping_started_at = perf_counter()
search_sections = build_search_group_sections(catalog_df, search_text)
logger.info(
    "build_search_group_sections finished in %.3fs query=%r safe=%s related=%s",
    perf_counter() - search_grouping_started_at,
    search_text,
    len(search_sections["safe_groups"]),
    len(search_sections["related_groups"]),
)
safe_groups = search_sections["safe_groups"][:SEARCH_RESULT_LIMIT]
related_groups = search_sections["related_groups"][:SEARCH_RESULT_LIMIT]
sync_subtype_selection_state(search_text, safe_groups, related_groups)

if not safe_groups and not related_groups:
    render_inline_message("Eşleşen ürün bulunamadı.", tone="warning")
    st.stop()

selected_group = None
recommendation = None

if safe_groups:
    group_lookup = {
        group["selection_id"]: group
        for group in safe_groups
    }
    (
        selected_selection_id,
        selected_group,
        selection_ids,
    ) = resolve_selected_group(
        group_lookup,
        "public_safe_result_selection",
    )

    recommendation = render_selected_group_summary(
        selected_group,
        catalog_df,
        freshness_row,
    )

    st.markdown("### Doğrudan karşılaştırılabilir sonuçlar")
    st.radio(
        "Doğrudan karşılaştırılabilir sonuçlar",
        selection_ids,
        index=selection_ids.index(selected_selection_id),
        format_func=lambda selection_id: format_search_option(
            group_lookup[selection_id],
            catalog_df,
        ),
        key="public_safe_result_selection",
        label_visibility="collapsed",
    )

    if related_groups:
        related_results_df = build_related_results_df(related_groups, catalog_df)
        if not related_results_df.empty:
            st.markdown(f"### {related_section_title(search_text)}")
            render_inline_message(
                "Paket gramajı veya ürün türü farklı olabilir; bu yüzden otomatik fiyat karşılaştırmasına alınmadı.",
                tone="muted",
            )
            render_related_results_table(related_results_df)
else:
    group_lookup = {
        group["selection_id"]: group
        for group in related_groups
    }

    st.markdown("### Farklı türler")
    render_inline_message(
        "Bu ürün için farklı türler bulundu, seçim yapınız",
        tone="info",
    )
    selected_selection_id = st.session_state.get(SUBTYPE_SELECTION_STATE_KEY)
    if selected_selection_id not in group_lookup:
        render_subtype_options(group_lookup, catalog_df)
        st.stop()

    if st.button("Farklı tür seç", key="clear_selected_subtype", type="primary"):
        st.session_state[SUBTYPE_SELECTION_STATE_KEY] = None
        st.rerun()

    selected_group = group_lookup[selected_selection_id]
    recommendation = render_selected_group_summary(
        selected_group,
        catalog_df,
        freshness_row,
    )

if recommendation.get("family_label"):
    render_inline_message(
        "Genel ürün aramalarında aynı aile içindeki seçenekler tek bir niyet olarak ele alınır. "
        "Bu yüzden benzer SKU'lar ayrı ayrı sonuç olarak yığılmaz.",
        tone="muted",
    )

mismatch_note = get_measurement_mismatch_label(recommendation)
if (
    mismatch_note
    and recommendation.get("coverage_status") != "comparable"
    and recommendation.get("recommended_retailer") is None
):
    render_inline_message(f"Kontrol notu: {mismatch_note}", tone="warning")
elif (
    mismatch_note
    and recommendation.get("coverage_status") != "comparable"
    and recommendation.get("comparison_confidence") != "high"
):
    render_inline_message(f"Ölçü notu: {mismatch_note}", tone="info")

if freshness_row:
    render_freshness_card(freshness_row)

st.markdown("### Not")
st.markdown(
    """
    <div class="app-note">
        Bu sayfa hızlı karar vermek için tasarlandı. Sepet tasarrufu, alarm ve şehir bazlı gelişmiş akışlar
        sonraki adımlarda bunun üstüne gelecek.
    </div>
    """,
    unsafe_allow_html=True,
)
