import base64
import html
import logging
import re
import sys
from datetime import date, datetime
from pathlib import Path
from time import perf_counter
from urllib.parse import quote

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
from pipeline.optimizer.cleaning_products import (
    augment_catalog_with_cleaning_rows,
    cleaning_brand_from_query_tokens,
    nearest_cleaning_brand_token,
    cleaning_tokens,
    infer_cleaning_product_profile,
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
from app.search_selection import (
    SEARCH_MODE_BRAND,
    SEARCH_MODE_CATEGORY,
    RESULT_STATUS_REVIEW_REQUIRED,
    RESULT_STATUS_SAFE,
    RESULT_STATUS_SINGLE_MARKET,
    build_brand_filter_options,
    build_category_row_price_model,
    build_group_result_rows,
    build_cleaning_sibling_product_groups,
    build_cleaning_selection_button_key,
    build_cleaning_family_product_groups,
    build_brand_only_cleaning_groups,
    build_category_product_results,
    build_category_result_sections,
    build_unified_category_results,
    combine_selection_groups,
    dedupe_compact_result_rows,
    detect_search_mode,
    format_compact_category_display_name,
    has_hidden_brand_result_rows,
    has_hidden_cleaning_family_product_groups,
    is_brand_only_cleaning_query,
    limit_brand_result_rows,
    limit_cleaning_family_product_groups,
    normalize_result_status,
    preserve_or_reset_cleaning_product_selection,
    resolve_category_filter_selection,
    resolve_category_result_selection,
    resolved_cleaning_brand_token,
    select_brand_only_cleaning_default_group,
    sort_brand_result_rows,
    sort_specific_result_rows,
)
from queries import GLOBAL_FRESHNESS_QUERY, PUBLIC_PRODUCT_CATALOG_QUERY


COVERAGE_LABELS = {
    "comparable": "Karşılaştırılabilir",
    "comparison_review_required": "Benzer ürün",
    "only_a101": "Tek markette",
    "only_migros": "Tek markette",
    "only_available_at_a101": "Tek markette",
    "only_available_at_migros": "Tek markette",
    "unavailable": "Uygun değil",
}
RETAILER_LABELS = {
    "a101": "A101",
    "migros": "Migros",
}
SEARCH_RESULT_LIMIT = 8
CATEGORY_RESULT_LIMIT = 12
SUBTYPE_SELECTION_STATE_KEY = "selected_subtype"
SUBTYPE_SELECTION_QUERY_STATE_KEY = "selected_subtype_query"
CATEGORY_SELECTION_STATE_KEY = "selected_category_product"
CATEGORY_SELECTION_QUERY_STATE_KEY = "selected_category_query"
CATEGORY_FILTER_STATE_KEY = "selected_category_filter"
CATEGORY_FILTER_QUERY_STATE_KEY = "selected_category_filter_query"
CATEGORY_ROW_QUERY_PARAM = "category_row"
CATEGORY_FILTER_QUERY_PARAM = "category_filter"
SEARCH_TEXT_QUERY_PARAM = "q"
CLEANING_FAMILY_SELECTION_STATE_KEY = "selected_cleaning_subtype"
CLEANING_PRODUCT_SELECTION_STATE_KEY = "selected_cleaning_subtype_product"
CLEANING_PRODUCT_EXPANDED_STATE_KEY = "selected_cleaning_subtype_expanded"
BRAND_FILTER_STATE_KEY = "selected_brand_filter"
BRAND_FILTER_QUERY_STATE_KEY = "selected_brand_filter_query"
BRAND_RESULT_SELECTION_STATE_KEY = "selected_brand_result"
BRAND_RESULT_SELECTION_QUERY_STATE_KEY = "selected_brand_result_query"
BRAND_LIST_EXPANDED_STATE_KEY = "selected_brand_list_expanded"
BRAND_LIST_EXPANDED_QUERY_STATE_KEY = "selected_brand_list_expanded_query"
BRAND_FILTER_QUERY_PARAM = "brand_filter"
BRAND_ROW_QUERY_PARAM = "brand_row"
PUBLIC_RESULT_SELECTION_STATE_KEY = "public_result_selection"
PUBLIC_RESULT_SELECTION_QUERY_STATE_KEY = "public_result_selection_query"
PUBLIC_ROW_QUERY_PARAM = "selection"
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


def freshness_display_model(freshness_row: dict | None) -> dict[str, str]:
    if not freshness_row:
        return {
            "latest_success": "-",
            "latest_price_observation": "-",
            "latest_data_date": "-",
        }

    return {
        "latest_success": format_metric_value(freshness_row.get("latest_success_started_at")),
        "latest_price_observation": format_metric_value(
            freshness_row.get("latest_price_observed_at")
            or freshness_row.get("latest_data_date")
        ),
        "latest_data_date": format_metric_value(freshness_row.get("latest_data_date")),
    }


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


def ui_status_label(status_key: str) -> str:
    if status_key == RESULT_STATUS_SAFE:
        return "Karşılaştırılabilir"
    if status_key == RESULT_STATUS_SINGLE_MARKET:
        return "Tek markette"
    return "Benzer ürün"


def status_theme(label: str) -> str:
    if label == "Karşılaştırılabilir":
        return "status-safe"
    if label == "Tek markette":
        return "status-single"
    return "status-review"


def result_title(group: dict, recommendation: dict) -> str:
    family_label = recommendation.get("family_label") or group.get("family_label")
    if family_label:
        return family_label

    preferred_sources = []
    recommended_retailer = recommendation.get("recommended_retailer")
    if recommended_retailer == "migros":
        preferred_sources.extend(
            [
                recommendation.get("migros_source_product_name"),
                recommendation.get("a101_source_product_name"),
            ]
        )
    else:
        preferred_sources.extend(
            [
                recommendation.get("a101_source_product_name"),
                recommendation.get("migros_source_product_name"),
            ]
        )

    for source_name in preferred_sources:
        if source_name:
            return str(source_name)
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
    status_label = ui_status_label(
        normalize_result_status(row.iloc[0].get("coverage_status"))
    )
    coverage_label = format_coverage(row.iloc[0].get("coverage_status"))
    return f"{display_name} • {status_label} • {coverage_label}"


def format_related_group_option(group: dict, catalog_df: pd.DataFrame) -> str:
    if group.get("selection_type") == "product_family":
        return group.get("family_label") or format_product_family_group(group)
    return format_search_option(group, catalog_df)


def format_money_tr(value) -> str:
    if value is None:
        return "-"

    try:
        formatted = f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return "-"

    return f"{formatted.replace(',', '_').replace('.', ',').replace('_', '.')} TL"


def format_unit_price(value, unit_label: str | None) -> str:
    money = format_money(value)
    if money == "-" or not unit_label:
        return money
    return f"{money}/{unit_label}"


def format_unit_price_tr(value, unit_label: str | None) -> str:
    money = format_money_tr(value)
    if money == "-" or not unit_label:
        return money
    return f"{money}/{unit_label}"


def compact_category_title(search_text: str) -> str:
    if not search_text:
        return "Ürün"
    return search_text[:1].upper() + search_text[1:]


def extract_compact_size_label(display_name: str, result: dict) -> str | None:
    measurement_patterns = [
        r"(\d+)\s*['’]?(?:li|lu)\b",
        r"(\d+(?:[.,]\d+)?)\s*(ml|l|kg|g)\b",
    ]
    for pattern in measurement_patterns:
        match = re.search(pattern, str(display_name), flags=re.IGNORECASE)
        if not match:
            continue

        groups = match.groups()
        if not groups:
            continue

        value = groups[0]
        unit = groups[1] if len(groups) > 1 else None
        if unit is None:
            return f"{value}'li"

        normalized_unit = unit.lower()
        if normalized_unit == "l":
            return f"{value.replace(',', '.')} L"
        if normalized_unit == "ml":
            return f"{value.replace(',', '.')} ml"
        if normalized_unit == "kg":
            return f"{value.replace(',', '.')} kg"
        if normalized_unit == "g":
            return f"{value.replace(',', '.')} g"

    unit_label = result.get("best_unit_label")
    if unit_label == "litre":
        quantity_value = None
        group_name = (result.get("group") or {}).get("product_names", [None])[0]
        for candidate_name in filter(None, [display_name, group_name]):
            liter_match = re.search(r"(\d+(?:[.,]\d+)?)\s*l\b", str(candidate_name), flags=re.IGNORECASE)
            ml_match = re.search(r"(\d+(?:[.,]\d+)?)\s*ml\b", str(candidate_name), flags=re.IGNORECASE)
            if ml_match:
                quantity_value = float(ml_match.group(1).replace(",", ".")) / 1000
                break
            if liter_match:
                quantity_value = float(liter_match.group(1).replace(",", "."))
                break
        if quantity_value is not None:
            if quantity_value < 1:
                return f"{int(round(quantity_value * 1000))} ml"
            if float(quantity_value).is_integer():
                return f"{int(quantity_value)} L"
            return f"{quantity_value:g} L"
    return None


def compact_category_product_name(result: dict) -> str:
    return format_compact_category_display_name(result)


def retailer_badge_html(retailer: str | None) -> str:
    if not retailer:
        return ""
    label = format_retailer(retailer)
    return f'<span class="compact-badge compact-badge--retailer">{html.escape(label)}</span>'


def category_retailer_badge_html(result: dict) -> str:
    coverage_status = result.get("coverage_status")
    if coverage_status in {"comparable", "comparison_review_required"}:
        return '<span class="compact-badge compact-badge--retailer">A101 + Migros</span>'
    return retailer_badge_html(result.get("best_retailer"))


def category_status_badge_html(result: dict) -> str:
    status_key = normalize_result_status(result.get("coverage_status"))
    if status_key == RESULT_STATUS_SAFE:
        label = ui_status_label(status_key)
        css_class = "compact-badge compact-badge--status-safe"
    elif status_key == RESULT_STATUS_SINGLE_MARKET:
        label = ui_status_label(status_key)
        css_class = "compact-badge compact-badge--status-single"
    else:
        label = ui_status_label(status_key)
        css_class = "compact-badge compact-badge--status-review"
    return f'<span class="{css_class}">{html.escape(label)}</span>'


def category_measurement_badge_html(result: dict) -> str:
    if result.get("category_display_section") != "secondary":
        return ""

    primary_unit_label = result.get("category_primary_unit_label")
    current_unit_label = result.get("best_unit_label")
    if primary_unit_label and current_unit_label and current_unit_label != primary_unit_label:
        return '<span class="compact-badge compact-badge--status-measure">Farklı ölçü</span>'
    return '<span class="compact-badge compact-badge--status-review">Benzer ürün</span>'


def cheapest_badge_html() -> str:
    return '<span class="compact-badge compact-badge--cheapest">En ucuz</span>'


def category_sort_hint(results: list[dict]) -> str:
    if not results:
        return "Fiyat sıralı"
    unit_label = results[0].get("best_unit_label")
    if not unit_label:
        return "Fiyat sıralı"
    return f"TL/{unit_label} sıralı"


def format_category_result_option(result: dict) -> str:
    retailer_label = format_retailer(result.get("best_retailer"))
    best_price = format_money(result.get("best_price"))
    unit_price = format_unit_price(
        result.get("best_unit_price"),
        result.get("best_unit_label"),
    )
    status_label = ui_status_label(
        normalize_result_status(result.get("coverage_status"))
    )
    prefix = "En ucuz • " if result.get("is_cheapest") else ""
    return (
        f"{prefix}{result.get('display_name')} • "
        f"{retailer_label} {best_price} • {unit_price} • {status_label}"
    )


def sync_subtype_selection_state(
    search_text: str,
    manual_selection_enabled: bool,
    selectable_groups: list[dict],
) -> None:
    previous_query = st.session_state.get(SUBTYPE_SELECTION_QUERY_STATE_KEY)
    if previous_query != search_text:
        st.session_state[SUBTYPE_SELECTION_QUERY_STATE_KEY] = search_text
        st.session_state[SUBTYPE_SELECTION_STATE_KEY] = None

    if not manual_selection_enabled:
        st.session_state[SUBTYPE_SELECTION_STATE_KEY] = None
        return

    valid_selection_ids = {group["selection_id"] for group in selectable_groups}
    if st.session_state.get(SUBTYPE_SELECTION_STATE_KEY) not in valid_selection_ids:
        st.session_state[SUBTYPE_SELECTION_STATE_KEY] = None


def sync_category_selection_state(
    search_text: str,
    category_results: list[dict],
) -> None:
    previous_query = st.session_state.get(CATEGORY_SELECTION_QUERY_STATE_KEY)
    if previous_query != search_text:
        st.session_state[CATEGORY_SELECTION_QUERY_STATE_KEY] = search_text
        st.session_state[CATEGORY_SELECTION_STATE_KEY] = None

    valid_selection_ids = {
        result["group"]["selection_id"]
        for result in category_results
    }
    if st.session_state.get(CATEGORY_SELECTION_STATE_KEY) not in valid_selection_ids:
        st.session_state[CATEGORY_SELECTION_STATE_KEY] = None


def sync_public_result_selection_state(
    search_text: str,
    result_rows: list[dict],
) -> None:
    previous_query = st.session_state.get(PUBLIC_RESULT_SELECTION_QUERY_STATE_KEY)
    if previous_query != search_text:
        st.session_state[PUBLIC_RESULT_SELECTION_QUERY_STATE_KEY] = search_text
        st.session_state[PUBLIC_RESULT_SELECTION_STATE_KEY] = None

    valid_selection_ids = {
        result["group"]["selection_id"]
        for result in result_rows
    }
    if st.session_state.get(PUBLIC_RESULT_SELECTION_STATE_KEY) not in valid_selection_ids:
        st.session_state[PUBLIC_RESULT_SELECTION_STATE_KEY] = None


def query_param_value(name: str) -> str | None:
    value = st.query_params.get(name)
    if value is None:
        return None
    if isinstance(value, list):
        if not value:
            return None
        value = value[-1]
    text = str(value).strip()
    return text or None


def clear_query_param(name: str) -> None:
    try:
        if name in st.query_params:
            del st.query_params[name]
    except Exception:
        pass


def hydrate_search_input_from_query_params() -> None:
    query_text = query_param_value(SEARCH_TEXT_QUERY_PARAM)
    if not query_text:
        return
    current_search_text = st.session_state.get("public_product_search")
    if not current_search_text:
        st.session_state["public_product_search"] = query_text


def build_category_row_href(
    search_text: str,
    selected_filter_id: str | None,
    selection_id: str,
) -> str:
    params = [
        f"{SEARCH_TEXT_QUERY_PARAM}={quote(search_text, safe='')}",
        f"{CATEGORY_ROW_QUERY_PARAM}={quote(selection_id, safe='')}",
    ]
    if selected_filter_id:
        params.append(
            f"{CATEGORY_FILTER_QUERY_PARAM}={quote(str(selected_filter_id), safe='')}"
        )
    return f"?{'&'.join(params)}#category-results"


def build_public_row_href(
    search_text: str,
    selection_id: str,
) -> str:
    params = [
        f"{SEARCH_TEXT_QUERY_PARAM}={quote(search_text, safe='')}",
        f"{PUBLIC_ROW_QUERY_PARAM}={quote(selection_id, safe='')}",
    ]
    return f"?{'&'.join(params)}#search-results"


def build_category_filter_href(
    search_text: str,
    filter_id: str,
    selection_id: str | None = None,
) -> str:
    params = [
        f"{SEARCH_TEXT_QUERY_PARAM}={quote(search_text, safe='')}",
        f"{CATEGORY_FILTER_QUERY_PARAM}={quote(str(filter_id), safe='')}",
    ]
    if selection_id:
        params.append(
            f"{CATEGORY_ROW_QUERY_PARAM}={quote(selection_id, safe='')}"
        )
    return f"?{'&'.join(params)}#category-results"


def build_brand_row_href(
    search_text: str,
    filter_id: str | None,
    selection_id: str,
) -> str:
    params = [
        f"{SEARCH_TEXT_QUERY_PARAM}={quote(search_text, safe='')}",
        f"{BRAND_ROW_QUERY_PARAM}={quote(selection_id, safe='')}",
    ]
    if filter_id:
        params.append(
            f"{BRAND_FILTER_QUERY_PARAM}={quote(str(filter_id), safe='')}"
        )
    return f"?{'&'.join(params)}#search-results"


def build_brand_filter_href(
    search_text: str,
    filter_id: str,
    selection_id: str | None = None,
) -> str:
    params = [
        f"{SEARCH_TEXT_QUERY_PARAM}={quote(search_text, safe='')}",
        f"{BRAND_FILTER_QUERY_PARAM}={quote(str(filter_id), safe='')}",
    ]
    if selection_id:
        params.append(
            f"{BRAND_ROW_QUERY_PARAM}={quote(selection_id, safe='')}"
        )
    return f"?{'&'.join(params)}#search-results"


def sync_category_filter_state(
    search_text: str,
    filter_ids: list[str],
    default_filter_id: str | None,
) -> None:
    previous_query = st.session_state.get(CATEGORY_FILTER_QUERY_STATE_KEY)
    if previous_query != search_text:
        st.session_state[CATEGORY_FILTER_QUERY_STATE_KEY] = search_text
        st.session_state[CATEGORY_FILTER_STATE_KEY] = default_filter_id

    if st.session_state.get(CATEGORY_FILTER_STATE_KEY) not in filter_ids:
        st.session_state[CATEGORY_FILTER_STATE_KEY] = default_filter_id


def sync_brand_filter_state(
    search_text: str,
    filter_ids: list[str],
    default_filter_id: str | None,
) -> None:
    previous_query = st.session_state.get(BRAND_FILTER_QUERY_STATE_KEY)
    if previous_query != search_text:
        st.session_state[BRAND_FILTER_QUERY_STATE_KEY] = search_text
        st.session_state[BRAND_FILTER_STATE_KEY] = default_filter_id

    if st.session_state.get(BRAND_FILTER_STATE_KEY) not in filter_ids:
        st.session_state[BRAND_FILTER_STATE_KEY] = default_filter_id


def sync_brand_result_selection_state(
    search_text: str,
    result_rows: list[dict],
) -> None:
    previous_query = st.session_state.get(BRAND_RESULT_SELECTION_QUERY_STATE_KEY)
    if previous_query != search_text:
        st.session_state[BRAND_RESULT_SELECTION_QUERY_STATE_KEY] = search_text
        st.session_state[BRAND_RESULT_SELECTION_STATE_KEY] = None

    valid_selection_ids = {
        result["group"]["selection_id"]
        for result in result_rows
    }
    if st.session_state.get(BRAND_RESULT_SELECTION_STATE_KEY) not in valid_selection_ids:
        st.session_state[BRAND_RESULT_SELECTION_STATE_KEY] = None


def sync_brand_result_expansion_state(
    search_text: str,
    filter_id: str | None,
) -> None:
    expansion_context = f"{search_text}|{filter_id or 'all'}"
    previous_context = st.session_state.get(BRAND_LIST_EXPANDED_QUERY_STATE_KEY)
    if previous_context != expansion_context:
        st.session_state[BRAND_LIST_EXPANDED_QUERY_STATE_KEY] = expansion_context
        st.session_state[BRAND_LIST_EXPANDED_STATE_KEY] = False


def sync_brand_cleaning_selection_state(
    search_text: str,
    family_groups: list[dict],
    product_groups: list[dict] | None = None,
) -> None:
    previous_query = st.session_state.get(SUBTYPE_SELECTION_QUERY_STATE_KEY)
    if previous_query != search_text:
        st.session_state[SUBTYPE_SELECTION_QUERY_STATE_KEY] = search_text
        st.session_state[CLEANING_FAMILY_SELECTION_STATE_KEY] = None
        st.session_state[CLEANING_PRODUCT_SELECTION_STATE_KEY] = None
        st.session_state[CLEANING_PRODUCT_EXPANDED_STATE_KEY] = False

    valid_family_ids = {group["selection_id"] for group in family_groups}
    selected_family_id = st.session_state.get(CLEANING_FAMILY_SELECTION_STATE_KEY)
    if selected_family_id not in valid_family_ids:
        st.session_state[CLEANING_FAMILY_SELECTION_STATE_KEY] = None
        st.session_state[CLEANING_PRODUCT_SELECTION_STATE_KEY] = None
        st.session_state[CLEANING_PRODUCT_EXPANDED_STATE_KEY] = False
        return

    previous_family_id = st.session_state.get("selected_cleaning_subtype_previous")
    if previous_family_id != selected_family_id:
        st.session_state["selected_cleaning_subtype_previous"] = selected_family_id
        st.session_state[CLEANING_PRODUCT_SELECTION_STATE_KEY] = None
        st.session_state[CLEANING_PRODUCT_EXPANDED_STATE_KEY] = False

    st.session_state[CLEANING_PRODUCT_SELECTION_STATE_KEY] = (
        preserve_or_reset_cleaning_product_selection(
            st.session_state.get(CLEANING_PRODUCT_SELECTION_STATE_KEY),
            product_groups,
        )
    )


def render_selection_buttons(
    group_lookup: dict[str, dict],
    catalog_df: pd.DataFrame,
    state_key: str,
    formatter,
    key_builder=None,
) -> None:
    for selection_id, group in group_lookup.items():
        button_key = (
            key_builder(selection_id, group)
            if key_builder is not None
            else f"{state_key}_{selection_id}"
        )
        if st.button(
            formatter(group, catalog_df),
            key=button_key,
            use_container_width=True,
            type="secondary",
        ):
            st.session_state[state_key] = selection_id
            st.rerun()


def render_subtype_options(group_lookup: dict[str, dict], catalog_df: pd.DataFrame) -> None:
    render_selection_buttons(
        group_lookup,
        catalog_df,
        SUBTYPE_SELECTION_STATE_KEY,
        format_related_group_option,
    )


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
                "Durum": ui_status_label(
                    normalize_result_status(row.get("coverage_status"))
                ),
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
    status_label = ui_status_label(
        normalize_result_status(recommendation.get("coverage_status"))
    )
    status_class = status_theme(status_label)
    product_title = result_title(group, recommendation)
    chosen_option = chosen_option_label(recommendation)
    freshness_text = freshness_display_model(freshness_row)["latest_success"]
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
        '<div class="result-card__meta-label">Son başarılı tarama</div>'
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
        if status_value == "Tek markette":
            status_class += " status-chip--single"
        elif status_value == "Karşılaştırılabilir":
            status_class += " status-chip--safe"
        elif status_value == "Benzer ürün":
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
    freshness_model = freshness_display_model(freshness_row)
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
                        <span>Son başarılı tarama</span>
                        <strong>{freshness_model["latest_success"]}</strong>
                    </p>
                    <p class="freshness-card__line">
                        <span>En yeni fiyat gözlemi</span>
                        <strong>{freshness_model["latest_price_observation"]}</strong>
                    </p>
                    <p class="freshness-card__line">
                        <span>Günlük fiyat özeti tarihi</span>
                        <strong>{freshness_model["latest_data_date"]}</strong>
                    </p>
                </div>
                <div class="{status_class}">
                    <span class="freshness-card__status-dot" aria-hidden="true"></span>
                    <span>{status_text}</span>
                </div>
            </div>
            <p class="freshness-card__note">
                Tarama zamanı, en yeni fiyat gözlemi ve günlük özet tarihi ayrı gösterilir.
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


def render_product_comparison(
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
    with st.expander("Fiyat detayları", expanded=False):
        render_price_rows(recommendation, display_recommendation)

    return recommendation


def render_selected_group_summary(
    selected_group: dict,
    catalog_df: pd.DataFrame,
    freshness_row: dict | None,
) -> dict:
    return render_product_comparison(
        selected_group,
        catalog_df,
        freshness_row,
    )


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
    cleaning_augmentation_started_at = perf_counter()
    catalog_df = augment_catalog_with_cleaning_rows(catalog_df)
    logger.info(
        "load_public_catalog cleaning augmentation finished in %.3fs rows=%s",
        perf_counter() - cleaning_augmentation_started_at,
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


def render_category_filter_chips(
    search_text: str,
    category_sections: dict[str, object],
    active_filter_id: str | None,
    selected_selection_id: str | None,
) -> None:
    filter_options = category_sections.get("filters", [])
    if not filter_options:
        return

    chip_html_parts: list[str] = []
    for filter_option in filter_options:
        filter_id = filter_option["id"]
        filter_label = str(filter_option.get("label") or "").strip() or "Tümü"
        valid_selection_ids = {
            result["group"]["selection_id"]
            for result in build_unified_category_results(
                category_sections,
                filter_id,
                limit=None,
            )
        }
        preserved_selection_id = (
            selected_selection_id
            if selected_selection_id in valid_selection_ids
            else None
        )
        chip_class = "category-chip"
        if filter_id == active_filter_id:
            chip_class += " category-chip--active"
        chip_html_parts.append(
            (
                f'<a class="{chip_class}" '
                f'href="{build_category_filter_href(search_text, filter_id, preserved_selection_id)}">'
                f'{html.escape(filter_label)}'
                "</a>"
            )
        )

    st.markdown(
        '<div class="category-chip-bar">'
        + "".join(chip_html_parts)
        + "</div>",
        unsafe_allow_html=True,
    )


def render_compact_category_result_rows(
    results: list[dict],
    selected_selection_id: str | None,
) -> None:
    for index, result in enumerate(results, start=1):
        selection_id = result["group"]["selection_id"]
        compact_name = compact_category_product_name(result)
        display_name = str(result.get("display_name") or "")
        subtitle_parts = []
        if compact_name != display_name:
            subtitle_parts.append(display_name)
        if result.get("coverage_status") != "comparable":
            subtitle_parts.append(
                result.get("comparison_status_label")
                or format_coverage(result.get("coverage_status"))
            )

        subtitle_html = (
            f'<div class="compact-result-subline">{" • ".join(html.escape(part) for part in subtitle_parts)}</div>'
            if subtitle_parts
            else ""
        )
        badge_html = retailer_badge_html(result.get("best_retailer"))
        if result.get("is_visible_cheapest"):
            badge_html += cheapest_badge_html()
        if selection_id == selected_selection_id:
            badge_html += '<span class="compact-badge compact-badge--retailer">Seçili</span>'

        row_columns = st.columns([0.45, 5.0, 1.8, 1.6, 0.9])
        row_columns[0].markdown(
            f'<div class="compact-result-rank">{index}</div>',
            unsafe_allow_html=True,
        )
        row_columns[1].markdown(
            (
                f'<div class="compact-result-name">{html.escape(compact_name)}</div>'
                f"{subtitle_html}"
            ),
            unsafe_allow_html=True,
        )
        row_columns[2].markdown(
            f'<div class="compact-result-badges">{badge_html}</div>',
            unsafe_allow_html=True,
        )
        row_columns[3].markdown(
            (
                f'<div class="compact-result-price">{html.escape(format_money_tr(result.get("best_price")))}</div>'
                f'<div class="compact-result-unit">{html.escape(format_unit_price_tr(result.get("best_unit_price"), result.get("best_unit_label")))}</div>'
            ),
            unsafe_allow_html=True,
        )
        if row_columns[4].button(
            "Seç",
            key=f"category_row_{selection_id}",
            use_container_width=True,
            type="secondary",
        ):
            st.session_state[CATEGORY_SELECTION_STATE_KEY] = selection_id
            st.rerun()


def render_compact_result_list(
    results: list[dict],
    selected_selection_id: str | None,
    row_href_builder,
    container_id: str,
) -> None:
    if not results:
        return

    row_html_parts: list[str] = []
    for index, result in enumerate(results, start=1):
        selection_id = result["group"]["selection_id"]
        compact_name = compact_category_product_name(result)
        display_name = str(result.get("display_name") or "")
        subtitle_parts = []
        if compact_name != display_name:
            subtitle_parts.append(display_name)

        subtitle_html = (
            f'<div class="compact-result-subline">{" • ".join(html.escape(part) for part in subtitle_parts)}</div>'
            if subtitle_parts
            else ""
        )
        price_model = build_category_row_price_model(result)
        primary_price_text = (
            format_unit_price_tr(
                price_model.get("primary_value"),
                price_model.get("primary_unit_label"),
            )
            if price_model.get("primary_kind") == "unit_price"
            else format_money_tr(price_model.get("primary_value"))
        )
        secondary_price_text = (
            f"Raf: {format_money_tr(price_model.get('secondary_value'))}"
            if price_model.get("secondary_kind") == "shelf_price"
            else ""
        )
        badge_parts = [
            category_retailer_badge_html(result),
            category_status_badge_html(result),
        ]
        measurement_badge = category_measurement_badge_html(result)
        if measurement_badge:
            badge_parts.append(measurement_badge)
        if result.get("is_visible_cheapest"):
            badge_parts.append(cheapest_badge_html())
        if selection_id == selected_selection_id:
            badge_parts.append('<span class="compact-badge compact-badge--selected">Seçili</span>')

        row_class = "compact-result-row"
        if selection_id == selected_selection_id:
            row_class += " compact-result-row--selected"

        row_href = row_href_builder(selection_id)
        row_html_parts.append(
            (
                f'<a class="compact-result-link" href="{row_href}">'
                f'<div class="{row_class}">'
                '<div class="compact-result-grid">'
                f'<div class="compact-result-rank">{index}</div>'
                '<div class="compact-result-main">'
                f'<div class="compact-result-name">{html.escape(compact_name)}</div>'
                f"{subtitle_html}"
                f'<div class="compact-result-badges">{"".join(part for part in badge_parts if part)}</div>'
                "</div>"
                '<div class="compact-result-aside">'
                f'<div class="compact-result-price">{html.escape(primary_price_text)}</div>'
                f'<div class="compact-result-unit">{html.escape(secondary_price_text)}</div>'
                "</div>"
                "</div>"
                "</div>"
                "</a>"
            )
        )

    st.markdown(
        f'<div id="{html.escape(container_id)}" class="compact-results-shell"><div class="compact-results-list">'
        + "".join(row_html_parts)
        + "</div></div>",
        unsafe_allow_html=True,
    )


def render_brand_filter_chips(
    search_text: str,
    filter_options: list[dict[str, str]],
    active_filter_id: str | None,
    selected_selection_id: str | None,
) -> None:
    if not filter_options:
        return

    chip_html_parts: list[str] = []
    for filter_option in filter_options:
        filter_id = str(filter_option.get("id") or "all")
        filter_label = str(filter_option.get("label") or "").strip() or "Tümü"
        chip_class = "category-chip"
        if filter_id == active_filter_id:
            chip_class += " category-chip--active"
        chip_html_parts.append(
            (
                f'<a class="{chip_class}" '
                f'href="{build_brand_filter_href(search_text, filter_id, selected_selection_id)}">'
                f"{html.escape(filter_label)}"
                "</a>"
            )
        )

    st.markdown(
        '<div class="category-chip-bar">'
        + "".join(chip_html_parts)
        + "</div>",
        unsafe_allow_html=True,
    )


def render_compact_category_result_list(
    results: list[dict],
    selected_selection_id: str | None,
    search_text: str,
    selected_filter_id: str | None,
) -> None:
    render_compact_result_list(
        results,
        selected_selection_id,
        lambda selection_id: build_category_row_href(
            search_text,
            selected_filter_id,
            selection_id,
        ),
        "category-results",
    )


def render_category_results(
    search_text: str,
    catalog_df: pd.DataFrame,
    freshness_row: dict | None,
) -> dict | None:
    category_results = build_category_product_results(
        catalog_df,
        search_text,
        limit=200,
    )
    if not category_results:
        render_inline_message("Eşleşen ürün bulunamadı.", tone="warning")
        return None

    category_sections = build_category_result_sections(category_results, search_text)
    filter_options = category_sections.get("filters", [])
    filter_ids = [filter_option["id"] for filter_option in filter_options]
    sync_category_filter_state(
        search_text,
        filter_ids,
        category_sections.get("default_filter"),
    )
    clicked_filter_id = query_param_value(CATEGORY_FILTER_QUERY_PARAM)
    resolved_filter_id = resolve_category_filter_selection(
        filter_ids,
        st.session_state.get(CATEGORY_FILTER_STATE_KEY),
        clicked_filter_id,
        category_sections.get("default_filter"),
    )
    st.session_state[CATEGORY_FILTER_STATE_KEY] = resolved_filter_id
    preview_results = build_unified_category_results(
        category_sections,
        resolved_filter_id,
        limit=None,
    )

    st.markdown(
        (
            '<div class="category-results-header">'
            f'<div class="category-results-header__title">{html.escape(compact_category_title(search_text))} — {len(preview_results)} ürün</div>'
            f'<div class="category-results-header__meta">{html.escape(category_sort_hint(preview_results))}</div>'
            "</div>"
        ),
        unsafe_allow_html=True,
    )

    selected_filter_id = resolved_filter_id
    rendered_results = build_unified_category_results(
        category_sections,
        selected_filter_id,
        limit=CATEGORY_RESULT_LIMIT,
    )
    sync_category_selection_state(search_text, rendered_results)
    clicked_selection_id = query_param_value(CATEGORY_ROW_QUERY_PARAM)
    selected_selection_id = resolve_category_result_selection(
        rendered_results,
        st.session_state.get(CATEGORY_SELECTION_STATE_KEY),
        clicked_selection_id,
    )
    st.session_state[CATEGORY_SELECTION_STATE_KEY] = selected_selection_id

    result_lookup = {
        result["group"]["selection_id"]: result
        for result in rendered_results
    }
    selected_result = result_lookup.get(selected_selection_id)
    recommendation = None
    if selected_result:
        st.markdown("### Seçilen ürün")
        recommendation = render_product_comparison(
            selected_result["group"],
            catalog_df,
            freshness_row,
        )

    render_category_filter_chips(
        search_text,
        category_sections,
        selected_filter_id,
        selected_selection_id,
    )

    primary_rendered_results = [
        result
        for result in rendered_results
        if result.get("category_display_section") != "secondary"
    ]
    secondary_rendered_results = [
        result
        for result in rendered_results
        if result.get("category_display_section") == "secondary"
    ]

    if primary_rendered_results:
        render_compact_category_result_list(
            primary_rendered_results,
            selected_selection_id,
            search_text,
            selected_filter_id,
        )

    if secondary_rendered_results:
        st.markdown("### Farklı ölçü")
        render_inline_message(
            "Bu ürünler farklı karşılaştırma ölçüsüyle geldiği için ayrı listeleniyor.",
            tone="muted",
        )
        render_compact_category_result_list(
            secondary_rendered_results,
            selected_selection_id,
            search_text,
            selected_filter_id,
        )

    return recommendation


def render_brand_results(
    search_text: str,
    catalog_df: pd.DataFrame,
    freshness_row: dict | None,
) -> dict | None:
    effective_search_text = search_text
    search_grouping_started_at = perf_counter()
    search_sections = build_search_group_sections(catalog_df, effective_search_text)
    logger.info(
        "build_search_group_sections finished in %.3fs query=%r safe=%s related=%s",
        perf_counter() - search_grouping_started_at,
        effective_search_text,
        len(search_sections["safe_groups"]),
        len(search_sections["related_groups"]),
    )

    all_safe_groups = search_sections["safe_groups"]
    all_related_groups = search_sections["related_groups"]
    search_results = search_sections["search_results"]
    if not all_safe_groups and not all_related_groups:
        fuzzy_brand_token = nearest_cleaning_brand_token(search_text)
        if fuzzy_brand_token:
            effective_search_text = fuzzy_brand_token
            search_grouping_started_at = perf_counter()
            search_sections = build_search_group_sections(catalog_df, effective_search_text)
            logger.info(
                "brand fallback build_search_group_sections finished in %.3fs query=%r safe=%s related=%s",
                perf_counter() - search_grouping_started_at,
                effective_search_text,
                len(search_sections["safe_groups"]),
                len(search_sections["related_groups"]),
            )
            all_safe_groups = search_sections["safe_groups"]
            all_related_groups = search_sections["related_groups"]
            search_results = search_sections["search_results"]
            if all_safe_groups or all_related_groups:
                render_inline_message(
                    "Tam eşleşme bulunamadı, benzer ürünler gösteriliyor.",
                    tone="info",
                )
        if not all_safe_groups and not all_related_groups:
            render_inline_message("Eşleşen ürün bulunamadı.", tone="warning")
            return None

    st.session_state[CLEANING_FAMILY_SELECTION_STATE_KEY] = None
    st.session_state[CLEANING_PRODUCT_SELECTION_STATE_KEY] = None
    st.session_state[CLEANING_PRODUCT_EXPANDED_STATE_KEY] = False

    family_groups = build_brand_only_cleaning_groups(
        catalog_df,
        all_safe_groups,
        all_related_groups,
        effective_search_text,
    )
    if not family_groups:
        render_inline_message("Eşleşen ürün bulunamadı.", tone="warning")
        return None

    family_product_groups_by_key: dict[str, list[dict]] = {}
    product_group_lookup: dict[str, tuple[str, dict]] = {}
    for family_group in family_groups:
        family_id = str(family_group.get("family_id") or "")
        family_key = family_id.split(":")[-1] if family_id else "all"
        product_groups = build_cleaning_family_product_groups(
            search_results,
            family_group,
        )
        family_product_groups_by_key[family_key] = product_groups
        for group in product_groups:
            product_name = (group.get("product_names") or [None])[0]
            if product_name:
                product_group_lookup[product_name] = (family_key, group)

    all_product_groups: list[dict] = []
    seen_selection_ids: set[str] = set()
    for _, row in search_results.iterrows():
        product_name = str(row.get("standardized_product_name") or "")
        family_entry = product_group_lookup.get(product_name)
        if not family_entry:
            continue
        _, group = family_entry
        selection_id = group.get("selection_id")
        if not selection_id or selection_id in seen_selection_ids:
            continue
        all_product_groups.append(group)
        seen_selection_ids.add(selection_id)

    brand_filter_options = build_brand_filter_options(family_groups)
    filter_ids = [str(filter_option.get("id") or "all") for filter_option in brand_filter_options]
    default_filter_id = filter_ids[0] if filter_ids else "all"
    sync_brand_filter_state(search_text, filter_ids, default_filter_id)
    clicked_filter_id = query_param_value(BRAND_FILTER_QUERY_PARAM)
    active_filter_id = resolve_category_filter_selection(
        filter_ids,
        st.session_state.get(BRAND_FILTER_STATE_KEY),
        clicked_filter_id,
        default_filter_id,
    )
    st.session_state[BRAND_FILTER_STATE_KEY] = active_filter_id
    sync_brand_result_expansion_state(search_text, active_filter_id)

    visible_groups = list(all_product_groups)
    if active_filter_id and active_filter_id != "all":
        visible_groups = list(family_product_groups_by_key.get(active_filter_id, []))

    result_rows = dedupe_compact_result_rows(build_group_result_rows(catalog_df, visible_groups))
    result_rows = sort_brand_result_rows(effective_search_text, result_rows)
    recommended_brand_group = select_brand_only_cleaning_default_group(
        catalog_df,
        all_safe_groups,
        effective_search_text,
    )
    sync_brand_result_selection_state(search_text, result_rows)
    clicked_selection_id = query_param_value(BRAND_ROW_QUERY_PARAM)
    selected_selection_id = resolve_category_result_selection(
        result_rows,
        st.session_state.get(BRAND_RESULT_SELECTION_STATE_KEY),
        clicked_selection_id,
    )

    recommended_selection_id = None
    if recommended_brand_group:
        recommended_selection_id = recommended_brand_group.get("selection_id")

    if selected_selection_id is None and result_rows:
        visible_selection_ids = {row["group"]["selection_id"] for row in result_rows}
        if recommended_selection_id in visible_selection_ids:
            selected_selection_id = recommended_selection_id
        else:
            selected_selection_id = result_rows[0]["group"]["selection_id"]
    st.session_state[BRAND_RESULT_SELECTION_STATE_KEY] = selected_selection_id

    row_lookup = {
        row["group"]["selection_id"]: row
        for row in result_rows
    }
    selected_row = row_lookup.get(selected_selection_id)

    st.markdown(
        (
            '<div class="category-results-header">'
            f'<div class="category-results-header__title">{html.escape(compact_category_title(search_text))} — {len(result_rows)} ürün</div>'
            '<div class="category-results-header__meta">Ürün sıralı</div>'
            "</div>"
        ),
        unsafe_allow_html=True,
    )
    render_brand_filter_chips(
        search_text,
        brand_filter_options,
        active_filter_id,
        selected_selection_id,
    )

    recommendation = None
    if selected_row:
        heading = (
            "Önerilen ürün"
            if active_filter_id == "all" and selected_selection_id == recommended_selection_id
            else "Seçilen ürün"
        )
        st.markdown(f"### {heading}")
        recommendation = render_product_comparison(
            selected_row["group"],
            catalog_df,
            freshness_row,
        )

    visible_result_rows = limit_brand_result_rows(
        result_rows,
        selected_selection_id=selected_selection_id,
        limit=SEARCH_RESULT_LIMIT,
        expanded=bool(st.session_state.get(BRAND_LIST_EXPANDED_STATE_KEY)),
    )
    render_compact_result_list(
        visible_result_rows,
        selected_selection_id,
        lambda selection_id: build_brand_row_href(
            search_text,
            active_filter_id,
            selection_id,
        ),
        "search-results",
    )
    if has_hidden_brand_result_rows(result_rows, visible_result_rows):
        expand_label = (
            "Daha az göster"
            if st.session_state.get(BRAND_LIST_EXPANDED_STATE_KEY)
            else "+ daha fazla"
        )
        if st.button(
            expand_label,
            key=f"brand_results_expand_{re.sub(r'[^0-9a-zA-Z_]+', '_', active_filter_id or 'all')}",
            use_container_width=True,
            type="secondary",
        ):
            st.session_state[BRAND_LIST_EXPANDED_STATE_KEY] = not bool(
                st.session_state.get(BRAND_LIST_EXPANDED_STATE_KEY)
            )
            st.rerun()
    return recommendation


def render_specific_results(
    search_text: str,
    catalog_df: pd.DataFrame,
    freshness_row: dict | None,
) -> dict | None:
    effective_search_text = search_text
    search_grouping_started_at = perf_counter()
    search_sections = build_search_group_sections(catalog_df, effective_search_text)
    logger.info(
        "build_search_group_sections finished in %.3fs query=%r safe=%s related=%s",
        perf_counter() - search_grouping_started_at,
        effective_search_text,
        len(search_sections["safe_groups"]),
        len(search_sections["related_groups"]),
    )

    safe_groups = search_sections["safe_groups"][:SEARCH_RESULT_LIMIT]
    related_groups = search_sections["related_groups"][:SEARCH_RESULT_LIMIT]
    fallback_message_shown = False

    if not safe_groups and not related_groups:
        fallback_brand_token = resolved_cleaning_brand_token(search_text)
        if fallback_brand_token:
            fallback_sections = build_search_group_sections(catalog_df, fallback_brand_token)
            fallback_groups = combine_selection_groups(
                fallback_sections["safe_groups"][:SEARCH_RESULT_LIMIT],
                fallback_sections["related_groups"][:SEARCH_RESULT_LIMIT],
            )
            if fallback_groups:
                effective_search_text = fallback_brand_token
                related_groups = fallback_groups
                render_inline_message(
                    "Tam eşleşme bulunamadı, benzer ürünler gösteriliyor.",
                    tone="info",
                )
                fallback_message_shown = True
            else:
                render_inline_message("Eşleşen ürün bulunamadı.", tone="warning")
                return None
        else:
            render_inline_message("Eşleşen ürün bulunamadı.", tone="warning")
            return None

    if not safe_groups and related_groups and not fallback_message_shown:
        render_inline_message(
            "Tam eşleşme bulunamadı, benzer ürünler gösteriliyor.",
            tone="info",
        )

    combined_groups = combine_selection_groups(safe_groups, related_groups)
    compact_rows = build_group_result_rows(catalog_df, combined_groups)
    compact_rows = sort_specific_result_rows(search_text, compact_rows)
    if not compact_rows:
        render_inline_message("Eşleşen ürün bulunamadı.", tone="warning")
        return None

    sync_public_result_selection_state(search_text, compact_rows)
    clicked_selection_id = query_param_value(PUBLIC_ROW_QUERY_PARAM)
    selected_selection_id = resolve_category_result_selection(
        compact_rows,
        st.session_state.get(PUBLIC_RESULT_SELECTION_STATE_KEY),
        clicked_selection_id,
    )
    if selected_selection_id is None and compact_rows:
        selected_selection_id = compact_rows[0]["group"]["selection_id"]
    st.session_state[PUBLIC_RESULT_SELECTION_STATE_KEY] = selected_selection_id

    row_lookup = {
        row["group"]["selection_id"]: row
        for row in compact_rows
    }
    selected_row = row_lookup.get(selected_selection_id)
    recommendation = None
    if selected_row:
        st.markdown("### Seçilen ürün")
        recommendation = render_product_comparison(
            selected_row["group"],
            catalog_df,
            freshness_row,
        )

    if len(compact_rows) > 1:
        st.markdown("### Diğer seçenekler")
        render_inline_message(
            "Benzer seçenekleri aynı listede görebilir, istediğin ürüne dokunarak karşılaştırmayı değiştirebilirsin.",
            tone="muted",
        )
    render_compact_result_list(
        compact_rows,
        selected_selection_id,
        lambda selection_id: build_public_row_href(search_text, selection_id),
        "search-results",
    )
    return recommendation


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
        font-size: 16px;
        line-height: 1.6;
    }
    p, div, span, label, input, textarea, button, table {
        color: var(--brand-text);
        font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        font-size: 16px;
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
    .category-results-header {
        display: flex;
        justify-content: space-between;
        gap: 1rem;
        align-items: flex-end;
        margin: 12px 0 8px;
        flex-wrap: wrap;
    }
    .category-results-header__title {
        font-family: Georgia, "Times New Roman", serif;
        font-size: 1.1rem;
        font-weight: 700;
        color: var(--text-900);
    }
    .category-results-header__meta {
        font-size: 12px;
        color: var(--text-300);
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }
    .category-chip-bar {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        margin: 12px 0 12px;
    }
    .category-chip {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        padding: 10px 14px;
        border-radius: 999px;
        border: 1px solid var(--green-100);
        background: rgba(255, 255, 255, 0.92);
        color: var(--green-700);
        font-size: 14px;
        font-weight: 700;
        line-height: 1.2;
        text-decoration: none !important;
        transition: background-color 0.16s ease, border-color 0.16s ease, color 0.16s ease;
    }
    .category-chip:hover {
        background: var(--green-50);
        border-color: var(--green-700);
        color: var(--green-700);
    }
    .category-chip--active {
        background: var(--green-700);
        border-color: var(--green-700);
        color: #FFFFFF;
    }
    .category-chip--active:hover {
        background: var(--green-900);
        border-color: var(--green-900);
        color: #FFFFFF;
    }
    .compact-results-shell {
        margin-top: 10px;
        padding: 12px;
        border-radius: 18px;
        background: linear-gradient(180deg, #223415 0%, #1A2812 100%);
        border: 1px solid rgba(39, 80, 10, 0.12);
    }
    .compact-results-list {
        display: grid;
        gap: 12px;
    }
    .compact-result-link {
        display: block;
        text-decoration: none !important;
        color: inherit !important;
    }
    .compact-result-row {
        background: rgba(255, 255, 255, 0.96);
        border: 1px solid rgba(39, 80, 10, 0.12);
        border-radius: 14px;
        padding: 13px 15px;
        transition: transform 0.12s ease, border-color 0.12s ease, box-shadow 0.12s ease, background-color 0.12s ease;
    }
    .compact-result-link:hover .compact-result-row {
        border-color: rgba(39, 80, 10, 0.28);
        box-shadow: 0 8px 18px rgba(16, 31, 6, 0.10);
        transform: translateY(-1px);
    }
    .compact-result-row--selected {
        border-color: var(--green-700);
        background: #F7FBEF;
        box-shadow: 0 0 0 1px rgba(39, 80, 10, 0.08);
    }
    .compact-result-grid {
        display: grid;
        grid-template-columns: 22px minmax(0, 1fr) auto;
        gap: 12px;
        align-items: center;
    }
    .compact-result-main {
        min-width: 0;
    }
    .compact-result-aside {
        text-align: right;
        min-width: 88px;
    }
    .compact-result-rank {
        font-size: 12px;
        font-weight: 700;
        color: var(--text-300);
        align-self: start;
        padding-top: 2px;
    }
    .compact-result-name {
        font-size: 16px;
        font-weight: 700;
        line-height: 1.25;
        color: var(--text-900);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .compact-result-subline {
        margin-top: 2px;
        font-size: 12.5px;
        line-height: 1.35;
        color: var(--text-500);
    }
    .compact-result-price {
        text-align: right;
        font-size: 16px;
        font-weight: 700;
        color: var(--text-900);
        line-height: 1.15;
        white-space: nowrap;
    }
    .compact-result-unit {
        margin-top: 3px;
        text-align: right;
        font-size: 12.5px;
        color: var(--text-500);
        line-height: 1.3;
        white-space: nowrap;
    }
    .compact-result-badges {
        display: flex;
        flex-wrap: wrap;
        gap: 6px;
        align-items: center;
        margin-top: 7px;
    }
    .compact-badge {
        display: inline-flex;
        align-items: center;
        border-radius: 999px;
        padding: 5px 9px;
        font-size: 12px;
        line-height: 1.2;
        font-weight: 700;
        white-space: nowrap;
        border: 1px solid transparent;
    }
    .compact-badge--retailer {
        background: rgba(39, 80, 10, 0.08);
        border-color: rgba(39, 80, 10, 0.12);
        color: var(--green-700);
    }
    .compact-badge--cheapest {
        background: var(--green-50);
        border-color: var(--green-100);
        color: var(--green-700);
    }
    .compact-badge--status-safe {
        background: rgba(192, 221, 151, 0.22);
        border-color: rgba(39, 80, 10, 0.12);
        color: var(--green-700);
    }
    .compact-badge--status-single {
        background: rgba(255, 243, 204, 0.95);
        border-color: rgba(130, 109, 40, 0.18);
        color: #6E5216;
    }
    .compact-badge--status-review {
        background: rgba(255, 249, 232, 0.95);
        border-color: rgba(130, 109, 40, 0.18);
        color: #7B611A;
    }
    .compact-badge--status-measure {
        background: rgba(240, 244, 236, 0.98);
        border-color: rgba(111, 132, 87, 0.2);
        color: #54663D;
    }
    .compact-badge--selected {
        background: var(--green-700);
        border-color: var(--green-700);
        color: #FFFFFF;
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
        .category-chip-bar {
            gap: 10px;
            margin: 12px 0;
        }
        .category-chip {
            padding: 10px 14px;
            min-height: 40px;
            font-size: 13px;
        }
        .compact-results-shell {
            padding: 12px;
            border-radius: 16px;
        }
        .compact-result-row {
            padding: 13px 15px;
        }
        .compact-result-grid {
            gap: 12px;
        }
        .compact-result-name {
            font-size: 14px;
            line-height: 1.3;
        }
        .compact-result-price {
            font-size: 14px;
        }
        .compact-result-unit,
        .compact-result-subline {
            font-size: 11px;
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
        .result-card {
            margin-top: 12px;
            padding: 16px 18px;
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
hydrate_search_input_from_query_params()
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

recommendation = None
search_mode = detect_search_mode(search_text)
if search_mode == SEARCH_MODE_CATEGORY:
    recommendation = render_category_results(
        search_text,
        catalog_df,
        freshness_row,
    )
elif search_mode == SEARCH_MODE_BRAND:
    recommendation = render_brand_results(
        search_text,
        catalog_df,
        freshness_row,
    )
else:
    recommendation = render_specific_results(
        search_text,
        catalog_df,
        freshness_row,
    )

if recommendation and recommendation.get("family_label"):
    render_inline_message(
        "Genel ürün aramalarında aynı aile içindeki seçenekler tek bir niyet olarak ele alınır. "
        "Bu yüzden benzer SKU'lar ayrı ayrı sonuç olarak yığılmaz.",
        tone="muted",
    )

if recommendation:
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
