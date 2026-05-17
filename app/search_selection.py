from __future__ import annotations

import re

import pandas as pd

from pipeline.transforms import normalize_text
from pipeline.optimizer.cleaning_products import (
    BLEACH_BRANDS,
    CLEANING_BRANDS,
    DISHWASHER_BRANDS,
    DISHWASHING_BRANDS,
    LAUNDRY_BRANDS,
    PACKAGE_FORMAT_SPRAY,
    cleaning_brand_display_name,
    cleaning_brand_from_query_tokens,
    cleaning_variant_from_query_tokens,
    cleaning_tokens,
    explicit_cleaning_query_subtype,
    extract_cleaning_bundle_signature,
    infer_cleaning_product_profile,
    nearest_cleaning_brand_token,
    preferred_cleaning_subtypes,
)
from pipeline.optimizer.product_search import product_selection_id, search_product_catalog


CATEGORY_RESULT_LIMIT = 12
BRAND_RESULT_LIMIT = 8
SEARCH_MODE_BRAND = "brand"
SEARCH_MODE_CATEGORY = "category"
SEARCH_MODE_SPECIFIC = "specific"
# Backward-compatible aliases for older imports while the app code settles.
CLEANING_SEARCH_MODE_BRAND = SEARCH_MODE_BRAND
CLEANING_SEARCH_MODE_CATEGORY = SEARCH_MODE_CATEGORY
CLEANING_SEARCH_MODE_SPECIFIC = SEARCH_MODE_SPECIFIC
RESULT_STATUS_SAFE = "safe"
RESULT_STATUS_REVIEW_REQUIRED = "review_required"
RESULT_STATUS_SINGLE_MARKET = "single_market"
_CLEANING_SIZE_HINT_PATTERN = re.compile(
    r"\b\d+(?:[.,]\d+)?\s*(?:ml|l|lt|kg|g|gr|tablet|tableti|kapsul|kapsulu|adet|rulo|roll)\b"
)
_CATEGORY_UNIT_LABELS = {
    "liter": "litre",
    "kg": "kg",
    "roll": "rulo",
    "piece": "adet",
}
_CATEGORY_SUBTYPE_CHIP_LABELS = {
    "dishwasher_tablet": "Tablet",
}
_CATEGORY_SUBTYPE_FILTER_IDS = {
    "dishwasher_tablet": "tablet",
}
_RETAILER_BRAND_TOKENS = {"a101", "migros"}
_CATEGORY_BRAND_PRIORITY = {
    "dishwashing_liquid": ["fairy", "pril", "bingo"],
    "dishwasher_tablet": ["finish", "fairy", "pril"],
    "laundry_liquid": ["omo", "ariel"],
    "laundry_powder": ["omo", "ariel"],
}
_CATEGORY_BRAND_FILTER_LIMIT = 4
_DISPLAY_BRAND_LABELS = {
    "fairy": "Fairy",
    "pril": "Pril",
    "bingo": "Bingo",
    "omo": "Omo",
    "ariel": "Ariel",
    "finish": "Finish",
    "domestos": "Domestos",
}
_PREFERRED_PRIMARY_UNITS = {
    "dishwashing_liquid": "litre",
    "dishwasher_tablet": "adet",
    "dishwasher_salt": "kg",
    "rinse_aid": "litre",
    "dishwasher_cleaner": "litre",
    "laundry_liquid": "litre",
    "laundry_powder": "kg",
    "laundry_capsule": "adet",
    "bleach": "litre",
    "surface_cleaner": "litre",
}
_CLEANING_QUERY_STOPWORDS = {
    "adet",
    "banyo",
    "bulasik",
    "camasir",
    "deterjan",
    "deterjani",
    "elde",
    "gel",
    "kapsul",
    "kapsulu",
    "kokulu",
    "li",
    "lu",
    "lt",
    "makine",
    "makinesi",
    "ml",
    "parlatici",
    "rulo",
    "sivi",
    "sprey",
    "spray",
    "tablet",
    "tableti",
    "temiz",
    "temizleyici",
    "toz",
    "yikama",
    "yuzey",
}
_FINISH_POWER_RELATED_TOKENS = {"power", "powerball", "quantum", "ultimate", "gel", "max"}
_PACK_MEASUREMENT_PATTERN = re.compile(
    r"\b(?P<count>\d+)\s*x\s*(?P<size>\d+(?:[.,]\d+)?)\s*(?P<unit>ml|l|kg|g)\b",
    flags=re.IGNORECASE,
)
_SINGLE_MARKET_COVERAGE_STATUSES = {
    "only_a101",
    "only_migros",
    "only_available_at_a101",
    "only_available_at_migros",
}


def _leading_cleaning_brand_token(profile: dict[str, object]) -> str | None:
    tokens = tuple(profile.get("tokens") or ())
    if not tokens:
        return None
    leading_token = str(tokens[0])
    return leading_token if leading_token in CLEANING_BRANDS else None


def resolved_cleaning_brand_token(search_text: str) -> str | None:
    query_tokens = cleaning_tokens(search_text)
    return cleaning_brand_from_query_tokens(query_tokens) or nearest_cleaning_brand_token(search_text)


def is_brand_only_cleaning_query(search_text: str) -> bool:
    tokens = cleaning_tokens(search_text)
    return len(tokens) == 1 and cleaning_brand_from_query_tokens(tokens) is not None


def detect_search_mode(search_text: str) -> str:
    query_tokens = cleaning_tokens(search_text)
    if is_brand_only_cleaning_query(search_text):
        return SEARCH_MODE_BRAND
    if _is_cleaning_category_query(search_text, query_tokens):
        return SEARCH_MODE_CATEGORY
    return SEARCH_MODE_SPECIFIC


def normalize_result_status(coverage_status: str | None) -> str:
    if coverage_status == "comparable":
        return RESULT_STATUS_SAFE
    if coverage_status in _SINGLE_MARKET_COVERAGE_STATUSES:
        return RESULT_STATUS_SINGLE_MARKET
    return RESULT_STATUS_REVIEW_REQUIRED


def _is_cleaning_category_query(
    search_text: str,
    query_tokens: set[str] | None = None,
) -> bool:
    query_tokens = query_tokens or cleaning_tokens(search_text)
    if not query_tokens or cleaning_brand_from_query_tokens(query_tokens):
        return False
    if _query_has_explicit_cleaning_detail(search_text, query_tokens):
        return False
    return explicit_cleaning_query_subtype(query_tokens) is not None


def _query_has_explicit_cleaning_detail(
    search_text: str,
    query_tokens: set[str],
) -> bool:
    if extract_cleaning_bundle_signature(search_text):
        return True
    if _CLEANING_SIZE_HINT_PATTERN.search(search_text):
        return True
    if any(token.isdigit() for token in query_tokens):
        return True
    return cleaning_variant_from_query_tokens(query_tokens) is not None


def _numeric_value(value) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _text_value(value) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text or None


def _effective_pack_measurement(
    source_name: str | None,
    normalized_unit: str | None,
    normalized_quantity: float | None,
) -> tuple[str | None, float | None]:
    if not source_name:
        return normalized_unit, normalized_quantity

    match = _PACK_MEASUREMENT_PATTERN.search(str(source_name))
    if not match:
        return normalized_unit, normalized_quantity

    pack_count = int(match.group("count"))
    pack_size = float(match.group("size").replace(",", "."))
    pack_unit = match.group("unit").lower()

    total_unit = None
    total_quantity = None
    if pack_unit == "ml":
        total_unit = "liter"
        total_quantity = pack_count * pack_size / 1000.0
    elif pack_unit == "l":
        total_unit = "liter"
        total_quantity = pack_count * pack_size
    elif pack_unit == "g":
        total_unit = "kg"
        total_quantity = pack_count * pack_size / 1000.0
    elif pack_unit == "kg":
        total_unit = "kg"
        total_quantity = pack_count * pack_size

    if (
        total_unit
        and total_quantity is not None
        and normalized_unit == total_unit
        and (normalized_quantity is None or total_quantity > normalized_quantity)
    ):
        return total_unit, total_quantity

    return normalized_unit, normalized_quantity


def _row_display_name(row: pd.Series) -> str:
    for field_name in ("a101_source_product_name", "migros_source_product_name"):
        value = _text_value(row.get(field_name))
        if value:
            return value
    return _text_value(row.get("standardized_product_name")) or "Ürün"


def _unit_label(unit: str | None) -> str | None:
    return _CATEGORY_UNIT_LABELS.get(str(unit), str(unit)) if unit else None


def _extract_compact_size_label(display_name: str, result: dict) -> str | None:
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

    if result.get("best_unit_label") != "litre":
        return None

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

    if quantity_value is None:
        return None
    if quantity_value < 1:
        return f"{int(round(quantity_value * 1000))} ml"
    if float(quantity_value).is_integer():
        return f"{int(quantity_value)} L"
    return f"{quantity_value:g} L"


def format_compact_category_display_name(result: dict) -> str:
    display_name = str(result.get("display_name") or "")
    profile = infer_cleaning_product_profile(display_name)
    brand_token = result.get("brand_token")
    subtype = result.get("subtype") or profile.get("subtype")
    variant_token = result.get("variant_token") or profile.get("variant_token")
    size_label = _extract_compact_size_label(display_name, result)

    if not brand_token:
        return display_name

    brand_label = _DISPLAY_BRAND_LABELS.get(brand_token, brand_token.title())
    parts = [brand_label]
    if subtype == "dishwashing_spray":
        parts.append("Sprey")
    elif subtype == "dishwasher_tablet":
        parts.append("Tablet")
    elif subtype == "rinse_aid":
        parts.append("Parlatıcı")

    if variant_token and variant_token not in {"platinum"}:
        parts.append(variant_token.title())
    elif variant_token == "platinum" and subtype == "dishwasher_tablet":
        parts.append("Platinum")

    if size_label:
        parts.append(size_label)

    return " ".join(parts).strip() or display_name


def resolve_category_result_selection(
    category_results: list[dict],
    current_selection_id: str | None,
    clicked_selection_id: str | None,
) -> str | None:
    valid_selection_ids = {
        result["group"]["selection_id"]
        for result in category_results
    }
    if clicked_selection_id in valid_selection_ids:
        return clicked_selection_id
    if current_selection_id in valid_selection_ids:
        return current_selection_id
    return None


def _retailer_offer(row: pd.Series, retailer: str) -> dict[str, object] | None:
    raw_price = _numeric_value(row.get(f"{retailer}_raw_price"))
    if raw_price is None:
        return None

    normalized_quantity = _numeric_value(row.get(f"{retailer}_normalized_quantity"))
    normalized_unit = _text_value(row.get(f"{retailer}_normalized_unit"))
    source_name = _text_value(row.get(f"{retailer}_source_product_name")) or _text_value(
        row.get("standardized_product_name")
    )
    normalized_unit, normalized_quantity = _effective_pack_measurement(
        source_name,
        normalized_unit,
        normalized_quantity,
    )
    comparison_price = _numeric_value(row.get(f"{retailer}_comparison_price"))
    comparison_unit = _text_value(row.get("comparison_price_unit"))

    unit_price = None
    unit_label = None
    if normalized_quantity and normalized_quantity > 0 and normalized_unit in _CATEGORY_UNIT_LABELS:
        unit_price = raw_price / normalized_quantity
        unit_label = _unit_label(normalized_unit)
    elif comparison_price is not None and comparison_unit:
        unit_price = comparison_price
        unit_label = _unit_label(comparison_unit)

    return {
        "retailer": retailer,
        "raw_price": raw_price,
        "unit_price": unit_price,
        "unit_label": unit_label,
    }


def _coverage_priority(coverage_status: str | None) -> int:
    normalized_status = normalize_result_status(coverage_status)
    if normalized_status == RESULT_STATUS_SAFE:
        return 0
    if normalized_status == RESULT_STATUS_SINGLE_MARKET:
        return 1
    return 2


def _category_decision_unit_price(result: dict[str, object]) -> float:
    unit_price = _numeric_value(result.get("best_unit_price"))
    if unit_price is None:
        return float("inf")

    normalized_status = normalize_result_status(_text_value(result.get("coverage_status")))
    if normalized_status == RESULT_STATUS_SAFE:
        return unit_price * 0.99
    if normalized_status == RESULT_STATUS_REVIEW_REQUIRED:
        return unit_price * 1.01
    return unit_price


def _catalog_result_from_row(
    row: pd.Series,
    selection_group: dict | None = None,
    display_name_override: str | None = None,
    coverage_status_override: str | None = None,
) -> dict[str, object] | None:
    product_name = _text_value(row.get("standardized_product_name"))
    if not product_name:
        return None

    offers = [
        offer
        for offer in (
            _retailer_offer(row, "a101"),
            _retailer_offer(row, "migros"),
        )
        if offer is not None
    ]
    if not offers:
        return None

    offers.sort(
        key=lambda offer: (
            offer["unit_price"] is None,
            offer["unit_price"] if offer["unit_price"] is not None else float("inf"),
            offer["raw_price"],
        )
    )
    best_offer = offers[0]

    profile = None
    for field_name in ("a101_source_product_name", "migros_source_product_name"):
        value = _text_value(row.get(field_name))
        if not value:
            continue
        candidate_profile = infer_cleaning_product_profile(value)
        if candidate_profile.get("is_cleaning"):
            profile = candidate_profile
            break
    profile = profile or {}

    coverage_status = coverage_status_override or _text_value(row.get("coverage_status"))
    result_group = selection_group or {
        "selection_id": product_selection_id(product_name),
        "selection_type": "product",
        "family_id": None,
        "family_label": None,
        "product_names": [product_name],
    }

    return {
        "group": result_group,
        "display_name": display_name_override or _row_display_name(row),
        "coverage_status": coverage_status,
        "normalized_status": normalize_result_status(coverage_status),
        "comparison_status_label": _text_value(row.get("comparison_status_label")),
        "best_retailer": best_offer["retailer"],
        "best_price": best_offer["raw_price"],
        "best_unit_price": best_offer["unit_price"],
        "best_unit_label": best_offer["unit_label"],
        "best_unit_key": next(
            (
                retailer_offer["unit_label"]
                for retailer_offer in offers
                if retailer_offer["retailer"] == best_offer["retailer"]
            ),
            None,
        ),
        "brand_token": profile.get("brand_token"),
        "subtype": profile.get("subtype"),
        "package_format": profile.get("package_format"),
        "variant_token": profile.get("variant_token"),
    }


def build_category_product_results(
    catalog_df: pd.DataFrame,
    search_text: str,
    limit: int = CATEGORY_RESULT_LIMIT,
) -> list[dict]:
    search_results = search_product_catalog(catalog_df, search_text)
    if search_results.empty:
        return []

    results: list[dict] = []
    for _, row in search_results.iterrows():
        result = _catalog_result_from_row(row)
        if result is not None:
            results.append(result)

    results.sort(
        key=lambda result: (
            result["best_unit_price"] is None,
            _category_decision_unit_price(result),
            result["best_unit_price"] if result["best_unit_price"] is not None else float("inf"),
            _coverage_priority(result.get("coverage_status")),
            result["best_price"] if result["best_price"] is not None else float("inf"),
            result["display_name"].lower(),
        )
    )
    for index, result in enumerate(results):
        result["is_cheapest"] = index == 0
    return results[:limit]


def _common_unit_label(results: list[dict]) -> str | None:
    unit_counts: dict[str, int] = {}
    for result in results:
        unit_label = result.get("best_unit_label")
        if not unit_label:
            continue
        unit_counts[unit_label] = unit_counts.get(unit_label, 0) + 1
    if not unit_counts:
        return None
    return max(unit_counts.items(), key=lambda item: (item[1], item[0]))[0]


def _preferred_primary_unit_label(
    results: list[dict],
    primary_subtype: str | None,
) -> str | None:
    preferred_unit = _PREFERRED_PRIMARY_UNITS.get(primary_subtype or "")
    if preferred_unit and any(result.get("best_unit_label") == preferred_unit for result in results):
        return preferred_unit
    return _common_unit_label(results)


def _category_primary_subtype(
    category_results: list[dict],
    search_text: str,
) -> str | None:
    explicit_subtype = explicit_cleaning_query_subtype(cleaning_tokens(search_text))
    if explicit_subtype:
        return explicit_subtype

    subtype_counts: dict[str, int] = {}
    for result in category_results:
        subtype = result.get("subtype")
        if not subtype:
            continue
        subtype_counts[subtype] = subtype_counts.get(subtype, 0) + 1
    if not subtype_counts:
        return None
    return max(subtype_counts.items(), key=lambda item: (item[1], item[0]))[0]


def build_category_result_sections(
    category_results: list[dict],
    search_text: str,
) -> dict[str, object]:
    if not category_results:
        return {
            "primary_results": [],
            "secondary_results": [],
            "filters": [],
            "default_filter": None,
        }

    primary_subtype = _category_primary_subtype(category_results, search_text)
    subtype_candidates = [
        result
        for result in category_results
        if not primary_subtype or result.get("subtype") == primary_subtype
    ]
    primary_unit_label = _preferred_primary_unit_label(subtype_candidates, primary_subtype)

    primary_results = [
        result
        for result in subtype_candidates
        if not primary_unit_label or result.get("best_unit_label") == primary_unit_label
    ]
    secondary_results = [
        result
        for result in category_results
        if result not in primary_results
    ]

    filters = [
        {
            "id": "all",
            "label": "Tümü",
            "results": primary_results,
        }
    ]

    brand_results_by_token: dict[str, list[dict]] = {}
    brand_token_order: list[str] = []
    for result in primary_results:
        brand_token = result.get("brand_token")
        if not brand_token or brand_token in _RETAILER_BRAND_TOKENS:
            continue
        if brand_token not in brand_results_by_token:
            brand_token_order.append(brand_token)
            brand_results_by_token[brand_token] = []
        brand_results_by_token[brand_token].append(result)

    preferred_brand_tokens = [
        token
        for token in _CATEGORY_BRAND_PRIORITY.get(primary_subtype or "", [])
        if token in brand_results_by_token
    ]
    remaining_brand_tokens = [
        token for token in brand_token_order if token not in preferred_brand_tokens
    ]
    ordered_brand_tokens = (
        preferred_brand_tokens[:_CATEGORY_BRAND_FILTER_LIMIT]
        if preferred_brand_tokens
        else remaining_brand_tokens[:_CATEGORY_BRAND_FILTER_LIMIT]
    )

    for brand_token in ordered_brand_tokens:
        brand_results = brand_results_by_token.get(brand_token) or []
        filters.append(
            {
                "id": brand_token,
                "label": cleaning_brand_display_name(brand_token),
                "results": brand_results,
            }
        )

    filters = [filter_option for filter_option in filters if filter_option["results"]]
    default_filter = filters[0]["id"] if filters else None
    return {
        "primary_results": primary_results,
        "secondary_results": secondary_results,
        "filters": filters,
        "default_filter": default_filter,
        "primary_unit_label": primary_unit_label,
    }


def filter_category_results(
    category_sections: dict[str, object],
    filter_id: str | None,
) -> list[dict]:
    for filter_option in category_sections.get("filters", []):
        if filter_option["id"] == filter_id:
            results = list(filter_option["results"])
            for index, result in enumerate(results):
                result["is_visible_cheapest"] = index == 0
            return results

    results = list(category_sections.get("primary_results", []))
    for index, result in enumerate(results):
        result["is_visible_cheapest"] = index == 0
    return results


def build_unified_category_results(
    category_sections: dict[str, object],
    filter_id: str | None,
    limit: int | None = CATEGORY_RESULT_LIMIT,
) -> list[dict]:
    primary_results = filter_category_results(category_sections, filter_id)
    secondary_results = list(category_sections.get("secondary_results", []))
    if filter_id and filter_id != "all":
        secondary_results = [
            row for row in secondary_results if row_matches_brand_filter(row, filter_id)
        ]

    primary_unit_label = _text_value(category_sections.get("primary_unit_label"))
    combined_results: list[dict] = []
    seen_selection_ids: set[str] = set()

    for display_section, source_rows in (
        ("primary", primary_results),
        ("secondary", secondary_results),
    ):
        for row in source_rows:
            selection_id = row["group"]["selection_id"]
            if selection_id in seen_selection_ids:
                continue
            cloned_row = dict(row)
            cloned_row["category_display_section"] = display_section
            cloned_row["category_primary_unit_label"] = primary_unit_label
            combined_results.append(cloned_row)
            seen_selection_ids.add(selection_id)

    if limit is not None:
        combined_results = combined_results[:limit]

    for index, result in enumerate(combined_results):
        result["is_visible_cheapest"] = index == 0

    return combined_results


def dedupe_compact_result_rows(results: list[dict]) -> list[dict]:
    deduped_results: list[dict] = []
    seen_keys: set[tuple[str, str | None, str | None]] = set()

    for result in results:
        compact_name = format_compact_category_display_name(result).strip().lower()
        dedupe_key = (
            compact_name,
            _text_value(result.get("normalized_status")) or normalize_result_status(result.get("coverage_status")),
            _text_value(result.get("best_unit_label")),
        )
        if dedupe_key in seen_keys:
            continue
        deduped_results.append(result)
        seen_keys.add(dedupe_key)

    for index, result in enumerate(deduped_results):
        result["is_visible_cheapest"] = index == 0
    return deduped_results


def row_matches_brand_filter(row: dict, brand_id: str | None) -> bool:
    if not brand_id or brand_id == "all":
        return True
    if brand_id == "tablet":
        return row.get("subtype") == "dishwasher_tablet"
    return row.get("brand_token") == brand_id


def split_category_rows_by_status(rows: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    comparable_rows: list[dict] = []
    single_market_rows: list[dict] = []
    incompatible_rows: list[dict] = []

    for row in rows:
        coverage_status = row.get("coverage_status")
        if coverage_status == "comparable":
            comparable_rows.append(row)
        elif coverage_status in {
            "only_a101",
            "only_migros",
            "only_available_at_a101",
            "only_available_at_migros",
        }:
            single_market_rows.append(row)
        else:
            incompatible_rows.append(row)

    return comparable_rows, single_market_rows, incompatible_rows


def build_group_result_rows(
    catalog_df: pd.DataFrame,
    groups: list[dict],
) -> list[dict]:
    results: list[dict] = []
    for group in groups:
        best_result = None
        for product_name in group.get("product_names") or []:
            row = catalog_df.loc[catalog_df["standardized_product_name"] == product_name]
            if row.empty:
                continue
            result = _catalog_result_from_row(
                row.iloc[0],
                selection_group=group,
                display_name_override=group.get("family_label"),
                coverage_status_override=(
                    "comparison_review_required" if group.get("force_review") else None
                ),
            )
            if result is None:
                continue
            if best_result is None:
                best_result = result
                continue

            candidate_key = (
                result["best_unit_price"] is None,
                result["best_unit_price"] if result["best_unit_price"] is not None else float("inf"),
                _coverage_priority(result.get("coverage_status")),
                result["best_price"] if result["best_price"] is not None else float("inf"),
                str(result["display_name"]).lower(),
            )
            current_key = (
                best_result["best_unit_price"] is None,
                best_result["best_unit_price"] if best_result["best_unit_price"] is not None else float("inf"),
                _coverage_priority(best_result.get("coverage_status")),
                best_result["best_price"] if best_result["best_price"] is not None else float("inf"),
                str(best_result["display_name"]).lower(),
            )
            if candidate_key < current_key:
                best_result = result

        if best_result is not None:
            results.append(best_result)

    for index, result in enumerate(results):
        result["is_visible_cheapest"] = index == 0
    return results


def build_category_row_price_model(result: dict) -> dict[str, object]:
    best_unit_price = result.get("best_unit_price")
    best_unit_label = _text_value(result.get("best_unit_label"))
    best_price = result.get("best_price")

    if best_unit_price is not None and best_unit_label:
        return {
            "primary_kind": "unit_price",
            "primary_value": best_unit_price,
            "primary_unit_label": best_unit_label,
            "secondary_kind": "shelf_price",
            "secondary_value": best_price,
        }

    return {
        "primary_kind": "shelf_price",
        "primary_value": best_price,
        "primary_unit_label": None,
        "secondary_kind": None,
        "secondary_value": None,
    }


def sort_brand_result_rows(
    search_text: str,
    results: list[dict[str, object]],
) -> list[dict[str, object]]:
    brand_token = resolved_cleaning_brand_token(search_text)
    preferred_subtypes = preferred_cleaning_subtypes(
        {brand_token} if brand_token else cleaning_tokens(search_text)
    )
    subtype_rank = {subtype: index for index, subtype in enumerate(preferred_subtypes)}

    ranked_results = sorted(
        results,
        key=lambda result: (
            0 if _text_value(result.get("brand_token")) == brand_token else 1,
            subtype_rank.get(_text_value(result.get("subtype")), len(subtype_rank) + 1),
            0 if normalize_result_status(_text_value(result.get("coverage_status"))) == RESULT_STATUS_SAFE else 1,
            _category_decision_unit_price(result),
            _numeric_value(result.get("best_unit_price")) if _numeric_value(result.get("best_unit_price")) is not None else float("inf"),
            _numeric_value(result.get("best_price")) if _numeric_value(result.get("best_price")) is not None else float("inf"),
            (_text_value(result.get("display_name")) or "").lower(),
        ),
    )
    for index, result in enumerate(ranked_results):
        result["is_visible_cheapest"] = index == 0
    return ranked_results


def sort_specific_result_rows(
    search_text: str,
    results: list[dict[str, object]],
) -> list[dict[str, object]]:
    brand_token = resolved_cleaning_brand_token(search_text)
    query_tokens = cleaning_tokens(search_text)
    explicit_subtype = explicit_cleaning_query_subtype(query_tokens)
    modifier_tokens = {
        token
        for token in query_tokens
        if token != brand_token and token not in _CLEANING_QUERY_STOPWORDS and not token.isdigit()
    }

    def semantic_score(result: dict[str, object]) -> int:
        score = 0
        if brand_token and _text_value(result.get("brand_token")) == brand_token:
            score += 30

        if explicit_subtype and _text_value(result.get("subtype")) == explicit_subtype:
            score += 20

        row_text = normalize_text(
            " ".join(
                filter(
                    None,
                    [
                        _text_value(result.get("display_name")) or "",
                        " ".join((result.get("group") or {}).get("product_names") or []),
                    ],
                )
            )
        )
        row_tokens = set(row_text.split())

        for token in modifier_tokens:
            if token in row_tokens:
                score += 18

        if brand_token == "finish" and "power" in modifier_tokens:
            if row_tokens & _FINISH_POWER_RELATED_TOKENS:
                score += 34
            else:
                score -= 18

        normalized_status = normalize_result_status(_text_value(result.get("coverage_status")))
        if normalized_status == RESULT_STATUS_SAFE:
            score += 6
        elif normalized_status == RESULT_STATUS_SINGLE_MARKET:
            score += 3
        return score

    ranked_results = sorted(
        results,
        key=lambda result: (
            -semantic_score(result),
            _category_decision_unit_price(result),
            _numeric_value(result.get("best_unit_price")) if _numeric_value(result.get("best_unit_price")) is not None else float("inf"),
            _numeric_value(result.get("best_price")) if _numeric_value(result.get("best_price")) is not None else float("inf"),
            (_text_value(result.get("display_name")) or "").lower(),
        ),
    )
    for index, result in enumerate(ranked_results):
        result["is_visible_cheapest"] = index == 0
    return ranked_results


def limit_brand_result_rows(
    result_rows: list[dict[str, object]],
    selected_selection_id: str | None = None,
    limit: int = BRAND_RESULT_LIMIT,
    expanded: bool = False,
) -> list[dict[str, object]]:
    if expanded or len(result_rows) <= limit:
        return list(result_rows)

    visible_rows = list(result_rows[:limit])
    if not selected_selection_id:
        return visible_rows

    if any(
        (row.get("group") or {}).get("selection_id") == selected_selection_id
        for row in visible_rows
    ):
        return visible_rows

    selected_row = next(
        (
            row
            for row in result_rows
            if (row.get("group") or {}).get("selection_id") == selected_selection_id
        ),
        None,
    )
    if selected_row is None:
        return visible_rows

    retained_rows = [
        row
        for row in result_rows
        if (row.get("group") or {}).get("selection_id") != selected_selection_id
    ][: max(limit - 1, 0)]
    return [selected_row, *retained_rows]


def has_hidden_brand_result_rows(
    result_rows: list[dict[str, object]],
    visible_rows: list[dict[str, object]],
) -> bool:
    return len(result_rows) > len(visible_rows)


def limit_cleaning_family_product_groups(
    product_groups: list[dict],
    selected_product_id: str | None = None,
    limit: int = 3,
    expanded: bool = False,
) -> list[dict]:
    if expanded or len(product_groups) <= limit:
        return list(product_groups)

    visible_groups = list(product_groups[:limit])
    if not selected_product_id:
        return visible_groups

    if any(group.get("selection_id") == selected_product_id for group in visible_groups):
        return visible_groups

    selected_group = next(
        (group for group in product_groups if group.get("selection_id") == selected_product_id),
        None,
    )
    if selected_group is None:
        return visible_groups

    retained_groups = [
        group for group in product_groups if group.get("selection_id") != selected_product_id
    ][: max(limit - 1, 0)]
    return [selected_group, *retained_groups]


def has_hidden_cleaning_family_product_groups(
    product_groups: list[dict],
    visible_groups: list[dict],
) -> bool:
    return len(product_groups) > len(visible_groups)


def resolve_category_filter_selection(
    filter_ids: list[str],
    current_filter_id: str | None,
    requested_filter_id: str | None,
    default_filter_id: str | None,
) -> str | None:
    if requested_filter_id in filter_ids:
        return requested_filter_id
    if current_filter_id in filter_ids:
        return current_filter_id
    if default_filter_id in filter_ids:
        return default_filter_id
    return filter_ids[0] if filter_ids else None


def build_cleaning_selection_button_key(
    level: str,
    selection_id: str,
    family_id: str | None = None,
) -> str:
    key_parts = [level]
    if family_id:
        key_parts.append(family_id)
    key_parts.append(selection_id)
    return re.sub(r"[^0-9a-zA-Z_]+", "_", "_".join(key_parts)).strip("_")


def preserve_or_reset_cleaning_product_selection(
    selected_product_id: str | None,
    product_groups: list[dict] | None,
) -> str | None:
    if product_groups is None:
        return selected_product_id

    valid_product_ids = {group["selection_id"] for group in product_groups}
    return selected_product_id if selected_product_id in valid_product_ids else None


def combine_selection_groups(
    safe_groups: list[dict],
    related_groups: list[dict],
) -> list[dict]:
    combined_groups: list[dict] = []
    seen_selection_ids: set[str] = set()

    for group in [*safe_groups, *related_groups]:
        selection_id = group.get("selection_id")
        if not selection_id or selection_id in seen_selection_ids:
            continue
        combined_groups.append(group)
        seen_selection_ids.add(selection_id)

    return combined_groups


def _cleaning_group_profile(
    catalog_df: pd.DataFrame,
    group: dict,
) -> dict[str, object] | None:
    product_names = group.get("product_names") or []
    for product_name in product_names:
        row = catalog_df.loc[catalog_df["standardized_product_name"] == product_name]
        if row.empty:
            continue

        for field_name in (
            "a101_source_product_name",
            "migros_source_product_name",
            "standardized_product_name",
        ):
            value = row.iloc[0].get(field_name)
            if not value:
                continue
            profile = infer_cleaning_product_profile(str(value))
            if profile.get("is_cleaning"):
                if _leading_cleaning_brand_token(profile) != profile.get("brand_token"):
                    continue
                tokens = set(profile.get("tokens") or ())
                if {"havlu", "havlusu"} & tokens and {"temizleme", "yuzey"} & tokens:
                    return {
                        "brand_token": profile.get("brand_token"),
                        "family_key": "cleaning_wipes",
                    }
                if profile.get("subtype") == "dishwashing_liquid" and profile.get("package_format") == PACKAGE_FORMAT_SPRAY:
                    return {
                        "brand_token": profile.get("brand_token"),
                        "family_key": "dishwashing_spray",
                    }
                if profile.get("subtype"):
                    return {
                        "brand_token": profile.get("brand_token"),
                        "family_key": profile.get("subtype"),
                    }

    return None


def _parse_cleaning_family_id(family_id: str | None) -> tuple[str | None, str | None]:
    if not family_id:
        return None, None
    parts = str(family_id).split(":")
    if len(parts) == 3 and parts[0] == "cleaning_family":
        return parts[1], parts[2]
    return None, None


def _cleaning_family_label(brand_token: str, family_key: str) -> str:
    brand_label = cleaning_brand_display_name(brand_token)
    family_labels = {
        "dishwashing_liquid": "Sıvı Bulaşık Deterjanı",
        "dishwashing_spray": "Sprey",
        "dishwasher_tablet": "Bulaşık Tableti",
        "dishwasher_salt": "Bulaşık Makinesi Tuzu",
        "rinse_aid": "Parlatıcı",
        "dishwasher_cleaner": "Makine Temizleyici",
        "laundry_liquid": "Sıvı Deterjan",
        "laundry_powder": "Toz Deterjan",
        "laundry_capsule": "Kapsül Deterjan",
        "bleach": "Çamaşır Suyu",
        "surface_cleaner": "Yüzey Temizleyici",
        "cleaning_wipes": "Temizlik Havlusu",
    }
    return f"{brand_label} {family_labels.get(family_key, family_key)}".strip()


def _brand_family_order(brand_token: str) -> list[str]:
    if brand_token in DISHWASHING_BRANDS:
        return [
            "dishwashing_liquid",
            "dishwashing_spray",
            "dishwasher_tablet",
            "dishwasher_salt",
            "rinse_aid",
            "dishwasher_cleaner",
            "cleaning_wipes",
            "surface_cleaner",
        ]
    if brand_token in DISHWASHER_BRANDS:
        return [
            "dishwasher_tablet",
            "dishwasher_salt",
            "rinse_aid",
            "dishwasher_cleaner",
            "dishwashing_liquid",
            "dishwashing_spray",
        ]
    if brand_token in LAUNDRY_BRANDS:
        return [
            "laundry_liquid",
            "laundry_powder",
            "laundry_capsule",
        ]
    if brand_token in BLEACH_BRANDS:
        return [
            "bleach",
            "surface_cleaner",
            "cleaning_wipes",
        ]
    return []


def build_brand_only_cleaning_groups(
    catalog_df: pd.DataFrame,
    safe_groups: list[dict],
    related_groups: list[dict],
    search_text: str,
) -> list[dict]:
    brand_token = cleaning_brand_from_query_tokens(cleaning_tokens(search_text))
    if not brand_token:
        return combine_selection_groups(safe_groups, related_groups)

    grouped: dict[str, dict] = {}
    for group in combine_selection_groups(safe_groups, related_groups):
        group_profile = _cleaning_group_profile(catalog_df, group)
        if not group_profile or group_profile.get("brand_token") != brand_token:
            continue

        family_key = str(group_profile["family_key"])
        family_id = f"cleaning_family:{brand_token}:{family_key}"
        family_group = grouped.setdefault(
            family_id,
            {
                "selection_id": family_id,
                "selection_type": "product_family",
                "family_id": family_id,
                "family_label": _cleaning_family_label(brand_token, family_key),
                "product_names": [],
            },
        )
        for product_name in group.get("product_names") or []:
            if product_name not in family_group["product_names"]:
                family_group["product_names"].append(product_name)

    order = _brand_family_order(brand_token)
    order_index = {family_key: index for index, family_key in enumerate(order)}
    sorted_groups = sorted(
        grouped.values(),
        key=lambda group: (
            order_index.get(str(group["family_id"]).split(":")[-1], 999),
            group["family_label"],
        ),
    )
    return sorted_groups


def build_cleaning_family_product_groups(
    search_results: pd.DataFrame,
    family_group: dict,
) -> list[dict]:
    if search_results.empty:
        return []

    brand_token, family_key = _parse_cleaning_family_id(family_group.get("family_id"))
    candidate_product_names = set(family_group.get("product_names") or [])
    if not brand_token or not family_key or not candidate_product_names:
        return []

    groups: list[dict] = []
    seen_product_names: set[str] = set()

    for _, row in search_results.iterrows():
        product_name = str(row.get("standardized_product_name") or "")
        if not product_name or product_name not in candidate_product_names:
            continue

        profile = None
        for field_name in (
            "a101_source_product_name",
            "migros_source_product_name",
            "standardized_product_name",
        ):
            value = row.get(field_name)
            if not value:
                continue
            candidate_profile = infer_cleaning_product_profile(str(value))
            if candidate_profile.get("is_cleaning"):
                if _leading_cleaning_brand_token(candidate_profile) != candidate_profile.get("brand_token"):
                    continue
                profile = candidate_profile
                break

        if not profile or profile.get("brand_token") != brand_token:
            continue

        row_family_key = profile.get("subtype")
        tokens = set(profile.get("tokens") or ())
        if {"havlu", "havlusu"} & tokens and {"temizleme", "yuzey"} & tokens:
            row_family_key = "cleaning_wipes"
        elif (
            profile.get("subtype") == "dishwashing_liquid"
            and profile.get("package_format") == PACKAGE_FORMAT_SPRAY
        ):
            row_family_key = "dishwashing_spray"

        if row_family_key != family_key or product_name in seen_product_names:
            continue

        groups.append(
            {
                "selection_id": product_selection_id(product_name),
                "selection_type": "product",
                "family_id": None,
                "family_label": None,
                "product_names": [product_name],
            }
        )
        seen_product_names.add(product_name)

    return groups


def build_cleaning_sibling_product_groups(
    product_groups: list[dict],
    selected_product_id: str | None,
) -> list[dict]:
    if not selected_product_id:
        return list(product_groups)

    return [
        group
        for group in product_groups
        if group.get("selection_id") != selected_product_id
    ]


def build_brand_filter_options(family_groups: list[dict]) -> list[dict[str, str]]:
    filter_options = [{"id": "all", "label": "Tümü"}]
    seen_ids: set[str] = {"all"}
    family_label_map = {
        "dishwashing_liquid": "Sıvı",
        "dishwashing_spray": "Sprey",
        "dishwasher_tablet": "Tablet",
        "cleaning_wipes": "Havlu",
        "laundry_liquid": "Sıvı",
        "laundry_powder": "Toz",
        "laundry_capsule": "Kapsül",
        "bleach": "Çamaşır suyu",
        "surface_cleaner": "Yüzey",
        "dishwasher_salt": "Tuz",
        "rinse_aid": "Parlatıcı",
        "dishwasher_cleaner": "Temizleyici",
    }

    for family_group in family_groups:
        _, family_key = _parse_cleaning_family_id(family_group.get("family_id"))
        if not family_key or family_key in seen_ids:
            continue
        label = family_label_map.get(family_key, str(family_group.get("family_label") or "").strip())
        label = label or "Tümü"
        filter_options.append({"id": family_key, "label": label})
        seen_ids.add(family_key)

    return filter_options


def select_brand_only_cleaning_default_group(
    catalog_df: pd.DataFrame,
    safe_groups: list[dict],
    search_text: str,
) -> dict | None:
    brand_token = resolved_cleaning_brand_token(search_text)
    if not brand_token:
        return None

    preferred_subtypes = preferred_cleaning_subtypes({brand_token})
    subtype_rank = {subtype: index for index, subtype in enumerate(preferred_subtypes)}
    candidate_groups: list[tuple[tuple[int, int, str], dict]] = []

    for group in safe_groups:
        if group.get("selection_type") != "product":
            continue
        group_profile = _cleaning_group_profile(catalog_df, group)
        if not group_profile:
            continue
        if group_profile.get("brand_token") == brand_token:
            candidate_groups.append(
                (
                    (
                        subtype_rank.get(
                            _text_value(group_profile.get("subtype")),
                            len(subtype_rank) + 1,
                        ),
                        0 if group_profile.get("coverage_status") == "comparable" else 1,
                        str(group.get("selection_id") or ""),
                    ),
                    group,
                )
            )

    if candidate_groups:
        candidate_groups.sort(key=lambda item: item[0])
        return candidate_groups[0][1]

    return None
