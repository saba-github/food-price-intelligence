from __future__ import annotations

from difflib import SequenceMatcher
from itertools import product
import logging
import re
from typing import Any

import pandas as pd

from pipeline.transforms import normalize_text

logger = logging.getLogger(__name__)


CLEANING_BRANDS = {
    "abc",
    "ace",
    "ariel",
    "asperox",
    "bingo",
    "cif",
    "domestos",
    "fairy",
    "finish",
    "omo",
    "pril",
    "renax",
    "rinso",
    "sleepy",
    "tursil",
    "tursilmatik",
    "yumos",
}

DISHWASHING_BRANDS = {"fairy", "pril"}
DISHWASHER_BRANDS = {"finish"}
LAUNDRY_BRANDS = {"omo", "ariel"}
BLEACH_BRANDS = {"domestos"}

PACKAGE_FORMAT_STANDARD_SINGLE_PACK = "standard_single_pack"
PACKAGE_FORMAT_MULTIPACK_BUNDLE = "multipack_bundle"
PACKAGE_FORMAT_SPRAY = "spray"
PACKAGE_FORMAT_TABLET_CAPSULE = "tablet_capsule"
SPRAY_TOKENS = {"spray", "sprey"}

CLEANING_CONTEXT_TOKENS = {
    "banyo",
    "bulasik",
    "camasir",
    "deterjan",
    "deterjani",
    "hepsi1arada",
    "hepsi",
    "jel",
    "kapsul",
    "kapsulu",
    "kopuk",
    "makine",
    "makinesi",
    "parlatici",
    "parlaticisi",
    "pods",
    "spray",
    "sivi",
    "sprey",
    "tablet",
    "tableti",
    "temizleyici",
    "toz",
    "yikama",
    "yuzey",
}

_BUNDLE_SIGNATURE_PATTERN = re.compile(
    r"\b(\d+)\s*x\s*(\d+(?:[.,]\d+)?(?:\s*\+\s*\d+(?:[.,]\d+)?)?)\b"
)
_PACK_COUNT_HINT_PATTERN = re.compile(r"\b\d+\s*'?(?:li|lu)\b")
_VOLUME_HINT_PATTERN = re.compile(r"\b\d+(?:[.,]\d+)?\s*(?:ml|l)\b")
_WEIGHT_HINT_PATTERN = re.compile(r"\b\d+(?:[.,]\d+)?\s*(?:g|gr|kg)\b")
_CLEANING_VARIANT_ALIASES = {
    "elma": "elma",
    "apple": "elma",
    "limon": "limon",
    "lemon": "limon",
    "aloe": "aloe",
    "portakal": "portakal",
    "orange": "portakal",
    "sirke": "sirke",
    "vinegar": "sirke",
    "sensitive": "sensitive",
    "platinum": "platinum",
}

CLEANING_BRAND_DISPLAY_NAMES = {
    "fairy": "Fairy",
    "pril": "Pril",
    "finish": "Finish",
    "omo": "Omo",
    "ariel": "Ariel",
    "domestos": "Domestos",
}


def _token_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [
        token
        for token in normalize_text(value).replace("-", " ").replace("&", " ").split()
        if token
    ]


def cleaning_tokens(value: str | None) -> set[str]:
    return set(_token_list(value))


def extract_cleaning_bundle_signature(value: str | None) -> str | None:
    if not value:
        return None

    normalized_value = normalize_text(value)
    match = _BUNDLE_SIGNATURE_PATTERN.search(normalized_value)
    if not match:
        return None

    bundle_count = match.group(1)
    bundle_size = re.sub(r"\s+", "", match.group(2)).replace(",", ".")
    return f"{bundle_count}x{bundle_size}"


def _extract_brand_token(tokens: list[str]) -> str | None:
    for token in tokens:
        if token in CLEANING_BRANDS:
            return token
    return tokens[0] if tokens else None


def _extract_variant_token(tokens: list[str]) -> str | None:
    for token in tokens:
        canonical = _CLEANING_VARIANT_ALIASES.get(token)
        if canonical:
            return canonical
    return None


def cleaning_variant_from_query_tokens(query_tokens: set[str]) -> str | None:
    for token in query_tokens:
        canonical = _CLEANING_VARIANT_ALIASES.get(token)
        if canonical:
            return canonical
    return None


def cleaning_brand_display_name(brand_token: str | None) -> str:
    if not brand_token:
        return ""
    return CLEANING_BRAND_DISPLAY_NAMES.get(brand_token, brand_token.title())


def infer_cleaning_product_profile(product_name: str | None) -> dict[str, object]:
    normalized_name = normalize_text(product_name or "")
    tokens = _token_list(product_name)
    token_set = set(tokens)

    subtype = None
    brand_token = _extract_brand_token(tokens)
    variant_token = _extract_variant_token(tokens)
    bundle_signature = extract_cleaning_bundle_signature(product_name)
    has_volume_hint = bool(_VOLUME_HINT_PATTERN.search(normalized_name))
    has_weight_hint = bool(_WEIGHT_HINT_PATTERN.search(normalized_name))
    has_pack_count_hint = bool(_PACK_COUNT_HINT_PATTERN.search(normalized_name))
    has_dishwasher_context = (
        "bulasik" in token_set
        and (
            "makinesi" in token_set
            or "makine" in token_set
            or brand_token in DISHWASHER_BRANDS
        )
    )
    has_tablet_like = bool(
        {"tablet", "tableti", "kapsul", "kapsulu", "pods"} & token_set
    )
    implicit_dishwasher_tablet = (
        brand_token in DISHWASHING_BRANDS
        and not has_volume_hint
        and has_pack_count_hint
        and has_weight_hint
    )

    if {"camasir", "suyu"}.issubset(token_set):
        subtype = "bleach"
    elif "parlatici" in token_set or "parlaticisi" in token_set:
        subtype = "rinse_aid"
    elif ({"tuz", "tuzu"} & token_set) and (
        has_dishwasher_context or brand_token in DISHWASHER_BRANDS
    ):
        subtype = "dishwasher_salt"
    elif (has_tablet_like or implicit_dishwasher_tablet) and (
        has_dishwasher_context
        or brand_token in DISHWASHER_BRANDS
        or brand_token in DISHWASHING_BRANDS
        or ("bulasik" in token_set and "tableti" in token_set)
    ):
        subtype = "dishwasher_tablet"
    elif "temizleyici" in token_set and (
        has_dishwasher_context or brand_token in DISHWASHER_BRANDS
    ):
        subtype = "dishwasher_cleaner"
    elif ("bulasik" in token_set and {"deterjan", "deterjani"} & token_set) or (
        brand_token in DISHWASHING_BRANDS
        and (
            {"limon", "elma", "aloe", "portakal", "sensitive"} & token_set
            or bool(SPRAY_TOKENS & token_set)
            or {"elde", "yikama"} <= token_set
            or not (
                has_dishwasher_context
                or has_tablet_like
                or {"parlatici", "parlaticisi", "temizleyici", "tuz", "tuzu"} & token_set
            )
        )
    ):
        if "makinesi" not in token_set and "makine" not in token_set:
            subtype = "dishwashing_liquid"
    elif (
        "camasir" in token_set and {"deterjan", "deterjani"} & token_set
    ) or (
        brand_token in LAUNDRY_BRANDS and {"deterjan", "deterjani"} & token_set
    ):
        if has_tablet_like:
            subtype = "laundry_capsule"
        elif "sivi" in token_set or "jel" in token_set:
            subtype = "laundry_liquid"
        elif "toz" in token_set:
            subtype = "laundry_powder"
    elif (
        "yuzey" in token_set
        or (
            "temizleyici" in token_set
            and {"banyo", "mutfak", "kopuk", "sprey"} & token_set
        )
    ):
        subtype = "surface_cleaner"
    elif brand_token in BLEACH_BRANDS:
        if {"banyo", "mutfak", "kopuk", "sprey", "yuzey"} & token_set:
            subtype = "surface_cleaner"
        else:
            subtype = "bleach"

    package_format = None
    if SPRAY_TOKENS & token_set:
        package_format = PACKAGE_FORMAT_SPRAY
    elif has_tablet_like or implicit_dishwasher_tablet:
        package_format = PACKAGE_FORMAT_TABLET_CAPSULE
    elif bundle_signature:
        package_format = PACKAGE_FORMAT_MULTIPACK_BUNDLE
    elif subtype in {"dishwashing_liquid", "laundry_liquid"}:
        package_format = PACKAGE_FORMAT_STANDARD_SINGLE_PACK

    is_cleaning = bool(subtype or token_set & CLEANING_CONTEXT_TOKENS or token_set & CLEANING_BRANDS)

    return {
        "tokens": tuple(tokens),
        "brand_token": brand_token,
        "variant_token": variant_token,
        "subtype": subtype,
        "package_format": package_format,
        "bundle_signature": bundle_signature,
        "is_cleaning": is_cleaning,
    }


def analyze_cleaning_pair(
    a101_name: str | None,
    migros_name: str | None,
    a101_unit: str | None = None,
    migros_unit: str | None = None,
    a101_quantity: object | None = None,
    migros_quantity: object | None = None,
) -> dict[str, object]:
    a101_profile = infer_cleaning_product_profile(a101_name)
    migros_profile = infer_cleaning_product_profile(migros_name)
    a101_brand = a101_profile.get("brand_token")
    migros_brand = migros_profile.get("brand_token")
    a101_variant = a101_profile.get("variant_token")
    migros_variant = migros_profile.get("variant_token")
    a101_subtype = a101_profile.get("subtype")
    migros_subtype = migros_profile.get("subtype")
    a101_package_format = a101_profile.get("package_format")
    migros_package_format = migros_profile.get("package_format")

    same_brand = bool(a101_brand and migros_brand and a101_brand == migros_brand)
    same_subtype = bool(a101_subtype and migros_subtype and a101_subtype == migros_subtype)
    same_variant = bool(
        a101_variant
        and migros_variant
        and a101_variant == migros_variant
    )
    same_package_format = bool(
        a101_package_format
        and migros_package_format
        and a101_package_format == migros_package_format
    )
    soft_equivalent = False

    def _to_float(value: object | None) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _within_tolerance(left: float | None, right: float | None, tolerance: float = 0.2) -> bool:
        if left is None or right is None or left <= 0 or right <= 0:
            return False
        baseline = max(left, right)
        return abs(left - right) / baseline <= tolerance

    same_or_unknown_variant = (
        same_variant
        or not a101_variant
        or not migros_variant
    )
    compatible_package_formats = (
        (a101_package_format in {None, PACKAGE_FORMAT_STANDARD_SINGLE_PACK})
        and (migros_package_format in {None, PACKAGE_FORMAT_STANDARD_SINGLE_PACK})
    )

    if (
        same_brand
        and same_subtype
        and a101_subtype == "dishwashing_liquid"
        and a101_unit == "liter"
        and migros_unit == "liter"
        and same_or_unknown_variant
        and compatible_package_formats
        and _within_tolerance(_to_float(a101_quantity), _to_float(migros_quantity))
    ):
        soft_equivalent = True
        logger.info(
            "cleaning soft equivalence matched a101=%r migros=%r subtype=%s quantities=(%s,%s)",
            a101_name,
            migros_name,
            a101_subtype,
            a101_quantity,
            migros_quantity,
        )

    return {
        "is_cleaning_pair": bool(
            a101_profile.get("is_cleaning") or migros_profile.get("is_cleaning")
        ),
        "a101_brand_token": a101_brand,
        "migros_brand_token": migros_brand,
        "a101_variant_token": a101_variant,
        "migros_variant_token": migros_variant,
        "a101_subtype": a101_subtype,
        "migros_subtype": migros_subtype,
        "a101_package_format": a101_package_format,
        "migros_package_format": migros_package_format,
        "same_brand": same_brand,
        "same_variant": same_variant,
        "same_subtype": same_subtype,
        "same_package_format": same_package_format,
        "soft_equivalent": soft_equivalent,
    }


def cleaning_brand_from_query_tokens(query_tokens: set[str]) -> str | None:
    for token in query_tokens:
        if token in CLEANING_BRANDS:
            return token
    return None


def nearest_cleaning_brand_token(value: str | None) -> str | None:
    tokens = _token_list(value)
    if len(tokens) != 1:
        return None

    token = tokens[0]
    if token in CLEANING_BRANDS:
        return token

    best_brand = None
    best_ratio = 0.0
    for brand in CLEANING_BRANDS:
        ratio = SequenceMatcher(None, token, brand).ratio()
        if ratio > best_ratio:
            best_brand = brand
            best_ratio = ratio

    if best_brand and best_ratio >= 0.66 and token[:1] == best_brand[:1]:
        return best_brand
    return None


def explicit_cleaning_query_subtype(query_tokens: set[str]) -> str | None:
    if "finish" in query_tokens and "tuz" in query_tokens:
        return "dishwasher_salt"
    if {"tablet", "tableti", "kapsul", "kapsulu", "pods"} & query_tokens:
        if (
            "finish" in query_tokens
            or query_tokens & DISHWASHING_BRANDS
            or "bulasik" in query_tokens
        ):
            return "dishwasher_tablet"
    if "parlatici" in query_tokens or "parlaticisi" in query_tokens:
        return "rinse_aid"
    if ("finish" in query_tokens and "temizleyici" in query_tokens) or (
        "makinesi" in query_tokens and "temizleyici" in query_tokens
    ):
        return "dishwasher_cleaner"
    if "camasir" in query_tokens and "suyu" in query_tokens:
        return "bleach"
    if "yuzey" in query_tokens and "temizleyici" in query_tokens:
        return "surface_cleaner"
    if SPRAY_TOKENS & query_tokens and query_tokens & DISHWASHING_BRANDS:
        return "dishwashing_liquid"
    if "bulasik" in query_tokens and {"deterjan", "deterjani"} & query_tokens:
        return "dishwashing_liquid"
    if ("kapsul" in query_tokens or "pods" in query_tokens) and {
        "deterjan",
        "deterjani",
    } & query_tokens:
        return "laundry_capsule"
    if "toz" in query_tokens and {"deterjan", "deterjani"} & query_tokens:
        return "laundry_powder"
    if "sivi" in query_tokens and {"deterjan", "deterjani"} & query_tokens:
        return "laundry_liquid"
    return None


def preferred_cleaning_subtypes(query_tokens: set[str]) -> tuple[str, ...]:
    explicit_subtype = explicit_cleaning_query_subtype(query_tokens)
    if explicit_subtype:
        return (explicit_subtype,)

    brand_token = cleaning_brand_from_query_tokens(query_tokens)
    if brand_token in DISHWASHING_BRANDS:
        return (
            "dishwashing_liquid",
            "dishwasher_tablet",
            "dishwasher_salt",
            "rinse_aid",
            "dishwasher_cleaner",
            "surface_cleaner",
        )
    if brand_token in DISHWASHER_BRANDS:
        return (
            "dishwasher_tablet",
            "dishwasher_salt",
            "rinse_aid",
            "dishwasher_cleaner",
            "dishwashing_liquid",
        )
    if brand_token in LAUNDRY_BRANDS:
        return (
            "laundry_liquid",
            "laundry_powder",
            "laundry_capsule",
        )
    if brand_token in BLEACH_BRANDS:
        return ("bleach", "surface_cleaner")

    return ()


def is_cleaning_context_text(product_name: str | None) -> bool:
    return bool(infer_cleaning_product_profile(product_name).get("is_cleaning"))


def _format_liter_quantity(quantity: float | int | None) -> str | None:
    if quantity is None:
        return None
    numeric_value = float(quantity)
    if numeric_value.is_integer():
        return str(int(numeric_value))
    return f"{numeric_value:g}"


def _dishwashing_liquid_name(
    brand_token: str,
    quantity_liters: float | None,
    variant_token: str | None = None,
) -> str:
    parts = [brand_token]
    if variant_token:
        parts.append(variant_token)
    parts.append("bulasik deterjani")
    if quantity_liters is not None:
        parts.append(f"{_format_liter_quantity(quantity_liters)} l")
    return " ".join(parts)


def _dishwashing_spray_name(
    brand_token: str,
    quantity_liters: float | None,
    variant_token: str | None = None,
) -> str:
    parts = [brand_token, "sprey"]
    if variant_token:
        parts.append(variant_token)
    if quantity_liters is not None:
        parts.append(f"{_format_liter_quantity(quantity_liters)} l")
    return " ".join(parts)


def _cleaning_entry_display_name(entry: dict[str, Any]) -> str:
    if entry.get("package_format") == PACKAGE_FORMAT_SPRAY:
        return _dishwashing_spray_name(
            str(entry["brand_token"]),
            _to_float(entry.get("normalized_quantity")),
            entry.get("variant_token"),
        )

    return _dishwashing_liquid_name(
        str(entry["brand_token"]),
        _to_float(entry.get("normalized_quantity")),
        entry.get("variant_token"),
    )


def _is_supported_dishwashing_entry(profile: dict[str, object]) -> bool:
    if profile.get("subtype") != "dishwashing_liquid":
        return False

    package_format = profile.get("package_format")
    return package_format in {
        None,
        PACKAGE_FORMAT_STANDARD_SINGLE_PACK,
        PACKAGE_FORMAT_SPRAY,
    }


def _cleaning_entry_from_catalog_row(
    row: pd.Series,
    retailer: str,
) -> dict[str, Any] | None:
    source_product_name = row.get(f"{retailer}_source_product_name")
    if not source_product_name:
        return None

    profile = infer_cleaning_product_profile(source_product_name)
    if not _is_supported_dishwashing_entry(profile):
        return None

    quantity = row.get(f"{retailer}_normalized_quantity")
    unit = row.get(f"{retailer}_normalized_unit")
    unit, quantity = _infer_liter_measurement(source_product_name, unit, quantity)
    if unit != "liter" or quantity is None:
        return None

    return {
        "retailer": retailer,
        "source_product_name": source_product_name,
        "brand_token": profile.get("brand_token"),
        "variant_token": profile.get("variant_token"),
        "subtype": profile.get("subtype"),
        "package_format": profile.get("package_format"),
        "normalized_unit": unit,
        "normalized_quantity": quantity,
        "raw_price": row.get(f"{retailer}_raw_price"),
        "comparison_price": row.get(f"{retailer}_comparison_price"),
    }


def _cleaning_entry_from_price_row(
    row: dict[str, Any],
    retailer: str,
) -> dict[str, Any] | None:
    source_product_name = row.get(f"{retailer}_source_product_name")
    if not source_product_name:
        return None

    profile = infer_cleaning_product_profile(source_product_name)
    if not _is_supported_dishwashing_entry(profile):
        return None

    quantity = row.get(f"{retailer}_normalized_quantity")
    unit = row.get(f"{retailer}_normalized_unit")
    unit, quantity = _infer_liter_measurement(source_product_name, unit, quantity)
    if unit != "liter" or quantity is None:
        return None

    return {
        "retailer": retailer,
        "source_product_name": source_product_name,
        "brand_token": profile.get("brand_token"),
        "variant_token": profile.get("variant_token"),
        "subtype": profile.get("subtype"),
        "package_format": profile.get("package_format"),
        "normalized_unit": unit,
        "normalized_quantity": quantity,
        "price": row.get(f"{retailer}_price"),
    }


def _to_float(value: object | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _same_or_unknown_variant(
    left_variant: str | None,
    right_variant: str | None,
) -> bool:
    return (
        left_variant == right_variant
        or left_variant is None
        or right_variant is None
    )


def _soft_pair_variant_rank(
    left_variant: str | None,
    right_variant: str | None,
) -> int:
    if left_variant and right_variant and left_variant == right_variant:
        return 0
    if {left_variant, right_variant} == {None, "limon"}:
        return 1
    if left_variant is None and right_variant is None:
        return 2
    if left_variant is None or right_variant is None:
        return 3
    return 4


def _exactish_liter_match(
    left_quantity: object | None,
    right_quantity: object | None,
    tolerance: float = 0.01,
) -> bool:
    left_value = _to_float(left_quantity)
    right_value = _to_float(right_quantity)
    if left_value is None or right_value is None:
        return False
    return abs(left_value - right_value) <= tolerance


def _synthesized_cleaning_variant(
    left_variant: str | None,
    right_variant: str | None,
) -> str | None:
    if left_variant and right_variant and left_variant == right_variant:
        return left_variant
    return None


def _infer_liter_measurement(
    source_product_name: str | None,
    unit: str | None,
    quantity: object | None,
) -> tuple[str | None, object | None]:
    if unit == "liter" and quantity is not None:
        return unit, quantity

    normalized_name = normalize_text(source_product_name or "")
    liter_match = re.search(r"\b(\d+(?:[.,]\d+)?)\s*l\b", normalized_name)
    if liter_match:
        return "liter", float(liter_match.group(1).replace(",", "."))

    ml_match = re.search(r"\b(\d+(?:[.,]\d+)?)\s*ml\b", normalized_name)
    if ml_match:
        return "liter", float(ml_match.group(1).replace(",", ".")) / 1000.0

    return unit, quantity


def _dishwashing_soft_pair(
    left_entry: dict[str, Any],
    right_entry: dict[str, Any],
) -> bool:
    left_package_format = left_entry.get("package_format")
    right_package_format = right_entry.get("package_format")
    same_liquid_package_format = (
        left_package_format in {None, PACKAGE_FORMAT_STANDARD_SINGLE_PACK}
        and right_package_format in {None, PACKAGE_FORMAT_STANDARD_SINGLE_PACK}
    )
    same_spray_package_format = (
        left_package_format == PACKAGE_FORMAT_SPRAY
        and right_package_format == PACKAGE_FORMAT_SPRAY
    )

    return (
        left_entry.get("brand_token")
        and left_entry.get("brand_token") == right_entry.get("brand_token")
        and left_entry.get("subtype") == "dishwashing_liquid"
        and right_entry.get("subtype") == "dishwashing_liquid"
        and left_entry.get("normalized_unit") == "liter"
        and right_entry.get("normalized_unit") == "liter"
        and (same_liquid_package_format or same_spray_package_format)
        and _same_or_unknown_variant(
            left_entry.get("variant_token"),
            right_entry.get("variant_token"),
        )
        and _exactish_liter_match(
            left_entry.get("normalized_quantity"),
            right_entry.get("normalized_quantity"),
        )
    )


def _build_catalog_cleaning_row(
    standardized_product_name: str,
    a101_entry: dict[str, Any] | None,
    migros_entry: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "standardized_product_name": standardized_product_name,
        "source_count": 2,
        "available_retailers": "a101, migros",
        "a101_source_product_name": a101_entry.get("source_product_name") if a101_entry else None,
        "migros_source_product_name": migros_entry.get("source_product_name") if migros_entry else None,
        "a101_normalized_unit": a101_entry.get("normalized_unit") if a101_entry else None,
        "migros_normalized_unit": migros_entry.get("normalized_unit") if migros_entry else None,
        "a101_normalized_quantity": a101_entry.get("normalized_quantity") if a101_entry else None,
        "migros_normalized_quantity": migros_entry.get("normalized_quantity") if migros_entry else None,
        "a101_raw_price": a101_entry.get("raw_price") if a101_entry else None,
        "migros_raw_price": migros_entry.get("raw_price") if migros_entry else None,
        "a101_comparison_price": a101_entry.get("comparison_price") if a101_entry else None,
        "migros_comparison_price": migros_entry.get("comparison_price") if migros_entry else None,
        "comparison_price_unit": "liter",
        "same_unit_flag": True,
        "same_quantity_flag": True,
        "comparison_confidence": "high",
        "coverage_status": "comparable",
        "comparison_review_reason": None,
    }


def _build_catalog_single_source_cleaning_row(
    standardized_product_name: str,
    entry: dict[str, Any],
) -> dict[str, Any]:
    retailer = str(entry["retailer"])
    other_retailer = "migros" if retailer == "a101" else "a101"
    raw_price = entry.get("raw_price")
    comparison_price = entry.get("comparison_price")
    normalized_unit = entry.get("normalized_unit")
    normalized_quantity = entry.get("normalized_quantity")

    return {
        "standardized_product_name": standardized_product_name,
        "source_count": 1,
        "available_retailers": retailer,
        "a101_source_product_name": entry.get("source_product_name") if retailer == "a101" else None,
        "migros_source_product_name": entry.get("source_product_name") if retailer == "migros" else None,
        "a101_normalized_unit": normalized_unit if retailer == "a101" else None,
        "migros_normalized_unit": normalized_unit if retailer == "migros" else None,
        "a101_normalized_quantity": normalized_quantity if retailer == "a101" else None,
        "migros_normalized_quantity": normalized_quantity if retailer == "migros" else None,
        "a101_raw_price": raw_price if retailer == "a101" else None,
        "migros_raw_price": raw_price if retailer == "migros" else None,
        "a101_comparison_price": comparison_price if retailer == "a101" else None,
        "migros_comparison_price": comparison_price if retailer == "migros" else None,
        "comparison_price_unit": "liter",
        "same_unit_flag": False,
        "same_quantity_flag": False,
        "comparison_confidence": "single_source",
        "coverage_status": f"only_{retailer}",
        "comparison_review_reason": None,
        f"{other_retailer}_source_product_name": None,
        f"{other_retailer}_normalized_unit": None,
        f"{other_retailer}_normalized_quantity": None,
        f"{other_retailer}_raw_price": None,
        f"{other_retailer}_comparison_price": None,
    }


def _build_price_cleaning_row(
    standardized_product_name: str,
    a101_entry: dict[str, Any] | None,
    migros_entry: dict[str, Any] | None,
) -> dict[str, Any]:
    a101_price = a101_entry.get("price") if a101_entry else None
    migros_price = migros_entry.get("price") if migros_entry else None
    if a101_price is None and migros_price is None:
        cheaper_source = None
    elif a101_price is None:
        cheaper_source = "migros"
    elif migros_price is None:
        cheaper_source = "a101"
    elif a101_price < migros_price:
        cheaper_source = "a101"
    elif migros_price < a101_price:
        cheaper_source = "migros"
    else:
        cheaper_source = "same"

    return {
        "standardized_product_name": standardized_product_name,
        "canonical_name": standardized_product_name,
        "a101_price": a101_price,
        "migros_price": migros_price,
        "cheaper_source": cheaper_source,
        "compared_at": None,
        "same_unit_flag": True,
        "same_quantity_flag": True,
        "comparison_confidence": "high",
        "comparison_review_reason": None,
        "coverage_status": "comparable",
        "a101_source_product_name": a101_entry.get("source_product_name") if a101_entry else None,
        "migros_source_product_name": migros_entry.get("source_product_name") if migros_entry else None,
        "a101_normalized_unit": a101_entry.get("normalized_unit") if a101_entry else None,
        "migros_normalized_unit": migros_entry.get("normalized_unit") if migros_entry else None,
        "a101_normalized_quantity": a101_entry.get("normalized_quantity") if a101_entry else None,
        "migros_normalized_quantity": migros_entry.get("normalized_quantity") if migros_entry else None,
    }


def _build_price_single_source_cleaning_row(
    standardized_product_name: str,
    entry: dict[str, Any],
) -> dict[str, Any]:
    retailer = str(entry["retailer"])
    price = entry.get("price")
    normalized_unit = entry.get("normalized_unit")
    normalized_quantity = entry.get("normalized_quantity")

    return {
        "standardized_product_name": standardized_product_name,
        "canonical_name": standardized_product_name,
        "a101_price": price if retailer == "a101" else None,
        "migros_price": price if retailer == "migros" else None,
        "cheaper_source": retailer,
        "compared_at": None,
        "same_unit_flag": False,
        "same_quantity_flag": False,
        "comparison_confidence": "single_source",
        "comparison_review_reason": None,
        "coverage_status": f"only_{retailer}",
        "a101_source_product_name": entry.get("source_product_name") if retailer == "a101" else None,
        "migros_source_product_name": entry.get("source_product_name") if retailer == "migros" else None,
        "a101_normalized_unit": normalized_unit if retailer == "a101" else None,
        "migros_normalized_unit": normalized_unit if retailer == "migros" else None,
        "a101_normalized_quantity": normalized_quantity if retailer == "a101" else None,
        "migros_normalized_quantity": normalized_quantity if retailer == "migros" else None,
    }


def _is_spray_entry(entry: dict[str, Any]) -> bool:
    return entry.get("package_format") == PACKAGE_FORMAT_SPRAY


def _is_spray_source_name(source_product_name: str | None) -> bool:
    if not source_product_name:
        return False
    profile = infer_cleaning_product_profile(source_product_name)
    return profile.get("subtype") == "dishwashing_liquid" and profile.get("package_format") == PACKAGE_FORMAT_SPRAY


def augment_catalog_with_cleaning_rows(catalog_df: pd.DataFrame) -> pd.DataFrame:
    dishwashing_entries: list[dict[str, Any]] = []
    for _, row in catalog_df.iterrows():
        for retailer in ("a101", "migros"):
            entry = _cleaning_entry_from_catalog_row(row, retailer)
            if entry is not None:
                dishwashing_entries.append(entry)

    if not dishwashing_entries:
        return catalog_df.copy()

    synthesized_rows: list[dict[str, Any]] = []
    synthesized_names: set[str] = set()
    synthesized_single_source_entries: dict[str, dict[str, Any]] = {}
    a101_entries = [entry for entry in dishwashing_entries if entry["retailer"] == "a101"]
    migros_entries = [entry for entry in dishwashing_entries if entry["retailer"] == "migros"]

    candidate_pairs = sorted(
        product(a101_entries, migros_entries),
        key=lambda pair: (
            pair[0].get("brand_token") != pair[1].get("brand_token"),
            _soft_pair_variant_rank(
                pair[0].get("variant_token"),
                pair[1].get("variant_token"),
            ),
            abs(
                (_to_float(pair[0].get("normalized_quantity")) or 0.0)
                - (_to_float(pair[1].get("normalized_quantity")) or 0.0)
            ),
        ),
    )

    for a101_entry, migros_entry in candidate_pairs:
        if not _dishwashing_soft_pair(a101_entry, migros_entry):
            continue

        standardized_product_name = _cleaning_entry_display_name(
            {
                **a101_entry,
                "variant_token": _synthesized_cleaning_variant(
                    a101_entry.get("variant_token"),
                    migros_entry.get("variant_token"),
                ),
            }
        )
        if standardized_product_name in synthesized_names:
            continue

        synthesized_rows.append(
            _build_catalog_cleaning_row(
                standardized_product_name,
                a101_entry,
                migros_entry,
            )
        )
        synthesized_names.add(standardized_product_name)

    for entry in dishwashing_entries:
        if not _is_spray_entry(entry):
            continue

        standardized_product_name = _cleaning_entry_display_name(entry)
        if standardized_product_name in synthesized_names:
            continue
        synthesized_single_source_entries.setdefault(standardized_product_name, entry)

    synthesized_rows.extend(
        _build_catalog_single_source_cleaning_row(name, entry)
        for name, entry in synthesized_single_source_entries.items()
    )

    if not synthesized_rows:
        return catalog_df.copy()

    remaining_catalog_df = catalog_df.loc[
        ~catalog_df.apply(
            lambda row: (
                row["standardized_product_name"] in synthesized_names
                or _is_spray_source_name(row.get("a101_source_product_name"))
                or _is_spray_source_name(row.get("migros_source_product_name"))
            ),
            axis=1,
        )
    ].reset_index(drop=True)
    synthesized_df = pd.DataFrame(synthesized_rows)
    return pd.concat(
        [remaining_catalog_df, synthesized_df],
        ignore_index=True,
        sort=False,
    ).sort_values(
        by=["standardized_product_name", "coverage_status"],
        kind="stable",
    ).reset_index(drop=True)


def synthesize_cleaning_price_rows(
    latest_price_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    dishwashing_entries: list[dict[str, Any]] = []
    for row in latest_price_rows:
        for retailer in ("a101", "migros"):
            entry = _cleaning_entry_from_price_row(row, retailer)
            if entry is not None:
                dishwashing_entries.append(entry)

    if not dishwashing_entries:
        return []

    synthesized_rows: list[dict[str, Any]] = []
    synthesized_names: set[str] = set()
    synthesized_single_source_entries: dict[str, dict[str, Any]] = {}
    a101_entries = [entry for entry in dishwashing_entries if entry["retailer"] == "a101"]
    migros_entries = [entry for entry in dishwashing_entries if entry["retailer"] == "migros"]

    candidate_pairs = sorted(
        product(a101_entries, migros_entries),
        key=lambda pair: (
            pair[0].get("brand_token") != pair[1].get("brand_token"),
            _soft_pair_variant_rank(
                pair[0].get("variant_token"),
                pair[1].get("variant_token"),
            ),
            abs(
                (_to_float(pair[0].get("normalized_quantity")) or 0.0)
                - (_to_float(pair[1].get("normalized_quantity")) or 0.0)
            ),
        ),
    )

    for a101_entry, migros_entry in candidate_pairs:
        if not _dishwashing_soft_pair(a101_entry, migros_entry):
            continue

        standardized_product_name = _cleaning_entry_display_name(
            {
                **a101_entry,
                "variant_token": _synthesized_cleaning_variant(
                    a101_entry.get("variant_token"),
                    migros_entry.get("variant_token"),
                ),
            }
        )
        if standardized_product_name in synthesized_names:
            continue

        synthesized_rows.append(
            _build_price_cleaning_row(
                standardized_product_name,
                a101_entry,
                migros_entry,
            )
        )
        synthesized_names.add(standardized_product_name)

    for entry in dishwashing_entries:
        if not _is_spray_entry(entry):
            continue

        standardized_product_name = _cleaning_entry_display_name(entry)
        if standardized_product_name in synthesized_names:
            continue
        synthesized_single_source_entries.setdefault(standardized_product_name, entry)

    synthesized_rows.extend(
        _build_price_single_source_cleaning_row(name, entry)
        for name, entry in synthesized_single_source_entries.items()
    )

    return synthesized_rows
