from __future__ import annotations

from itertools import product
from typing import Any

import pandas as pd

from pipeline.transforms import calculate_price_per_unit, infer_paper_product_profile


COMPARABLE_STATUS = "comparable"
COMPARISON_REVIEW_STATUS = "comparison_review_required"
PAPER_TOWEL_KIND = "paper_towel"
PAPER_TOWEL_PREFERRED_ROLL_COUNTS = {6, 8, 12}


def _format_roll_count(roll_count: float | int | None) -> str | None:
    if roll_count is None:
        return None

    numeric_value = float(roll_count)
    if numeric_value.is_integer():
        return str(int(numeric_value))
    return f"{numeric_value:g}"


def _paper_towel_name(brand_token: str, roll_count: float | None = None) -> str:
    if roll_count is None:
        return f"{brand_token} kagit havlu"
    return f"{brand_token} kagit havlu {_format_roll_count(roll_count)} roll"


def _generic_paper_towel_name(roll_count: float) -> str:
    return f"kagit havlu {_format_roll_count(roll_count)} roll"


def _entry_rank(entry: dict[str, Any]) -> tuple:
    roll_count = entry.get("roll_count")
    preferred_roll_rank = 1
    if roll_count in PAPER_TOWEL_PREFERRED_ROLL_COUNTS:
        preferred_roll_rank = 0

    return (
        0 if entry.get("standard_roll_pack") else 1,
        0 if not entry.get("is_special_format") else 1,
        0 if entry.get("product_line_tokens") else 1,
        preferred_roll_rank,
        abs(float(roll_count or 0) - 8.0) if roll_count is not None else 999.0,
        entry.get("source_product_name") or "",
    )


def _relative_roll_difference(left: dict[str, Any], right: dict[str, Any]) -> float | None:
    left_count = left.get("roll_count")
    right_count = right.get("roll_count")
    if left_count is None or right_count is None:
        return None

    larger = max(float(left_count), float(right_count))
    if larger <= 0:
        return None
    return abs(float(left_count) - float(right_count)) / larger


def _build_catalog_entry(row: pd.Series, retailer: str) -> dict[str, Any] | None:
    source_product_name = row.get(f"{retailer}_source_product_name")
    if not source_product_name:
        return None

    profile = infer_paper_product_profile(source_product_name)
    if profile.get("kind") != PAPER_TOWEL_KIND:
        return None

    raw_price = row.get(f"{retailer}_raw_price")
    roll_count = profile.get("roll_count")
    comparison_price = row.get(f"{retailer}_comparison_price")
    if roll_count is not None and raw_price is not None:
        comparison_price = calculate_price_per_unit(raw_price, roll_count)

    return {
        "retailer": retailer,
        "source_product_name": source_product_name,
        "brand_token": profile.get("brand_token"),
        "product_line": profile.get("product_line"),
        "product_line_tokens": profile.get("product_line_tokens") or (),
        "roll_count": roll_count,
        "standard_roll_pack": profile.get("standard_roll_pack"),
        "is_special_format": profile.get("is_special_format"),
        "dev_rulo": profile.get("dev_rulo"),
        "asilabilir": profile.get("asilabilir"),
        "el_yuz_havlusu": profile.get("el_yuz_havlusu"),
        "yaprak_based": profile.get("yaprak_based"),
        "multipack_equivalent": profile.get("multipack_equivalent"),
        "raw_price": raw_price,
        "comparison_price": comparison_price,
        "normalized_unit": "roll" if roll_count is not None else row.get(f"{retailer}_normalized_unit"),
        "normalized_quantity": roll_count if roll_count is not None else row.get(f"{retailer}_normalized_quantity"),
    }


def _build_price_entry(row: dict[str, Any], retailer: str) -> dict[str, Any] | None:
    source_product_name = row.get(f"{retailer}_source_product_name")
    if not source_product_name:
        return None

    profile = infer_paper_product_profile(source_product_name)
    if profile.get("kind") != PAPER_TOWEL_KIND:
        return None

    raw_price = row.get(f"{retailer}_price")
    roll_count = profile.get("roll_count")
    comparison_price = raw_price
    if roll_count is not None and raw_price is not None:
        comparison_price = calculate_price_per_unit(raw_price, roll_count)

    return {
        "retailer": retailer,
        "source_product_name": source_product_name,
        "brand_token": profile.get("brand_token"),
        "product_line": profile.get("product_line"),
        "product_line_tokens": profile.get("product_line_tokens") or (),
        "roll_count": roll_count,
        "standard_roll_pack": profile.get("standard_roll_pack"),
        "is_special_format": profile.get("is_special_format"),
        "dev_rulo": profile.get("dev_rulo"),
        "asilabilir": profile.get("asilabilir"),
        "el_yuz_havlusu": profile.get("el_yuz_havlusu"),
        "yaprak_based": profile.get("yaprak_based"),
        "multipack_equivalent": profile.get("multipack_equivalent"),
        "raw_price": raw_price,
        "comparison_price": comparison_price,
        "normalized_unit": "roll" if roll_count is not None else row.get(f"{retailer}_normalized_unit"),
        "normalized_quantity": roll_count if roll_count is not None else row.get(f"{retailer}_normalized_quantity"),
    }


def _paper_towel_entries_from_catalog(catalog_df: pd.DataFrame) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    paper_towel_entries: list[dict[str, Any]] = []
    paper_towel_indexes: set[int] = set()

    for index, row in catalog_df.iterrows():
        row_has_paper_towel = False
        for retailer in ("a101", "migros"):
            entry = _build_catalog_entry(row, retailer)
            if entry is None:
                continue

            row_has_paper_towel = True
            paper_towel_entries.append(entry)

        if row_has_paper_towel:
            paper_towel_indexes.add(index)

    remaining_catalog_df = catalog_df.drop(index=list(paper_towel_indexes)).reset_index(drop=True)
    return paper_towel_entries, remaining_catalog_df


def _paper_towel_entries_from_price_rows(latest_price_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    paper_towel_entries: list[dict[str, Any]] = []

    for row in latest_price_rows:
        for retailer in ("a101", "migros"):
            entry = _build_price_entry(row, retailer)
            if entry is not None:
                paper_towel_entries.append(entry)

    return paper_towel_entries


def _exact_group_key(entry: dict[str, Any]) -> str | None:
    brand_token = entry.get("brand_token")
    roll_count = entry.get("roll_count")
    if not brand_token or roll_count is None:
        return None

    return _paper_towel_name(brand_token, float(roll_count))


def _product_line_state(
    left_entry: dict[str, Any],
    right_entry: dict[str, Any],
) -> str:
    left_line_tokens = tuple(left_entry.get("product_line_tokens") or ())
    right_line_tokens = tuple(right_entry.get("product_line_tokens") or ())

    if left_line_tokens and right_line_tokens:
        if left_line_tokens == right_line_tokens:
            return "same"
        return "mismatch"

    if not left_line_tokens and not right_line_tokens:
        return "unknown_both"

    return "unknown_partial"


def _build_catalog_row(
    standardized_product_name: str,
    a101_entry: dict[str, Any] | None,
    migros_entry: dict[str, Any] | None,
    coverage_status: str,
    comparison_confidence: str,
    comparison_review_reason: str | None,
) -> dict[str, Any]:
    source_count = int(a101_entry is not None) + int(migros_entry is not None)
    retailers = []
    if a101_entry is not None:
        retailers.append("a101")
    if migros_entry is not None:
        retailers.append("migros")

    same_unit_flag = (
        a101_entry is not None
        and migros_entry is not None
        and a101_entry.get("normalized_unit") == "roll"
        and migros_entry.get("normalized_unit") == "roll"
    )
    same_quantity_flag = (
        same_unit_flag
        and a101_entry.get("roll_count") is not None
        and migros_entry.get("roll_count") is not None
        and float(a101_entry["roll_count"]) == float(migros_entry["roll_count"])
    )

    comparison_price_unit = "roll" if source_count == 2 and same_unit_flag else None

    return {
        "standardized_product_name": standardized_product_name,
        "source_count": source_count,
        "available_retailers": ", ".join(retailers),
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
        "comparison_price_unit": comparison_price_unit,
        "same_unit_flag": same_unit_flag,
        "same_quantity_flag": same_quantity_flag,
        "comparison_confidence": comparison_confidence,
        "coverage_status": coverage_status,
        "comparison_review_reason": comparison_review_reason,
    }


def _build_price_row(
    standardized_product_name: str,
    a101_entry: dict[str, Any] | None,
    migros_entry: dict[str, Any] | None,
    comparison_confidence: str,
    comparison_review_reason: str | None,
) -> dict[str, Any]:
    same_unit_flag = (
        a101_entry is not None
        and migros_entry is not None
        and a101_entry.get("normalized_unit") == "roll"
        and migros_entry.get("normalized_unit") == "roll"
    )
    same_quantity_flag = (
        same_unit_flag
        and a101_entry.get("roll_count") is not None
        and migros_entry.get("roll_count") is not None
        and float(a101_entry["roll_count"]) == float(migros_entry["roll_count"])
    )

    if a101_entry and migros_entry:
        if comparison_confidence == "high":
            coverage_status = COMPARABLE_STATUS
        else:
            coverage_status = COMPARISON_REVIEW_STATUS
    elif a101_entry:
        coverage_status = "only_available_at_a101"
    elif migros_entry:
        coverage_status = "only_available_at_migros"
    else:
        coverage_status = "unavailable"

    return {
        "standardized_product_name": standardized_product_name,
        "canonical_name": standardized_product_name,
        "a101_price": a101_entry.get("comparison_price") if a101_entry else None,
        "migros_price": migros_entry.get("comparison_price") if migros_entry else None,
        "cheaper_source": None,
        "compared_at": None,
        "same_unit_flag": same_unit_flag,
        "same_quantity_flag": same_quantity_flag,
        "comparison_confidence": comparison_confidence,
        "comparison_review_reason": comparison_review_reason,
        "coverage_status": coverage_status,
        "a101_source_product_name": a101_entry.get("source_product_name") if a101_entry else None,
        "migros_source_product_name": migros_entry.get("source_product_name") if migros_entry else None,
        "a101_normalized_unit": a101_entry.get("normalized_unit") if a101_entry else None,
        "migros_normalized_unit": migros_entry.get("normalized_unit") if migros_entry else None,
        "a101_normalized_quantity": a101_entry.get("normalized_quantity") if a101_entry else None,
        "migros_normalized_quantity": migros_entry.get("normalized_quantity") if migros_entry else None,
    }


def _paper_towel_pair_state(
    a101_entry: dict[str, Any],
    migros_entry: dict[str, Any],
) -> tuple[str, str | None] | None:
    same_roll_count = (
        a101_entry.get("roll_count") is not None
        and migros_entry.get("roll_count") is not None
        and float(a101_entry["roll_count"]) == float(migros_entry["roll_count"])
    )

    if a101_entry.get("brand_token") != migros_entry.get("brand_token"):
        if same_roll_count:
            return "low", "brand_mismatch"
        return None

    relative_difference = _relative_roll_difference(a101_entry, migros_entry)
    if relative_difference is None:
        return None

    product_line_state = _product_line_state(a101_entry, migros_entry)

    if relative_difference == 0:
        if product_line_state == "mismatch":
            return "medium", "product_line_mismatch"
        if product_line_state in {"unknown_both", "unknown_partial"}:
            return "medium", "product_line_unknown"
        if a101_entry.get("standard_roll_pack") and migros_entry.get("standard_roll_pack"):
            return "high", None
        return "medium", "paper_towel_special_format"

    if relative_difference <= 0.5:
        if product_line_state == "mismatch":
            return "medium", "product_line_mismatch"
        if product_line_state in {"unknown_both", "unknown_partial"}:
            return "medium", "product_line_unknown"
        return "medium", "paper_towel_close_roll_count"

    return None


def _best_cross_retailer_pair(entries: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any], str, str | None] | None:
    a101_entries = [entry for entry in entries if entry["retailer"] == "a101"]
    migros_entries = [entry for entry in entries if entry["retailer"] == "migros"]
    ranked_pairs = []

    for a101_entry, migros_entry in product(a101_entries, migros_entries):
        pair_state = _paper_towel_pair_state(a101_entry, migros_entry)
        if pair_state is None:
            continue

        confidence, review_reason = pair_state
        relative_difference = _relative_roll_difference(a101_entry, migros_entry) or 0.0
        ranked_pairs.append(
            (
                0 if confidence == "high" else 1,
                0 if a101_entry.get("brand_token") == migros_entry.get("brand_token") else 1,
                0 if (a101_entry.get("product_line_tokens") or ()) == (migros_entry.get("product_line_tokens") or ()) and a101_entry.get("product_line_tokens") else 1,
                0 if not a101_entry.get("is_special_format") and not migros_entry.get("is_special_format") else 1,
                relative_difference,
                _entry_rank(a101_entry),
                _entry_rank(migros_entry),
                a101_entry,
                migros_entry,
                confidence,
                review_reason,
            )
        )

    if not ranked_pairs:
        return None

    ranked_pairs.sort()
    best_pair = ranked_pairs[0]
    return best_pair[7], best_pair[8], best_pair[9], best_pair[10]


def augment_catalog_with_paper_towel_rows(catalog_df: pd.DataFrame) -> pd.DataFrame:
    paper_towel_entries, remaining_catalog_df = _paper_towel_entries_from_catalog(catalog_df)
    if not paper_towel_entries:
        return catalog_df.copy()

    exact_groups: dict[str, list[dict[str, Any]]] = {}
    brand_entries: dict[str, list[dict[str, Any]]] = {}
    generic_count_groups: dict[float, dict[str, list[dict[str, Any]]]] = {}

    for entry in paper_towel_entries:
        brand_token = entry.get("brand_token")
        if not brand_token:
            continue

        brand_entries.setdefault(brand_token, []).append(entry)

        if entry.get("roll_count") is not None:
            generic_count_groups.setdefault(float(entry["roll_count"]), {}).setdefault(entry["retailer"], []).append(entry)

        exact_group_key = _exact_group_key(entry)
        if exact_group_key is not None:
            exact_groups.setdefault(exact_group_key, []).append(entry)

    synthesized_rows: list[dict[str, Any]] = []
    used_standardized_names: set[str] = set()

    for standardized_product_name, entries in sorted(exact_groups.items()):
        best_pair = _best_cross_retailer_pair(entries)
        a101_entries = [entry for entry in entries if entry["retailer"] == "a101"]
        migros_entries = [entry for entry in entries if entry["retailer"] == "migros"]
        a101_entry = min(a101_entries, key=_entry_rank) if a101_entries else None
        migros_entry = min(migros_entries, key=_entry_rank) if migros_entries else None
        pair_state = None
        if best_pair is not None:
            a101_entry, migros_entry, comparison_confidence, comparison_review_reason = best_pair
            pair_state = (comparison_confidence, comparison_review_reason)

        if pair_state is None:
            coverage_status = (
                "only_a101" if a101_entry and not migros_entry
                else "only_migros" if migros_entry and not a101_entry
                else COMPARISON_REVIEW_STATUS
            )
            comparison_confidence = "single_source" if coverage_status.startswith("only_") else "medium"
            comparison_review_reason = None if coverage_status.startswith("only_") else "paper_towel_review_required"
        else:
            comparison_confidence, comparison_review_reason = pair_state
            coverage_status = (
                COMPARABLE_STATUS
                if comparison_confidence == "high"
                else COMPARISON_REVIEW_STATUS
            )

        synthesized_rows.append(
            _build_catalog_row(
                standardized_product_name,
                a101_entry,
                migros_entry,
                coverage_status,
                comparison_confidence,
                comparison_review_reason,
            )
        )
        used_standardized_names.add(standardized_product_name)

    for brand_token, entries in sorted(brand_entries.items()):
        best_pair = _best_cross_retailer_pair(entries)
        if best_pair is None:
            continue

        a101_entry, migros_entry, comparison_confidence, comparison_review_reason = best_pair

        standardized_product_name = _paper_towel_name(brand_token)
        if standardized_product_name in used_standardized_names:
            continue

        synthesized_rows.append(
            _build_catalog_row(
                standardized_product_name,
                a101_entry,
                migros_entry,
                COMPARISON_REVIEW_STATUS,
                comparison_confidence,
                comparison_review_reason,
            )
        )
        used_standardized_names.add(standardized_product_name)

    for roll_count, retailer_entries in sorted(generic_count_groups.items()):
        a101_entries = retailer_entries.get("a101") or []
        migros_entries = retailer_entries.get("migros") or []
        if not a101_entries or not migros_entries:
            continue

        a101_entry = min(a101_entries, key=_entry_rank)
        migros_entry = min(migros_entries, key=_entry_rank)
        if a101_entry.get("brand_token") == migros_entry.get("brand_token"):
            continue

        standardized_product_name = _generic_paper_towel_name(roll_count)
        if standardized_product_name in used_standardized_names:
            continue

        synthesized_rows.append(
            _build_catalog_row(
                standardized_product_name,
                a101_entry,
                migros_entry,
                COMPARISON_REVIEW_STATUS,
                "low",
                "brand_mismatch",
            )
        )
        used_standardized_names.add(standardized_product_name)

    synthesized_df = pd.DataFrame(synthesized_rows)
    if synthesized_df.empty:
        return catalog_df.copy()

    combined_catalog_df = pd.concat(
        [remaining_catalog_df, synthesized_df],
        ignore_index=True,
        sort=False,
    )
    return combined_catalog_df.sort_values(
        by=["standardized_product_name", "coverage_status"],
        kind="stable",
    ).reset_index(drop=True)


def synthesize_paper_towel_price_rows(latest_price_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    paper_towel_entries = _paper_towel_entries_from_price_rows(latest_price_rows)
    if not paper_towel_entries:
        return []

    exact_groups: dict[str, list[dict[str, Any]]] = {}
    brand_entries: dict[str, list[dict[str, Any]]] = {}
    generic_count_groups: dict[float, dict[str, list[dict[str, Any]]]] = {}

    for entry in paper_towel_entries:
        brand_token = entry.get("brand_token")
        if not brand_token:
            continue

        brand_entries.setdefault(brand_token, []).append(entry)

        if entry.get("roll_count") is not None:
            generic_count_groups.setdefault(float(entry["roll_count"]), {}).setdefault(entry["retailer"], []).append(entry)

        exact_group_key = _exact_group_key(entry)
        if exact_group_key is not None:
            exact_groups.setdefault(exact_group_key, []).append(entry)

    synthesized_rows: list[dict[str, Any]] = []
    used_standardized_names: set[str] = set()

    for standardized_product_name, entries in sorted(exact_groups.items()):
        best_pair = _best_cross_retailer_pair(entries)
        a101_entries = [entry for entry in entries if entry["retailer"] == "a101"]
        migros_entries = [entry for entry in entries if entry["retailer"] == "migros"]
        a101_entry = min(a101_entries, key=_entry_rank) if a101_entries else None
        migros_entry = min(migros_entries, key=_entry_rank) if migros_entries else None

        if best_pair is not None:
            a101_entry, migros_entry, comparison_confidence, comparison_review_reason = best_pair
        else:
            comparison_confidence = "single_source"
            comparison_review_reason = None

        synthesized_rows.append(
            _build_price_row(
                standardized_product_name,
                a101_entry,
                migros_entry,
                comparison_confidence,
                comparison_review_reason,
            )
        )
        used_standardized_names.add(standardized_product_name)

    for brand_token, entries in sorted(brand_entries.items()):
        best_pair = _best_cross_retailer_pair(entries)
        if best_pair is None:
            continue

        a101_entry, migros_entry, comparison_confidence, comparison_review_reason = best_pair
        if comparison_confidence == "high":
            continue

        standardized_product_name = _paper_towel_name(brand_token)
        if standardized_product_name in used_standardized_names:
            continue

        synthesized_rows.append(
            _build_price_row(
                standardized_product_name,
                a101_entry,
                migros_entry,
                comparison_confidence,
                comparison_review_reason,
            )
        )
        used_standardized_names.add(standardized_product_name)

    for roll_count, retailer_entries in sorted(generic_count_groups.items()):
        a101_entries = retailer_entries.get("a101") or []
        migros_entries = retailer_entries.get("migros") or []
        if not a101_entries or not migros_entries:
            continue

        a101_entry = min(a101_entries, key=_entry_rank)
        migros_entry = min(migros_entries, key=_entry_rank)
        if a101_entry.get("brand_token") == migros_entry.get("brand_token"):
            continue

        standardized_product_name = _generic_paper_towel_name(roll_count)
        if standardized_product_name in used_standardized_names:
            continue

        synthesized_rows.append(
            _build_price_row(
                standardized_product_name,
                a101_entry,
                migros_entry,
                "low",
                "brand_mismatch",
            )
        )
        used_standardized_names.add(standardized_product_name)

    return synthesized_rows
