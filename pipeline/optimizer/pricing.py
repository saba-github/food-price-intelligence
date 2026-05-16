from decimal import Decimal

from pipeline.optimizer.cleaning_products import (
    analyze_cleaning_pair,
    infer_cleaning_product_profile,
    synthesize_cleaning_price_rows,
)
from pipeline.optimizer.measurement import add_measurement_labels
from pipeline.optimizer.paper_products import synthesize_paper_towel_price_rows
from pipeline.transforms import infer_paper_product_profile, normalize_text


RETAILER_PRICE_COLUMNS = {
    "a101": "a101_price",
    "migros": "migros_price",
}
SAFE_COMPARISON_CONFIDENCE = "high"
COMPARABLE_STATUS = "comparable"
COMPARISON_REVIEW_STATUS = "comparison_review_required"
MEASUREMENT_FIELDS = [
    "a101_source_product_name",
    "migros_source_product_name",
    "a101_normalized_unit",
    "migros_normalized_unit",
    "a101_normalized_quantity",
    "migros_normalized_quantity",
    "a101_measurement_label",
    "migros_measurement_label",
]
SELECTION_FIELDS = [
    "selection_type",
    "family_id",
    "family_label",
    "family_options",
    "family_option_count",
    "selected_family_product_name",
    "force_review",
]
TOILET_PAPER_PRODUCT_LINE_ALIASES = {
    "platinum": "platinum",
    "egzotik": "egzotik",
    "bamboo": "bambu",
    "bambu": "bambu",
    "deluxe": "deluxe",
    "natural": "natural",
    "soft": "soft",
}


def _measurement_fields(price_row: dict) -> dict:
    return {
        field_name: price_row.get(field_name)
        for field_name in MEASUREMENT_FIELDS
    }


def _selection_fields(price_row: dict) -> dict:
    return {
        field_name: price_row.get(field_name)
        for field_name in SELECTION_FIELDS
        if field_name in price_row
    }


def _toilet_paper_tokens(source_product_name: str | None) -> set[str]:
    if not source_product_name:
        return set()
    return {
        token
        for token in normalize_text(source_product_name).replace("-", " ").split()
        if token
    }


def _is_toilet_paper_name(source_product_name: str | None) -> bool:
    tokens = _toilet_paper_tokens(source_product_name)
    return "tuvalet" in tokens and "kagidi" in tokens


def _extract_brand_token(source_product_name: str | None) -> str | None:
    if not source_product_name:
        return None
    tokens = [token for token in normalize_text(source_product_name).replace("-", " ").split() if token]
    if not tokens:
        return None
    return tokens[0]


def _extract_product_line_token(source_product_name: str | None) -> str | None:
    tokens = _toilet_paper_tokens(source_product_name)
    for token, canonical in TOILET_PAPER_PRODUCT_LINE_ALIASES.items():
        if token in tokens:
            return canonical
    return None


def _apply_paper_towel_comparison_rules(price_row: dict) -> dict:
    a101_name = price_row.get("a101_source_product_name")
    migros_name = price_row.get("migros_source_product_name")
    a101_profile = infer_paper_product_profile(a101_name)
    migros_profile = infer_paper_product_profile(migros_name)

    if not (
        a101_profile.get("kind") == "paper_towel"
        and migros_profile.get("kind") == "paper_towel"
    ):
        return price_row

    if not (
        price_row.get("a101_price") is not None
        and price_row.get("migros_price") is not None
        and price_row.get("same_unit_flag") is True
        and price_row.get("same_quantity_flag") is True
    ):
        return price_row

    a101_brand = a101_profile.get("brand_token")
    migros_brand = migros_profile.get("brand_token")
    a101_line_tokens = tuple(a101_profile.get("product_line_tokens") or ())
    migros_line_tokens = tuple(migros_profile.get("product_line_tokens") or ())

    updated_row = dict(price_row)

    if not a101_brand or not migros_brand or a101_brand != migros_brand:
        updated_row["comparison_confidence"] = "low"
        updated_row["comparison_review_reason"] = "brand_mismatch"
        updated_row["coverage_status"] = COMPARISON_REVIEW_STATUS
        return updated_row

    if a101_line_tokens and migros_line_tokens:
        if a101_line_tokens == migros_line_tokens:
            updated_row["comparison_confidence"] = SAFE_COMPARISON_CONFIDENCE
            updated_row["comparison_review_reason"] = None
            updated_row["coverage_status"] = COMPARABLE_STATUS
            return updated_row

        updated_row["comparison_confidence"] = "medium"
        updated_row["comparison_review_reason"] = "product_line_mismatch"
        updated_row["coverage_status"] = COMPARISON_REVIEW_STATUS
        return updated_row

    updated_row["comparison_confidence"] = "medium"
    updated_row["comparison_review_reason"] = "product_line_unknown"
    updated_row["coverage_status"] = COMPARISON_REVIEW_STATUS
    return updated_row


def _apply_toilet_paper_comparison_rules(price_row: dict) -> dict:
    updated_paper_towel_row = _apply_paper_towel_comparison_rules(price_row)
    if updated_paper_towel_row is not price_row:
        return updated_paper_towel_row

    a101_name = price_row.get("a101_source_product_name")
    migros_name = price_row.get("migros_source_product_name")

    if not (_is_toilet_paper_name(a101_name) and _is_toilet_paper_name(migros_name)):
        return price_row

    if not (
        price_row.get("a101_price") is not None
        and price_row.get("migros_price") is not None
        and price_row.get("same_unit_flag") is True
        and price_row.get("same_quantity_flag") is True
    ):
        return price_row

    a101_brand = _extract_brand_token(a101_name)
    migros_brand = _extract_brand_token(migros_name)
    a101_line = _extract_product_line_token(a101_name)
    migros_line = _extract_product_line_token(migros_name)

    updated_row = dict(price_row)

    if not a101_brand or not migros_brand or a101_brand != migros_brand:
        updated_row["comparison_confidence"] = "low"
        updated_row["comparison_review_reason"] = "brand_mismatch"
        updated_row["coverage_status"] = COMPARISON_REVIEW_STATUS
        return updated_row

    if a101_line and migros_line:
        if a101_line == migros_line:
            updated_row["comparison_confidence"] = SAFE_COMPARISON_CONFIDENCE
            updated_row["comparison_review_reason"] = None
            updated_row["coverage_status"] = COMPARABLE_STATUS
            return updated_row

        updated_row["comparison_confidence"] = "medium"
        updated_row["comparison_review_reason"] = "product_line_mismatch"
        updated_row["coverage_status"] = COMPARISON_REVIEW_STATUS
        return updated_row

    updated_row["comparison_confidence"] = "medium"
    updated_row["comparison_review_reason"] = "product_line_unknown"
    updated_row["coverage_status"] = COMPARISON_REVIEW_STATUS
    return updated_row


def _apply_cleaning_comparison_rules(price_row: dict) -> dict:
    if not (
        price_row.get("a101_price") is not None
        and price_row.get("migros_price") is not None
    ):
        return price_row

    pair_info = analyze_cleaning_pair(
        price_row.get("a101_source_product_name"),
        price_row.get("migros_source_product_name"),
        price_row.get("a101_normalized_unit"),
        price_row.get("migros_normalized_unit"),
        price_row.get("a101_normalized_quantity"),
        price_row.get("migros_normalized_quantity"),
    )
    if not pair_info.get("is_cleaning_pair"):
        return price_row

    updated_row = dict(price_row)
    if pair_info.get("soft_equivalent"):
        updated_row["soft_equivalent_match"] = True
        updated_row["comparison_confidence"] = SAFE_COMPARISON_CONFIDENCE
        updated_row["comparison_review_reason"] = None
        updated_row["coverage_status"] = COMPARABLE_STATUS
        return updated_row

    if not (
        price_row.get("same_unit_flag") is True
        and price_row.get("same_quantity_flag") is True
    ):
        return price_row

    if not pair_info.get("same_brand"):
        updated_row["comparison_confidence"] = "low"
        updated_row["comparison_review_reason"] = "brand_mismatch"
        updated_row["coverage_status"] = COMPARISON_REVIEW_STATUS
        return updated_row

    if not pair_info.get("same_subtype"):
        updated_row["comparison_confidence"] = "medium"
        if pair_info.get("a101_subtype") and pair_info.get("migros_subtype"):
            updated_row["comparison_review_reason"] = "subtype_mismatch"
        else:
            updated_row["comparison_review_reason"] = "subtype_unknown"
        updated_row["coverage_status"] = COMPARISON_REVIEW_STATUS
        return updated_row

    if (
        pair_info.get("a101_variant_token")
        and pair_info.get("migros_variant_token")
        and not pair_info.get("same_variant")
    ):
        updated_row["comparison_confidence"] = "medium"
        updated_row["comparison_review_reason"] = "variant_mismatch"
        updated_row["coverage_status"] = COMPARISON_REVIEW_STATUS
        return updated_row

    if (
        pair_info.get("a101_package_format")
        and pair_info.get("migros_package_format")
        and not pair_info.get("same_package_format")
    ):
        updated_row["comparison_confidence"] = "medium"
        updated_row["comparison_review_reason"] = "package_format_mismatch"
        updated_row["coverage_status"] = COMPARISON_REVIEW_STATUS
        return updated_row

    updated_row["comparison_confidence"] = SAFE_COMPARISON_CONFIDENCE
    updated_row["comparison_review_reason"] = None
    updated_row["coverage_status"] = COMPARABLE_STATUS
    return updated_row


def get_selected_price(price_info: dict) -> int | float | None:
    selected_price = price_info.get("price_per_unit")

    if selected_price is None:
        selected_price = price_info.get("price")

    return selected_price


def _needs_paper_towel_supplement(standardized_products: list[str]) -> bool:
    return any(
        infer_paper_product_profile(product_name).get("kind") == "paper_towel"
        for product_name in standardized_products
    )


def _cleaning_supplement_brands(standardized_products: list[str]) -> set[str]:
    return {
        str(profile.get("brand_token"))
        for product_name in standardized_products
        for profile in [infer_cleaning_product_profile(product_name)]
        if profile.get("is_cleaning") and profile.get("brand_token")
    }


def _needs_cleaning_supplement(standardized_products: list[str]) -> bool:
    return bool(_cleaning_supplement_brands(standardized_products))


def _build_single_source_price_row(
    source_name: str,
    source_product_name: str,
    price,
    normalized_unit,
    normalized_quantity,
) -> dict:
    return {
        "standardized_product_name": normalize_text(source_product_name),
        "canonical_name": normalize_text(source_product_name),
        "a101_price": price if source_name == "a101" else None,
        "migros_price": price if source_name == "migros" else None,
        "cheaper_source": source_name,
        "compared_at": None,
        "same_unit_flag": False,
        "same_quantity_flag": False,
        "comparison_confidence": "single_source",
        "a101_source_product_name": source_product_name if source_name == "a101" else None,
        "migros_source_product_name": source_product_name if source_name == "migros" else None,
        "a101_normalized_unit": normalized_unit if source_name == "a101" else None,
        "migros_normalized_unit": normalized_unit if source_name == "migros" else None,
        "a101_normalized_quantity": normalized_quantity if source_name == "a101" else None,
        "migros_normalized_quantity": normalized_quantity if source_name == "migros" else None,
    }


def _get_cleaning_source_rows(cursor, brand_tokens: set[str]) -> list[dict]:
    if not brand_tokens:
        return []

    like_patterns = [f"%{brand_token}%" for brand_token in sorted(brand_tokens)]
    cursor.execute(
        """
        WITH latest_source AS (
            SELECT
                source_name,
                source_product_name,
                price,
                normalized_unit,
                normalized_quantity,
                ROW_NUMBER() OVER (
                    PARTITION BY source_name, source_product_name
                    ORDER BY observed_at DESC, price_observation_id DESC
                ) AS source_rn
            FROM price_history
            WHERE price IS NOT NULL
              AND LOWER(source_product_name) LIKE ANY(%s)
        )
        SELECT
            source_name,
            source_product_name,
            price,
            normalized_unit,
            normalized_quantity
        FROM latest_source
        WHERE source_rn = 1
        """,
        (like_patterns,),
    )

    rows = cursor.fetchall()
    return [
        _build_single_source_price_row(
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
        )
        for row in rows
    ]


def _get_paper_towel_source_rows(cursor) -> list[dict]:
    cursor.execute(
        """
        WITH latest_source AS (
            SELECT
                source_name,
                source_product_name,
                price,
                normalized_unit,
                normalized_quantity,
                ROW_NUMBER() OVER (
                    PARTITION BY source_name, source_product_name
                    ORDER BY observed_at DESC, price_observation_id DESC
                ) AS source_rn
            FROM price_history
            WHERE price IS NOT NULL
              AND LOWER(source_product_name) LIKE '%%havlu%%'
              AND LOWER(source_product_name) NOT LIKE '%%tuvalet%%'
              AND LOWER(source_product_name) NOT LIKE '%%mendil%%'
              AND LOWER(source_product_name) NOT LIKE '%%pecete%%'
              AND LOWER(source_product_name) NOT LIKE '%%islak%%'
        )
        SELECT
            source_name,
            source_product_name,
            price,
            normalized_unit,
            normalized_quantity
        FROM latest_source
        WHERE source_rn = 1
        """,
        (),
    )

    rows = cursor.fetchall()
    return [
        _build_single_source_price_row(
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
        )
        for row in rows
    ]


def get_cross_compare_prices(cursor, standardized_products: list[str]) -> list[dict]:
    if not standardized_products:
        return []

    cursor.execute(
        """
        SELECT
            m.standardized_product_name,
            m.canonical_name,
            m.a101_price,
            m.migros_price,
            m.cheaper_source,
            m.compared_at,
            m.same_unit_flag,
            m.same_quantity_flag,
            m.comparison_confidence,
            m.a101_source_product_name,
            m.migros_source_product_name,
            m.a101_normalized_unit,
            m.migros_normalized_unit,
            m.a101_normalized_quantity,
            m.migros_normalized_quantity
        FROM mart_cross_compare m
        WHERE m.standardized_product_name = ANY(%s)
        """,
        (standardized_products,),
    )

    rows = cursor.fetchall()
    latest_rows = [
        add_measurement_labels(
            _apply_cleaning_comparison_rules(
                _apply_toilet_paper_comparison_rules(
                {
                    "standardized_product_name": row[0],
                    "canonical_name": row[1],
                    "a101_price": row[2],
                    "migros_price": row[3],
                    "cheaper_source": row[4],
                    "compared_at": row[5],
                    "same_unit_flag": row[6],
                    "same_quantity_flag": row[7],
                    "comparison_confidence": row[8],
                    "a101_source_product_name": row[9],
                    "migros_source_product_name": row[10],
                    "a101_normalized_unit": row[11],
                    "migros_normalized_unit": row[12],
                    "a101_normalized_quantity": row[13],
                    "migros_normalized_quantity": row[14],
                }
                )
            )
        )
        for row in rows
    ]
    return latest_rows


def get_latest_price_history_prices(cursor, standardized_products: list[str]) -> list[dict]:
    if not standardized_products:
        return []

    cursor.execute(
        """
        WITH latest_source_base AS (
            SELECT
                standardized_product_name,
                source_name,
                source_product_name,
                brand_name,
                price,
                CASE
                    WHEN standardized_product_name ILIKE '%%uno%%'
                     AND standardized_product_name ILIKE '%%bugday%%'
                     AND standardized_product_name ILIKE '%%tava%%'
                     AND standardized_product_name ILIKE '%%ekmeg%%'
                     AND normalized_unit = 'kg'
                     AND normalized_quantity = 0.45
                    THEN 'tava ekmek'
                    WHEN (
                        (
                            standardized_product_name = 'su'
                            OR standardized_product_name LIKE 'su %%'
                            OR standardized_product_name LIKE '%% su'
                            OR standardized_product_name LIKE '%% suyu%%'
                            OR standardized_product_name LIKE '%% kaynak suyu%%'
                        )
                        AND standardized_product_name NOT ILIKE '%%sut%%'
                        AND standardized_product_name NOT ILIKE '%%yogurt%%'
                        AND standardized_product_name NOT ILIKE '%%susam%%'
                        AND standardized_product_name NOT ILIKE '%%sos%%'
                        AND standardized_product_name NOT ILIKE '%%maden%%'
                        AND standardized_product_name NOT ILIKE '%%mineral%%'
                        AND standardized_product_name NOT ILIKE '%%soda%%'
                        AND standardized_product_name NOT ILIKE '%%gazli%%'
                        AND standardized_product_name NOT ILIKE '%%aromali%%'
                        AND NOT (
                            standardized_product_name ILIKE '%%camasir%%'
                            AND standardized_product_name ILIKE '%%suyu%%'
                        )
                        AND standardized_product_name NOT ILIKE '%%12x%%'
                        AND standardized_product_name NOT ILIKE '%%6x%%'
                        AND standardized_product_name NOT ILIKE '%%4x%%'
                    )
                    THEN 'su'
                    WHEN (
                        standardized_product_name LIKE '%%tuz%%'
                        AND standardized_product_name NOT ILIKE '%%tuzlu%%'
                        AND standardized_product_name NOT ILIKE '%%zeytin%%'
                        AND standardized_product_name NOT ILIKE '%%tereyagi%%'
                        AND standardized_product_name NOT ILIKE '%%limon tuzu%%'
                        AND standardized_product_name NOT ILIKE '%%himalaya%%'
                        AND standardized_product_name NOT ILIKE '%%kaya%%'
                        AND standardized_product_name NOT ILIKE '%%salamura%%'
                        AND standardized_product_name NOT ILIKE '%%sarimsakli%%'
                        AND standardized_product_name NOT ILIKE '%%truf%%'
                        AND standardized_product_name NOT ILIKE '%%mantarli%%'
                    )
                    THEN 'tuz'
                    WHEN (
                        standardized_product_name LIKE '%%kola pepsi%%'
                        AND standardized_product_name NOT ILIKE '%%4x%%'
                        AND standardized_product_name NOT ILIKE '%%6x%%'
                        AND standardized_product_name NOT ILIKE '%%kutu%%'
                        AND standardized_product_name NOT ILIKE '%%zero%%'
                        AND standardized_product_name NOT ILIKE '%%sekersiz%%'
                        AND standardized_product_name NOT ILIKE '%%light%%'
                        AND standardized_product_name NOT ILIKE '%%diet%%'
                        AND standardized_product_name NOT ILIKE '%%sugar%%'
                        AND standardized_product_name NOT ILIKE '%%free%%'
                        AND standardized_product_name NOT ILIKE '%%lime%%'
                        AND standardized_product_name NOT ILIKE '%%lemon%%'
                        AND standardized_product_name NOT ILIKE '%%vanilla%%'
                        AND standardized_product_name NOT ILIKE '%%cherry%%'
                    )
                    THEN 'kola pepsi'
                    WHEN (
                        (
                            standardized_product_name LIKE '%%coca-cola%%'
                            OR standardized_product_name LIKE '%%coca cola%%'
                        )
                        AND standardized_product_name NOT ILIKE '%%zero%%'
                        AND standardized_product_name NOT ILIKE '%%sekersiz%%'
                        AND standardized_product_name NOT ILIKE '%%light%%'
                        AND standardized_product_name NOT ILIKE '%%diet%%'
                        AND standardized_product_name NOT ILIKE '%%sugar%%'
                        AND standardized_product_name NOT ILIKE '%%free%%'
                        AND standardized_product_name NOT ILIKE '%%lime%%'
                        AND standardized_product_name NOT ILIKE '%%lemon%%'
                        AND standardized_product_name NOT ILIKE '%%vanilla%%'
                        AND standardized_product_name NOT ILIKE '%%cherry%%'
                    )
                    THEN 'kola coca-cola'
                    WHEN (
                        standardized_product_name ILIKE '%%aycicek%%'
                        AND standardized_product_name ILIKE '%%yag%%'
                        AND standardized_product_name NOT ILIKE '%%zeytin%%'
                    )
                    THEN 'aycicek yagi'
                    WHEN (
                        (
                            standardized_product_name ILIKE '%%zeytinyagi%%'
                            OR (
                                standardized_product_name ILIKE '%%zeytin%%'
                                AND standardized_product_name ILIKE '%%yag%%'
                            )
                        )
                        AND standardized_product_name NOT ILIKE '%%aycicek%%'
                    )
                    THEN 'zeytinyagi'
                    WHEN (
                        (
                            standardized_product_name ILIKE '%%misirozu%%'
                            OR (
                                standardized_product_name ILIKE '%%misir%%'
                                AND standardized_product_name ILIKE '%%yag%%'
                            )
                        )
                        AND standardized_product_name NOT ILIKE '%%zeytin%%'
                    )
                    THEN 'misir yagi'
                    WHEN (
                        standardized_product_name ILIKE '%%findik%%'
                        AND standardized_product_name ILIKE '%%yag%%'
                    )
                    THEN 'findik yagi'
                    WHEN (
                        (
                            standardized_product_name ILIKE '%%bulasik%%'
                            AND (
                                standardized_product_name ILIKE '%%deterjan%%'
                                OR standardized_product_name ILIKE '%%deterjani%%'
                                OR standardized_product_name ILIKE '%%elde%%'
                                OR standardized_product_name ILIKE '%%yikama%%'
                                OR standardized_product_name ILIKE '%%sivi%%'
                            )
                            AND standardized_product_name NOT ILIKE '%%makine%%'
                            AND standardized_product_name NOT ILIKE '%%makinesi%%'
                            AND standardized_product_name NOT ILIKE '%%tablet%%'
                            AND standardized_product_name NOT ILIKE '%%tableti%%'
                            AND standardized_product_name NOT ILIKE '%%kapsul%%'
                            AND standardized_product_name NOT ILIKE '%%parlatici%%'
                            AND standardized_product_name NOT ILIKE '%%temizleyici%%'
                            AND standardized_product_name NOT ILIKE '%%tuz%%'
                            AND standardized_product_name NOT ILIKE '%%sprey%%'
                        )
                        OR (
                            standardized_product_name ILIKE '%%fairy%%'
                            AND (
                                standardized_product_name ILIKE '%%elma%%'
                                OR standardized_product_name ILIKE '%%limon%%'
                                OR standardized_product_name ILIKE '%%aloe%%'
                                OR standardized_product_name ILIKE '%%portakal%%'
                                OR standardized_product_name ILIKE '%%sensitive%%'
                                OR standardized_product_name ILIKE '%%elde%%'
                                OR standardized_product_name ILIKE '%%yikama%%'
                            )
                            AND standardized_product_name NOT ILIKE '%%tablet%%'
                            AND standardized_product_name NOT ILIKE '%%tableti%%'
                            AND standardized_product_name NOT ILIKE '%%kapsul%%'
                            AND standardized_product_name NOT ILIKE '%%parlatici%%'
                            AND standardized_product_name NOT ILIKE '%%temizleyici%%'
                            AND standardized_product_name NOT ILIKE '%%tuz%%'
                            AND standardized_product_name NOT ILIKE '%%sprey%%'
                        )
                    )
                    THEN CONCAT_WS(
                        ' ',
                        CASE
                            WHEN standardized_product_name ILIKE '%%fairy%%' THEN 'fairy'
                            WHEN standardized_product_name ILIKE '%%pril%%' THEN 'pril'
                            WHEN standardized_product_name ILIKE '%%bingo%%' THEN 'bingo'
                            WHEN standardized_product_name ILIKE '%%asperox%%' THEN 'asperox'
                            ELSE NULLIF(TRIM(LOWER(SPLIT_PART(standardized_product_name, ' ', 1))), '')
                        END,
                        CASE
                            WHEN standardized_product_name ILIKE '%%elma%%' THEN 'elma'
                            WHEN standardized_product_name ILIKE '%%limon%%' THEN 'limon'
                            WHEN standardized_product_name ILIKE '%%aloe%%' THEN 'aloe'
                            WHEN standardized_product_name ILIKE '%%portakal%%' THEN 'portakal'
                            WHEN standardized_product_name ILIKE '%%sensitive%%' THEN 'sensitive'
                            WHEN standardized_product_name ILIKE '%%platinum%%' THEN 'platinum'
                            ELSE NULL
                        END,
                        'bulasik deterjani'
                    )
                    WHEN (
                        standardized_product_name ILIKE '%%tuvalet%%'
                        AND standardized_product_name ILIKE '%%kagid%%'
                        AND standardized_product_name NOT ILIKE '%%islak%%'
                        AND standardized_product_name NOT ILIKE '%%mendil%%'
                        AND standardized_product_name NOT ILIKE '%%havlu%%'
                        AND standardized_product_name NOT ILIKE '%%pecete%%'
                    )
                    THEN COALESCE(NULLIF(TRIM(LOWER(SPLIT_PART(source_product_name, ' ', 1))), '') || ' tuvalet kagidi', 'tuvalet kagidi')
                    WHEN (
                        (
                            (
                                standardized_product_name ILIKE '%%kagit%%'
                                AND standardized_product_name ILIKE '%%havlu%%'
                            )
                            OR (
                                standardized_product_name ILIKE '%%havlu%%'
                                AND standardized_product_name ILIKE '%%kagidi%%'
                            )
                        )
                        AND standardized_product_name NOT ILIKE '%%tuvalet%%'
                        AND standardized_product_name NOT ILIKE '%%islak%%'
                        AND standardized_product_name NOT ILIKE '%%mendil%%'
                        AND standardized_product_name NOT ILIKE '%%pecete%%'
                        AND standardized_product_name NOT ILIKE '%%el havlu%%'
                        AND standardized_product_name NOT ILIKE '%%yuz havlu%%'
                    )
                    THEN COALESCE(NULLIF(TRIM(LOWER(SPLIT_PART(source_product_name, ' ', 1))), '') || ' kagit havlu', 'kagit havlu')
                    WHEN (
                        standardized_product_name ILIKE '%%arpacik%%'
                        AND standardized_product_name ILIKE '%%sogan%%'
                    )
                    THEN 'arpacik sogan'
                    WHEN (
                        standardized_product_name ILIKE '%%sogan%%'
                        AND standardized_product_name NOT ILIKE '%%soganli%%'
                        AND (
                            source_product_name ILIKE '%%taze%%'
                            OR source_product_name ILIKE '%%demet%%'
                            OR source_product_name ILIKE '%%frenk%%'
                        )
                    )
                    THEN 'sogan taze'
                    WHEN (
                        standardized_product_name ILIKE '%%sogan%%'
                        AND standardized_product_name NOT ILIKE '%%soganli%%'
                        AND standardized_product_name NOT ILIKE '%%arpacik%%'
                    )
                    THEN 'sogan'
                    WHEN (
                        standardized_product_name ILIKE '%%bildircin%%'
                        AND standardized_product_name ILIKE '%%yumurta%%'
                    )
                    THEN 'bildircin yumurta'
                    WHEN standardized_product_name ILIKE '%%yumurta%%' THEN 'yumurta'
                    WHEN standardized_product_name IN ('ekmek', 'ekmek sofra') THEN 'ekmek'
                    WHEN standardized_product_name IN ('salatalik', 'hiyar') THEN 'salatalik'
                    WHEN standardized_product_name ILIKE '%%shiitake%%' THEN 'shiitake mantar'
                    WHEN standardized_product_name ILIKE '%%istiridye%%'
                      OR standardized_product_name ILIKE '%%istridye%%' THEN 'istiridye mantar'
                    WHEN standardized_product_name ILIKE '%%kestane%%' THEN 'kestane mantar'
                    WHEN standardized_product_name ILIKE '%%izgaralik%%' THEN 'izgaralik mantar'
                    WHEN standardized_product_name ILIKE '%%mantar%%' THEN 'mantar'
                    ELSE COALESCE(
                        (
                            SELECT STRING_AGG(token, ' ' ORDER BY ordinality)
                            FROM UNNEST(
                                REGEXP_SPLIT_TO_ARRAY(standardized_product_name, E'[[:space:]]+')
                            ) WITH ORDINALITY AS parts(token, ordinality)
                            WHERE token <> ''
                              AND token !~ E'^[0-9]+(?:[.,][0-9]+)?$'
                              AND token NOT IN (
                                  'kg', 'g', 'gram', 'ml', 'l', 'lt',
                                  'adet', 'demet', 'paket',
                                  'dokme', 'kuru', 'taze', 'yerli', 'ithal', 'salkim'
                              )
                        ),
                        standardized_product_name
                    )
                END AS canonical_search_name,
                CASE
                    WHEN standardized_product_name ILIKE '%%yumurta%%'
                     AND (
                         SUBSTRING(LOWER(standardized_product_name) FROM '([1-9][0-9]?)\s*[''''’]') IS NOT NULL
                         OR SUBSTRING(LOWER(standardized_product_name) FROM '([1-9][0-9]?)\s*adet') IS NOT NULL
                     ) THEN 'piece'
                    WHEN SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *kg') IS NOT NULL THEN 'kg'
                    WHEN SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *g(ram)?') IS NOT NULL THEN 'kg'
                    WHEN SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *ml') IS NOT NULL THEN 'liter'
                    WHEN SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *l') IS NOT NULL THEN 'liter'
                    ELSE normalized_unit
                END AS comparison_unit,
                CASE
                    WHEN standardized_product_name ILIKE '%%yumurta%%'
                     AND (
                         SUBSTRING(LOWER(standardized_product_name) FROM '([1-9][0-9]?)\s*[''''’]') IS NOT NULL
                         OR SUBSTRING(LOWER(standardized_product_name) FROM '([1-9][0-9]?)\s*adet') IS NOT NULL
                     ) THEN COALESCE(
                        REPLACE(
                            SUBSTRING(
                                LOWER(standardized_product_name)
                                FROM '([1-9][0-9]?)\s*[''''’]'
                            ),
                            ',',
                            '.'
                        )::numeric,
                        REPLACE(
                            SUBSTRING(
                                LOWER(standardized_product_name)
                                FROM '([1-9][0-9]?)\s*adet'
                            ),
                            ',',
                            '.'
                        )::numeric
                    )
                    WHEN SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *kg') IS NOT NULL THEN
                        REPLACE(
                            SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *kg'),
                            ',',
                            '.'
                        )::numeric
                    WHEN SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *g(ram)?') IS NOT NULL THEN
                        REPLACE(
                            SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *g(ram)?'),
                            ',',
                            '.'
                        )::numeric / 1000
                    WHEN SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *ml') IS NOT NULL THEN
                        REPLACE(
                            SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *ml'),
                            ',',
                            '.'
                        )::numeric / 1000
                    WHEN SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *l') IS NOT NULL THEN
                        REPLACE(
                            SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *l'),
                            ',',
                            '.'
                        )::numeric
                    ELSE normalized_quantity
                END AS comparison_quantity,
                observed_at,
                price_observation_id,
                ROW_NUMBER() OVER (
                    PARTITION BY source_name, source_product_name
                    ORDER BY observed_at DESC, price_observation_id DESC
                ) AS source_rn
            FROM price_history
            WHERE price IS NOT NULL
        ),
        latest AS (
            SELECT
                *,
                COALESCE(
                    (
                        SELECT STRING_AGG(token, ' ' ORDER BY token)
                        FROM UNNEST(
                            REGEXP_SPLIT_TO_ARRAY(LOWER(source_product_name), E'[[:space:]]+')
                        ) AS parts(token)
                        WHERE token <> ''
                          AND token !~ E'^[0-9]+(?:[.,][0-9]+)?$'
                          AND token NOT IN ('kg', 'g', 'gram', 'ml', 'l', 'lt', 'adet', 'demet')
                    ),
                    LOWER(source_product_name)
                ) AS source_base_name,
                (
                    SELECT COUNT(*)
                    FROM UNNEST(
                        REGEXP_SPLIT_TO_ARRAY(LOWER(source_product_name), E'[[:space:]]+')
                    ) AS parts(token)
                    WHERE token <> ''
                      AND token !~ E'^[0-9]+(?:[.,][0-9]+)?$'
                      AND token NOT IN ('kg', 'g', 'gram', 'ml', 'l', 'lt', 'adet', 'demet')
                ) AS source_token_count,
                (
                    SELECT COUNT(*)
                    FROM UNNEST(
                        REGEXP_SPLIT_TO_ARRAY(LOWER(source_product_name), E'[[:space:]]+')
                    ) AS parts(token)
                    WHERE token IN (
                        'yerli', 'kokteyl', 'salkim', 'salkım', 'pembe', 'cherry', 'mini',
                        'organik', 'ithal', 'paket', 'özel', 'ozel', 'sosluk',
                        'salçalık', 'salcalik', 'şeker', 'seker',
                        'çengelköy', 'cengelkoy', 'kestane', 'istiridye', 'istridye', 'shiitake',
                        'maden', 'mineral', 'soda', 'gazli', 'aromali',
                        'himalaya', 'kaya', 'salamura', 'sarimsakli', 'truf', 'mantarli',
                        '4x1', '6x200', '6x330', '12x330', 'kutu', 'zero', 'sekersiz',
                        'light', 'diet', 'sugar', 'free', 'lime', 'lemon', 'vanilla', 'cherry'
                    )
                      AND NOT (
                          token = ANY(
                              REGEXP_SPLIT_TO_ARRAY(COALESCE(canonical_search_name, ''), E'[[:space:]]+')
                          )
                      )
                ) AS variant_penalty,
                (
                    SELECT COUNT(*)
                    FROM UNNEST(
                        REGEXP_SPLIT_TO_ARRAY(LOWER(source_product_name), E'[[:space:]]+')
                    ) AS parts(token)
                    WHERE token <> ''
                      AND token !~ E'^[0-9]+(?:[.,][0-9]+)?$'
                      AND token NOT IN ('kg', 'g', 'gram', 'ml', 'l', 'lt', 'adet', 'demet')
                      AND NOT (
                          token = ANY(
                              REGEXP_SPLIT_TO_ARRAY(COALESCE(canonical_search_name, ''), E'[[:space:]]+')
                          )
                      )
                ) AS extra_token_count
            FROM latest_source_base
            WHERE source_rn = 1
        ),
        normalized_latest AS (
            SELECT
                *,
                CASE
                    WHEN canonical_search_name = 'ekmek'
                     AND standardized_product_name IN ('ekmek', 'ekmek sofra')
                    THEN NULL
                    ELSE comparison_unit
                END AS grouping_unit,
                CASE
                    WHEN canonical_search_name = 'ekmek'
                     AND standardized_product_name IN ('ekmek', 'ekmek sofra')
                    THEN NULL
                    WHEN canonical_search_name = 'mantar'
                     AND comparison_unit = 'kg'
                    THEN NULL
                    ELSE comparison_quantity
                END AS grouping_quantity
            FROM latest
        ),
        latest_group_keys AS (
            SELECT
                canonical_search_name,
                grouping_unit,
                grouping_quantity,
                CASE
                    WHEN canonical_search_name IN ('sogan', 'sogan taze', 'arpacik sogan')
                    THEN canonical_search_name
                    WHEN grouping_unit = 'roll'
                     AND grouping_quantity IS NOT NULL
                     AND canonical_search_name LIKE '%%tuvalet kagidi'
                    THEN canonical_search_name || ' ' || grouping_quantity::int::text || ' roll'
                    WHEN grouping_unit = 'roll'
                     AND grouping_quantity IS NOT NULL
                     AND canonical_search_name LIKE '%%kagit havlu'
                    THEN canonical_search_name || ' ' || grouping_quantity::int::text || ' roll'
                    WHEN grouping_unit = 'liter'
                     AND grouping_quantity IS NOT NULL
                     AND canonical_search_name LIKE '%%bulasik deterjani'
                    THEN
                        canonical_search_name || ' ' ||
                        REGEXP_REPLACE(grouping_quantity::text, '\.?0+$', '') || ' l'
                    WHEN canonical_search_name IN (
                        'su',
                        'tuz',
                        'kola pepsi',
                        'kola coca-cola',
                        'yumurta',
                        'bildircin yumurta',
                        'aycicek yagi',
                        'zeytinyagi',
                        'misir yagi',
                        'findik yagi'
                    )
                     AND grouping_unit IS NOT NULL
                     AND grouping_quantity IS NOT NULL
                    THEN
                        canonical_search_name || ' ' ||
                        CASE
                            WHEN grouping_unit = 'kg'
                            THEN REGEXP_REPLACE((grouping_quantity * 1000)::text, '\.?0+$', '') || ' g'
                            WHEN grouping_unit = 'liter'
                            THEN REGEXP_REPLACE(grouping_quantity::text, '\.?0+$', '') || ' l'
                            WHEN grouping_unit = 'piece'
                            THEN grouping_quantity::int::text || ' adet'
                            ELSE REGEXP_REPLACE(grouping_quantity::text, '\.?0+$', '') || ' ' || grouping_unit
                        END
                    WHEN COUNT(DISTINCT standardized_product_name) > 1 THEN canonical_search_name
                    ELSE MAX(standardized_product_name)
                END AS display_product_name
            FROM normalized_latest
            GROUP BY canonical_search_name, grouping_unit, grouping_quantity
        ),
        grouped_latest AS (
            SELECT
                standardized_product_name,
                source_name,
                source_product_name,
                price,
                normalized_unit,
                normalized_quantity,
                observed_at
            FROM (
                SELECT
                    lgk.display_product_name AS standardized_product_name,
                    l.source_name,
                    l.source_product_name,
                    l.price,
                    l.comparison_unit AS normalized_unit,
                    l.comparison_quantity AS normalized_quantity,
                    l.observed_at,
                    l.source_base_name,
                    l.source_token_count,
                    l.variant_penalty,
                    l.extra_token_count,
                    l.price_observation_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY lgk.display_product_name, l.source_name
                        ORDER BY
                            CASE
                                WHEN l.source_base_name = lgk.display_product_name THEN 0
                                ELSE 1
                            END,
                            l.variant_penalty,
                            l.extra_token_count,
                            l.source_token_count,
                            l.observed_at DESC,
                            l.price_observation_id DESC
                    ) AS group_rn
                FROM normalized_latest l
                JOIN latest_group_keys lgk
                    ON l.canonical_search_name = lgk.canonical_search_name
                   AND l.grouping_unit IS NOT DISTINCT FROM lgk.grouping_unit
                   AND l.grouping_quantity IS NOT DISTINCT FROM lgk.grouping_quantity
            ) ranked_grouped
            WHERE group_rn = 1
        ),
        pivoted AS (
            SELECT
                standardized_product_name,
                MAX(standardized_product_name) AS canonical_name,
                MAX(CASE WHEN source_name = 'a101' THEN price END) AS a101_price,
                MAX(CASE WHEN source_name = 'migros' THEN price END) AS migros_price,
                MAX(CASE WHEN source_name = 'a101' THEN source_product_name END) AS a101_source_product_name,
                MAX(CASE WHEN source_name = 'migros' THEN source_product_name END) AS migros_source_product_name,
                MAX(CASE WHEN source_name = 'a101' THEN normalized_unit END) AS a101_unit,
                MAX(CASE WHEN source_name = 'migros' THEN normalized_unit END) AS migros_unit,
                MAX(CASE WHEN source_name = 'a101' THEN normalized_quantity END) AS a101_quantity,
                MAX(CASE WHEN source_name = 'migros' THEN normalized_quantity END) AS migros_quantity,
                MAX(observed_at) AS compared_at,
                COUNT(DISTINCT source_name) AS source_count
            FROM grouped_latest
            WHERE standardized_product_name = ANY(%s)
            GROUP BY standardized_product_name
        ),
        safety AS (
            SELECT
                *,
                (
                    source_count = 2
                    AND a101_unit IS NOT NULL
                    AND migros_unit IS NOT NULL
                    AND a101_unit = migros_unit
                ) AS same_unit_flag,
                (
                    source_count = 2
                    AND a101_quantity IS NOT NULL
                    AND migros_quantity IS NOT NULL
                    AND (
                        a101_quantity = migros_quantity
                        OR (
                            standardized_product_name = 'mantar'
                            AND a101_unit = 'kg'
                            AND migros_unit = 'kg'
                        )
                    )
                ) AS same_quantity_flag
            FROM pivoted
        )
        SELECT
            standardized_product_name,
            canonical_name,
            a101_price,
            migros_price,
            compared_at,
            same_unit_flag,
            same_quantity_flag,
            CASE
                WHEN source_count < 2 THEN 'single_source'
                WHEN same_unit_flag AND same_quantity_flag THEN 'high'
                WHEN same_unit_flag THEN 'medium'
                ELSE 'low'
            END AS comparison_confidence,
            a101_source_product_name,
            migros_source_product_name,
            a101_unit,
            migros_unit,
            a101_quantity,
            migros_quantity
        FROM safety
        """,
        (standardized_products,),
    )

    rows = cursor.fetchall()
    latest_rows = [
        add_measurement_labels(
            _apply_toilet_paper_comparison_rules(
                {
                    "standardized_product_name": row[0],
                    "canonical_name": row[1],
                    "a101_price": row[2],
                    "migros_price": row[3],
                    "cheaper_source": _infer_cheaper_source(row[2], row[3]),
                    "compared_at": row[4],
                    "same_unit_flag": row[5],
                    "same_quantity_flag": row[6],
                    "comparison_confidence": row[7],
                    "a101_source_product_name": row[8],
                    "migros_source_product_name": row[9],
                    "a101_normalized_unit": row[10],
                    "migros_normalized_unit": row[11],
                    "a101_normalized_quantity": row[12],
                    "migros_normalized_quantity": row[13],
                }
            )
        )
        for row in rows
    ]
    if _needs_cleaning_supplement(standardized_products):
        latest_rows.extend(
            _get_cleaning_source_rows(
                cursor,
                _cleaning_supplement_brands(standardized_products),
            )
        )
    if _needs_paper_towel_supplement(standardized_products):
        latest_rows.extend(_get_paper_towel_source_rows(cursor))

    return latest_rows


def _infer_cheaper_source(a101_price, migros_price) -> str | None:
    if a101_price is None and migros_price is None:
        return None
    if a101_price is None:
        return "migros"
    if migros_price is None:
        return "a101"
    if a101_price < migros_price:
        return "a101"
    if migros_price < a101_price:
        return "migros"
    return "same"


def build_price_rows_with_partial_coverage(
    comparable_rows: list[dict],
    latest_price_rows: list[dict],
) -> dict[str, dict]:
    price_rows_by_product = {
        row["standardized_product_name"]: {
            **row,
            "coverage_status": _coverage_status_for_price_row(row),
            "comparison_safe": is_high_confidence_comparison(row),
            "comparison_review_reason": get_comparison_review_reason(row),
        }
        for row in comparable_rows
    }

    augmented_latest_rows = [
        *latest_price_rows,
        *synthesize_cleaning_price_rows(latest_price_rows),
        *synthesize_paper_towel_price_rows(latest_price_rows),
    ]

    for row in augmented_latest_rows:
        standardized_product_name = row["standardized_product_name"]
        existing_row = price_rows_by_product.get(standardized_product_name)
        if existing_row and not _should_prefer_latest_price_row(existing_row, row):
            continue

        retailer_prices = _available_retailer_prices(
            row,
            enforce_comparison_safety=False,
        )
        if len(retailer_prices) == 1:
            coverage_status = f"only_available_at_{retailer_prices[0]['retailer']}"
        elif len(retailer_prices) >= 2 and is_high_confidence_comparison(row):
            coverage_status = COMPARABLE_STATUS
        elif len(retailer_prices) >= 2:
            coverage_status = COMPARISON_REVIEW_STATUS
        else:
            coverage_status = "unavailable"

        price_rows_by_product[standardized_product_name] = {
            **row,
            "coverage_status": coverage_status,
            "comparison_safe": coverage_status != COMPARISON_REVIEW_STATUS,
            "comparison_review_reason": get_comparison_review_reason(row),
        }

    return price_rows_by_product


def _comparison_confidence_rank(price_row: dict) -> int:
    confidence = price_row.get("comparison_confidence")
    return {
        "high": 3,
        "medium": 2,
        "low": 1,
        "single_source": 0,
    }.get(confidence, -1)


def _should_prefer_latest_price_row(
    existing_row: dict,
    latest_row: dict,
) -> bool:
    existing_rank = _comparison_confidence_rank(existing_row)
    latest_rank = _comparison_confidence_rank(latest_row)

    if latest_rank > existing_rank:
        return True

    if latest_rank < existing_rank:
        return False

    existing_has_both_prices = (
        existing_row.get("a101_price") is not None
        and existing_row.get("migros_price") is not None
    )
    latest_has_both_prices = (
        latest_row.get("a101_price") is not None
        and latest_row.get("migros_price") is not None
    )

    if latest_has_both_prices and not existing_has_both_prices:
        return True

    return False


def is_high_confidence_comparison(price_row: dict) -> bool:
    return (
        price_row.get("a101_price") is not None
        and price_row.get("migros_price") is not None
        and price_row.get("same_unit_flag") is True
        and (
            price_row.get("same_quantity_flag") is True
            or price_row.get("soft_equivalent_match") is True
        )
        and price_row.get("comparison_confidence") == SAFE_COMPARISON_CONFIDENCE
    )


def get_comparison_review_reason(price_row: dict) -> str | None:
    has_both_prices = (
        price_row.get("a101_price") is not None
        and price_row.get("migros_price") is not None
    )
    if not has_both_prices or is_high_confidence_comparison(price_row):
        return None

    if price_row.get("same_unit_flag") is False:
        return "unit_mismatch"

    if price_row.get("same_quantity_flag") is False:
        return "quantity_mismatch"

    explicit_reason = price_row.get("comparison_review_reason")
    if explicit_reason:
        return explicit_reason

    return "comparison_confidence_not_high"


def _coverage_status_for_price_row(price_row: dict) -> str:
    retailer_prices = _available_retailer_prices(
        price_row,
        enforce_comparison_safety=False,
    )

    if len(retailer_prices) < 2:
        return "unavailable"

    if is_high_confidence_comparison(price_row):
        return COMPARABLE_STATUS

    return COMPARISON_REVIEW_STATUS


def _requires_comparison_review(price_row: dict) -> bool:
    return price_row.get("coverage_status") == COMPARISON_REVIEW_STATUS


def _zero_like(prices: list[Decimal | int | float]):
    if any(isinstance(price, Decimal) for price in prices):
        return Decimal("0")
    return 0


def _available_retailer_prices(
    price_row: dict,
    enforce_comparison_safety: bool = True,
) -> list[dict]:
    if enforce_comparison_safety and _requires_comparison_review(price_row):
        return []

    retailer_prices = []

    for retailer, price_column in RETAILER_PRICE_COLUMNS.items():
        price = price_row.get(price_column)
        if price is None:
            continue

        retailer_prices.append(
            {
                "retailer": retailer,
                "price": price,
            }
        )

    return retailer_prices


def _choose_retailer_price(price_row: dict) -> dict | None:
    retailer_prices = _available_retailer_prices(price_row)

    if not retailer_prices:
        return None

    return min(
        retailer_prices,
        key=lambda item: (item["price"], item["retailer"]),
    )


def choose_best_valid_retailer_price(price_row: dict) -> dict | None:
    return _choose_retailer_price(price_row)


def calculate_cross_compare_mixed_basket(price_rows_by_product: dict[str, dict]) -> dict:
    items = []
    selected_prices = []

    for standardized_product_name, price_row in price_rows_by_product.items():
        selected = _choose_retailer_price(price_row)

        if selected is None:
            availability_status = (
                COMPARISON_REVIEW_STATUS
                if _requires_comparison_review(price_row)
                else "no_valid_price"
            )
            items.append(
                {
                    "product_name": standardized_product_name,
                    "standardized_product_name": standardized_product_name,
                    "market": None,
                    "recommended_retailer": None,
                    "price": None,
                    "selected_price": None,
                    "availability_status": availability_status,
                    "a101_price": price_row.get("a101_price"),
                    "migros_price": price_row.get("migros_price"),
                    "coverage_status": price_row.get("coverage_status"),
                    "same_unit_flag": price_row.get("same_unit_flag"),
                    "same_quantity_flag": price_row.get("same_quantity_flag"),
                    "comparison_confidence": price_row.get("comparison_confidence"),
                    "comparison_review_reason": price_row.get(
                        "comparison_review_reason"
                    ),
                    **_selection_fields(price_row),
                    **_measurement_fields(price_row),
                }
            )
            continue

        selected_prices.append(selected["price"])
        items.append(
            {
                "product_name": standardized_product_name,
                "standardized_product_name": standardized_product_name,
                "market": selected["retailer"],
                "recommended_retailer": selected["retailer"],
                "price": selected["price"],
                "price_per_unit": None,
                "selected_price": selected["price"],
                "availability_status": "ok",
                "a101_price": price_row.get("a101_price"),
                "migros_price": price_row.get("migros_price"),
                "coverage_status": price_row.get("coverage_status", COMPARABLE_STATUS),
                "same_unit_flag": price_row.get("same_unit_flag"),
                "same_quantity_flag": price_row.get("same_quantity_flag"),
                "comparison_confidence": price_row.get("comparison_confidence"),
                **_selection_fields(price_row),
                **_measurement_fields(price_row),
            }
        )

    total_price = _zero_like(selected_prices)
    for price in selected_prices:
        total_price += price

    return {
        "items": items,
        "total_price": total_price,
        "complete": all(item["availability_status"] == "ok" for item in items),
    }


def calculate_cross_compare_single_retailer_baskets(
    price_rows_by_product: dict[str, dict],
) -> list[dict]:
    baskets = []

    for retailer, price_column in RETAILER_PRICE_COLUMNS.items():
        items = []
        selected_prices = []
        missing_products = []

        for standardized_product_name, price_row in price_rows_by_product.items():
            if _requires_comparison_review(price_row):
                missing_products.append(standardized_product_name)
                continue

            price = price_row.get(price_column)

            if price is None:
                missing_products.append(standardized_product_name)
                continue

            selected_prices.append(price)
            items.append(
                {
                    "product_name": standardized_product_name,
                    "standardized_product_name": standardized_product_name,
                    "market": retailer,
                    "price": price,
                    "price_per_unit": None,
                    "selected_price": price,
                    "availability_status": "ok",
                    "coverage_status": price_row.get("coverage_status", COMPARABLE_STATUS),
                    **_selection_fields(price_row),
                    **_measurement_fields(price_row),
                }
            )

        if missing_products:
            continue

        total_price = _zero_like(selected_prices)
        for price in selected_prices:
            total_price += price

        baskets.append(
            {
                "retailer": retailer,
                "market": retailer,
                "items_count": len(items),
                "total_price": total_price,
                "items": items,
                "complete": True,
            }
        )

    return sorted(
        baskets,
        key=lambda basket: (basket["total_price"], basket["retailer"]),
    )


def build_product_recommendations(price_rows_by_product: dict[str, dict]) -> list[dict]:
    recommendations = []

    for standardized_product_name, price_row in price_rows_by_product.items():
        selected = _choose_retailer_price(price_row)

        recommendations.append(
            {
                "standardized_product_name": standardized_product_name,
                "canonical_name": price_row.get("canonical_name"),
                "recommended_retailer": selected["retailer"] if selected else None,
                "recommended_price": selected["price"] if selected else None,
                "a101_price": price_row.get("a101_price"),
                "migros_price": price_row.get("migros_price"),
                "availability_status": (
                    "ok"
                    if selected
                    else price_row.get("coverage_status", "no_valid_price")
                ),
                "coverage_status": price_row.get("coverage_status", COMPARABLE_STATUS),
                "same_unit_flag": price_row.get("same_unit_flag"),
                "same_quantity_flag": price_row.get("same_quantity_flag"),
                "comparison_confidence": price_row.get("comparison_confidence"),
                "comparison_review_reason": price_row.get("comparison_review_reason"),
                **_selection_fields(price_row),
                **_measurement_fields(price_row),
            }
        )

    return recommendations


def get_latest_prices(cursor, product_id: int) -> list[dict]:
    cursor.execute(
        """
        SELECT
            source_name,
            standardized_product_name,
            price_per_unit,
            price
        FROM mart_latest_prices
        WHERE product_id = %s;
        """,
        (product_id,),
    )

    rows = cursor.fetchall()

    return [
        {
            "source_name": row[0],
            "standardized_product_name": row[1],
            "price_per_unit": row[2],
            "price": row[3],
        }
        for row in rows
    ]


def get_cheapest_price(prices: list[dict]) -> dict | None:
    valid_prices = [
        price_info
        for price_info in prices
        if get_selected_price(price_info) is not None
    ]

    if not valid_prices:
        return None

    return min(valid_prices, key=get_selected_price)


def calculate_single_market_basket(cursor, product_ids: list[int]) -> list[dict]:
    market_totals = {}
    required_items_count = len(product_ids)

    for product_id in product_ids:
        prices = get_latest_prices(cursor, product_id)
        best_prices_by_market = {}

        for price_info in prices:
            selected_price = get_selected_price(price_info)

            if selected_price is None:
                continue

            market = price_info["source_name"]
            current_best = best_prices_by_market.get(market)

            if current_best is None or selected_price < current_best["selected_price"]:
                best_prices_by_market[market] = {
                    "selected_price": selected_price,
                }

        for market, market_price_info in best_prices_by_market.items():
            if market not in market_totals:
                market_totals[market] = {
                    "market": market,
                    "items_count": 0,
                    "total_price": 0,
                }

            market_totals[market]["items_count"] += 1
            market_totals[market]["total_price"] += market_price_info["selected_price"]

    return [
        market_info
        for market_info in sorted(
            market_totals.values(),
            key=lambda market_info: (market_info["total_price"], market_info["market"]),
        )
        if market_info["items_count"] == required_items_count
    ]


def calculate_split_basket(cursor, product_ids: list[int]) -> dict:
    items = []
    total_price = 0

    for product_id in product_ids:
        prices = get_latest_prices(cursor, product_id)

        if not prices:
            items.append(
                {
                    "product_id": product_id,
                    "product_name": None,
                    "market": None,
                    "price": None,
                    "price_per_unit": None,
                    "selected_price": None,
                    "availability_status": "no_prices_found",
                }
            )
            continue

        cheapest = get_cheapest_price(prices)

        if cheapest is None:
            items.append(
                {
                    "product_id": product_id,
                    "product_name": prices[0].get("standardized_product_name"),
                    "market": None,
                    "price": None,
                    "price_per_unit": None,
                    "selected_price": None,
                    "availability_status": "no_valid_price",
                }
            )
            continue

        selected_price = get_selected_price(cheapest)

        items.append(
            {
                "product_id": product_id,
                "product_name": cheapest.get("standardized_product_name"),
                "market": cheapest["source_name"],
                "price": cheapest.get("price"),
                "price_per_unit": cheapest.get("price_per_unit"),
                "selected_price": selected_price,
                "availability_status": "ok",
            }
        )

        if selected_price is not None:
            total_price += selected_price

    return {
        "items": items,
        "total_price": total_price,
    }
