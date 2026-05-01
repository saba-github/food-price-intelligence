from pipeline.optimizer.matching import normalize_input
from pipeline.transforms import normalize_text
from pipeline.optimizer.pricing import (
    COMPARABLE_STATUS,
    COMPARISON_REVIEW_STATUS,
    MEASUREMENT_FIELDS,
    RETAILER_PRICE_COLUMNS,
    build_product_recommendations,
    build_price_rows_with_partial_coverage,
    calculate_cross_compare_mixed_basket,
    calculate_cross_compare_single_retailer_baskets,
    choose_best_valid_retailer_price,
    get_cross_compare_prices,
    get_comparison_review_reason,
    get_latest_price_history_prices,
    is_high_confidence_comparison,
)


def _measurement_fields(price_row: dict) -> dict:
    return {
        field_name: price_row.get(field_name)
        for field_name in MEASUREMENT_FIELDS
    }


def _selection_fields(price_row: dict) -> dict:
    field_names = [
        "selection_type",
        "family_id",
        "family_label",
        "family_options",
        "family_option_count",
        "selected_family_product_name",
        "force_review",
    ]
    return {
        field_name: price_row.get(field_name)
        for field_name in field_names
        if field_name in price_row
    }


def _normalize_product_names(product_names: list[str]) -> list[str]:
    normalized_products = []
    for product_name in product_names:
        standardized_product_name = normalize_input(product_name)
        raw_normalized_product_name = normalize_text(product_name)
        if (
            standardized_product_name
            and standardized_product_name not in normalized_products
        ):
            normalized_products.append(standardized_product_name)
        if (
            raw_normalized_product_name
            and raw_normalized_product_name not in normalized_products
        ):
            normalized_products.append(raw_normalized_product_name)

    return normalized_products


def _normalize_basket_requests(user_inputs: list[str | dict]) -> list[dict]:
    basket_requests = []
    seen_requests = set()

    for user_input in user_inputs:
        if isinstance(user_input, dict) and user_input.get("type") == "product_family":
            candidate_products = _normalize_product_names(
                user_input.get("product_names") or user_input.get("family_options") or []
            )
            if not candidate_products:
                continue

            family_id = user_input.get("family_id") or "|".join(candidate_products)
            request_key = ("product_family", family_id)
            if request_key in seen_requests:
                continue

            seen_requests.add(request_key)
            basket_requests.append(
                {
                    "selection_type": "product_family",
                    "family_id": family_id,
                    "family_label": user_input.get("family_label") or family_id,
                    "candidate_products": candidate_products,
                    "force_review": bool(user_input.get("force_review")),
                }
            )
            continue

        standardized_product_name = normalize_input(user_input)
        raw_normalized_product_name = normalize_text(user_input)
        candidate_products = []
        if standardized_product_name:
            candidate_products.append(standardized_product_name)
        if (
            raw_normalized_product_name
            and raw_normalized_product_name not in candidate_products
        ):
            candidate_products.append(raw_normalized_product_name)

        if not candidate_products:
            continue

        request_key = ("product", tuple(candidate_products))
        if request_key in seen_requests:
            continue

        seen_requests.add(request_key)
        basket_requests.append(
            {
                "selection_type": "product",
                "candidate_products": candidate_products,
                "family_id": None,
                "family_label": None,
            }
        )

    return basket_requests


def _candidate_products(basket_requests: list[dict]) -> list[str]:
    products = []
    for request in basket_requests:
        for product_name in request["candidate_products"]:
            if product_name not in products:
                products.append(product_name)

    return products


def _decorate_selected_price_row(
    price_row: dict,
    request: dict,
    selected_product_name: str,
) -> dict:
    if request["selection_type"] != "product_family":
        return price_row

    decorated_row = {
        **price_row,
        "selection_type": "product_family",
        "family_id": request["family_id"],
        "family_label": request["family_label"],
        "family_options": request["candidate_products"],
        "family_option_count": len(request["candidate_products"]),
        "selected_family_product_name": selected_product_name,
    }
    if request.get("force_review"):
        decorated_row["force_review"] = True
        decorated_row["coverage_status"] = COMPARISON_REVIEW_STATUS
        decorated_row["comparison_review_reason"] = (
            decorated_row.get("comparison_review_reason")
            or "subtype_selection_required"
        )
    return decorated_row


def _choose_request_product(
    request: dict,
    price_rows_by_product: dict[str, dict],
) -> tuple[str | None, dict | None]:
    if request["selection_type"] != "product_family":
        ranked_candidates = []
        for index, product_name in enumerate(request["candidate_products"]):
            price_row = price_rows_by_product.get(product_name)
            if price_row is None:
                continue

            if is_high_confidence_comparison(price_row):
                rank = 0
            elif price_row.get("coverage_status") == COMPARISON_REVIEW_STATUS:
                rank = 1
            elif choose_best_valid_retailer_price(price_row) is not None:
                rank = 2
            else:
                rank = 3

            ranked_candidates.append((rank, index, product_name, price_row))

        if ranked_candidates:
            _, _, selected_product_name, selected_price_row = min(ranked_candidates)
            return selected_product_name, selected_price_row

        product_name = request["candidate_products"][0]
        return product_name, None

    valid_candidates = []
    fallback_candidates = []

    for index, product_name in enumerate(request["candidate_products"]):
        price_row = price_rows_by_product.get(product_name)
        if price_row is None:
            continue

        fallback_candidates.append((index, product_name, price_row))
        selected_price = choose_best_valid_retailer_price(price_row)
        if selected_price is None:
            continue

        valid_candidates.append(
            (
                0 if is_high_confidence_comparison(price_row) else (
                    1 if price_row.get("coverage_status") == COMPARISON_REVIEW_STATUS else 2
                ),
                index,
                selected_price["price"],
                selected_price["retailer"],
                product_name,
                price_row,
            )
        )

    if valid_candidates:
        _, _, _, _, selected_product_name, selected_price_row = min(valid_candidates)
        return selected_product_name, selected_price_row

    if fallback_candidates:
        _, selected_product_name, selected_price_row = fallback_candidates[0]
        return selected_product_name, selected_price_row

    return None, None


def optimize_basket(cursor, user_inputs: list[str | dict]) -> dict:
    basket_requests = _normalize_basket_requests(user_inputs)
    candidate_products = _candidate_products(basket_requests)

    comparable_price_rows = get_cross_compare_prices(cursor, candidate_products)
    latest_price_rows = get_latest_price_history_prices(cursor, candidate_products)
    price_rows_by_product = build_price_rows_with_partial_coverage(
        comparable_price_rows,
        latest_price_rows,
    )
    comparable_product_names = {
        row["standardized_product_name"]
        for row in price_rows_by_product.values()
        if is_high_confidence_comparison(row)
    }
    review_required_product_names = {
        row["standardized_product_name"]
        for row in price_rows_by_product.values()
        if not is_high_confidence_comparison(row)
        and row.get("a101_price") is not None
        and row.get("migros_price") is not None
    }
    cross_compare_product_names = {
        row["standardized_product_name"]
        for row in comparable_price_rows
    }

    matched_products = []
    unavailable_products = []
    standardized_products = []
    selected_price_rows_by_product = {}

    for request in basket_requests:
        standardized_product_name, price_row = _choose_request_product(
            request,
            price_rows_by_product,
        )

        if standardized_product_name:
            standardized_products.append(standardized_product_name)

        display_name = (
            request["family_label"]
            if request["selection_type"] == "product_family"
            else standardized_product_name
        )

        if price_row is not None and standardized_product_name:
            price_row = _decorate_selected_price_row(
                price_row,
                request,
                standardized_product_name,
            )
            selected_price_rows_by_product[standardized_product_name] = price_row

        if not standardized_product_name:
            unavailable_products.append(
                {
                    "standardized_product_name": display_name,
                    "reason": "not_found_in_price_history",
                    "selection_type": request["selection_type"],
                    "family_options": request["candidate_products"],
                }
            )
            matched_products.append(
                {
                    "input": display_name,
                    "normalized_input": display_name,
                    "product_id": None,
                    "found": False,
                    "match_type": "not_found_in_price_history",
                    "standardized_product_name": display_name,
                    "available_markets": [],
                    "coverage_status": "unavailable",
                    "selection_type": request["selection_type"],
                    "family_options": request["candidate_products"],
                }
            )
            continue

        price_row = selected_price_rows_by_product.get(standardized_product_name)

        if price_row is None:
            unavailable_products.append(
                {
                    "standardized_product_name": display_name,
                    "reason": "not_found_in_price_history",
                    **(
                        {
                            "selection_type": request["selection_type"],
                            "family_options": request["candidate_products"],
                        }
                        if request["selection_type"] == "product_family"
                        else {}
                    ),
                }
            )
            matched_products.append(
                {
                    "input": display_name,
                    "normalized_input": display_name,
                    "product_id": None,
                    "found": False,
                    "match_type": "not_found_in_price_history",
                    "standardized_product_name": display_name,
                    "available_markets": [],
                    "coverage_status": "unavailable",
                    **(
                        {
                            "selection_type": request["selection_type"],
                            "family_options": request["candidate_products"],
                        }
                        if request["selection_type"] == "product_family"
                        else {}
                    ),
                }
            )
            continue

        available_markets = [
            retailer
            for retailer, price_column in RETAILER_PRICE_COLUMNS.items()
            if price_row.get(price_column) is not None
        ]
        coverage_status = price_row.get("coverage_status", COMPARABLE_STATUS)
        if coverage_status == COMPARISON_REVIEW_STATUS:
            match_type = COMPARISON_REVIEW_STATUS
        elif standardized_product_name in comparable_product_names:
            if standardized_product_name in cross_compare_product_names:
                match_type = "mart_cross_compare_exact"
            else:
                match_type = "latest_price_history_exact"
        else:
            match_type = "single_source_latest_price"

        matched_products.append(
            {
                "input": standardized_product_name,
                "normalized_input": standardized_product_name,
                "product_id": None,
                "found": True,
                "match_type": match_type,
                "standardized_product_name": price_row["standardized_product_name"],
                "available_markets": available_markets,
                "coverage_status": coverage_status,
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

    matched_price_rows = {
        product_name: selected_price_rows_by_product[product_name]
        for product_name in standardized_products
        if product_name in selected_price_rows_by_product
    }
    comparable_products = [
        {
            "standardized_product_name": product_name,
            "canonical_name": selected_price_rows_by_product[product_name].get(
                "canonical_name"
            ),
            "a101_price": selected_price_rows_by_product[product_name].get(
                "a101_price"
            ),
            "migros_price": selected_price_rows_by_product[product_name].get(
                "migros_price"
            ),
            "cheaper_source": selected_price_rows_by_product[product_name].get(
                "cheaper_source"
            ),
            "comparison_confidence": selected_price_rows_by_product[product_name].get(
                "comparison_confidence"
            ),
            **_selection_fields(selected_price_rows_by_product[product_name]),
            **_measurement_fields(selected_price_rows_by_product[product_name]),
        }
        for product_name in standardized_products
        if product_name in comparable_product_names
        and product_name in selected_price_rows_by_product
    ]
    suspicious_comparison_products = [
        {
            "standardized_product_name": product_name,
            "canonical_name": selected_price_rows_by_product[product_name].get(
                "canonical_name"
            ),
            "a101_price": selected_price_rows_by_product[product_name].get(
                "a101_price"
            ),
            "migros_price": selected_price_rows_by_product[product_name].get(
                "migros_price"
            ),
            "same_unit_flag": selected_price_rows_by_product[product_name].get(
                "same_unit_flag"
            ),
            "same_quantity_flag": selected_price_rows_by_product[product_name].get(
                "same_quantity_flag"
            ),
            "comparison_confidence": selected_price_rows_by_product[product_name].get(
                "comparison_confidence"
            ),
            "comparison_review_reason": (
                selected_price_rows_by_product[product_name].get(
                    "comparison_review_reason"
                )
                or get_comparison_review_reason(
                    selected_price_rows_by_product[product_name]
                )
            ),
            **_selection_fields(selected_price_rows_by_product[product_name]),
            **_measurement_fields(selected_price_rows_by_product[product_name]),
        }
        for product_name in standardized_products
        if product_name in review_required_product_names
        and product_name in selected_price_rows_by_product
    ]
    single_source_only_products = []

    for product_name in standardized_products:
        price_row = selected_price_rows_by_product.get(product_name)
        if not price_row:
            continue

        coverage_status = price_row.get("coverage_status")
        if not coverage_status or not coverage_status.startswith("only_available_at_"):
            continue

        only_retailer = coverage_status.replace("only_available_at_", "")
        single_source_only_products.append(
            {
                "standardized_product_name": product_name,
                "canonical_name": price_row.get("canonical_name"),
                "retailer": only_retailer,
                "availability_status": coverage_status,
                "price": price_row.get(RETAILER_PRICE_COLUMNS[only_retailer]),
                "a101_price": price_row.get("a101_price"),
                "migros_price": price_row.get("migros_price"),
                **_selection_fields(price_row),
                **_measurement_fields(price_row),
            }
        )

    mixed_basket = calculate_cross_compare_mixed_basket(matched_price_rows)
    single_market_options = calculate_cross_compare_single_retailer_baskets(
        matched_price_rows
    )
    cheapest_single_retailer_basket = (
        single_market_options[0] if single_market_options else None
    )
    savings_amount = None

    if cheapest_single_retailer_basket is not None:
        savings_amount = (
            cheapest_single_retailer_basket["total_price"]
            - mixed_basket["total_price"]
        )

    return {
        "input": user_inputs,
        "basket_requests": basket_requests,
        "standardized_products": standardized_products,
        "matched_products": matched_products,
        "unmatched_products": [
            item["standardized_product_name"]
            for item in unavailable_products
        ],
        "unavailable_products": unavailable_products,
        "single_source_only_products": single_source_only_products,
        "comparable_products": comparable_products,
        "suspicious_comparison_products": suspicious_comparison_products,
        "optimized_basket_total": mixed_basket["total_price"],
        "cheapest_single_retailer_basket": cheapest_single_retailer_basket,
        "cheapest_mixed_basket": mixed_basket,
        "mixed_basket": mixed_basket,
        "savings_amount": savings_amount,
        "per_product_recommendations": build_product_recommendations(
            matched_price_rows
        ),
        "split_basket": mixed_basket,
        "single_market_options": single_market_options,
    }
