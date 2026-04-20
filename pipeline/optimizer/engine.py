from pipeline.optimizer.matching import find_product_match
from pipeline.optimizer.pricing import (
    calculate_single_market_basket,
    calculate_split_basket,
    get_latest_prices,
)


def optimize_basket(cursor, user_inputs: list[str]) -> dict:
    matched_products = []

    for user_input in user_inputs:
        match = find_product_match(cursor, user_input)

        available_markets = []
        standardized_product_name = None

        if match["product_id"] is not None:
            prices = get_latest_prices(cursor, match["product_id"])
            available_markets = sorted(
                list(
                    {
                        p["source_name"]
                        for p in prices
                        if p.get("source_name")
                    }
                )
            )
            if prices:
                standardized_product_name = prices[0].get("standardized_product_name")

        matched_products.append(
            {
                "input": user_input,
                "normalized_input": match["normalized_input"],
                "product_id": match["product_id"],
                "found": match["found"],
                "match_type": match["match_type"],
                "standardized_product_name": standardized_product_name,
                "available_markets": available_markets,
            }
        )

    product_ids = [
        item["product_id"]
        for item in matched_products
        if item["product_id"] is not None
    ]

    if product_ids:
        split_basket = calculate_split_basket(cursor, product_ids)
        single_market_options = calculate_single_market_basket(cursor, product_ids)
    else:
        split_basket = {
            "items": [],
            "total_price": 0,
        }
        single_market_options = []

    return {
        "input": user_inputs,
        "matched_products": matched_products,
        "split_basket": split_basket,
        "single_market_options": single_market_options,
    }
