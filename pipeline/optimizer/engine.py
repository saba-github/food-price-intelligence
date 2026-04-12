from pipeline.optimizer.matching import find_product_id
from pipeline.optimizer.pricing import (
    calculate_single_market_basket,
    calculate_split_basket,
)


def optimize_basket(cursor, user_inputs: list[str]) -> dict:
    matched_products = []

    for user_input in user_inputs:
        product_id = find_product_id(cursor, user_input)
        matched_products.append(
            {
                "input": user_input,
                "product_id": product_id,
                "found": product_id is not None,
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