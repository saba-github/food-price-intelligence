from pipeline.optimizer.matching import find_product_id
from pipeline.optimizer.pricing import calculate_split_basket


def optimize_basket(cursor, user_inputs: list[str]) -> dict:
    matched_products = []

    for user_input in user_inputs:
        product_id = find_product_id(cursor, user_input)
        matched_products.append(
            {
                "input": user_input,
                "product_id": product_id,
            }
        )

    product_ids = [
        item["product_id"]
        for item in matched_products
        if item["product_id"] is not None
    ]

    if product_ids:
        basket = calculate_split_basket(cursor, product_ids)
    else:
        basket = {
            "items": [],
            "total_price": 0,
        }

    return {
        "input": user_inputs,
        "matched_products": matched_products,
        "basket": basket,
    }
