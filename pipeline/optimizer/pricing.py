def get_latest_prices(cursor, product_id: int) -> list[dict]:
    cursor.execute(
        """
        SELECT
            source_name,
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
            "price_per_unit": row[1],
            "price": row[2],
        }
        for row in rows
    ]


def get_cheapest_price(prices: list[dict]) -> dict | None:
    valid_prices = [
        price_info
        for price_info in prices
        if price_info.get("price_per_unit") is not None
    ]

    if not valid_prices:
        return None

    return min(valid_prices, key=lambda price_info: price_info["price_per_unit"])


def calculate_split_basket(cursor, product_ids: list[int]) -> dict:
    items = []
    total_price = 0

    for product_id in product_ids:
        prices = get_latest_prices(cursor, product_id)
        cheapest = get_cheapest_price(prices)

        if cheapest is None:
            continue

        item_price = cheapest.get("price")

        items.append(
            {
                "product_id": product_id,
                "market": cheapest["source_name"],
                "price": item_price,
            }
        )

        if item_price is not None:
            total_price += item_price

    return {
        "items": items,
        "total_price": total_price,
    }
