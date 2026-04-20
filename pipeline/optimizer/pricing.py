def get_selected_price(price_info: dict) -> int | float | None:
    selected_price = price_info.get("price_per_unit")

    if selected_price is None:
        selected_price = price_info.get("price")

    return selected_price


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
