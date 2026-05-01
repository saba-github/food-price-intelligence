from scraper.migros import categories


def _page_payload(page_count, product_ids):
    return {
        "data": {
            "searchInfo": {
                "pageCount": page_count,
                "storeProductInfos": [
                    {"id": product_id}
                    for product_id in product_ids
                ],
            }
        }
    }


def test_migros_category_products_fetches_all_pages(monkeypatch):
    calls = []
    payloads = {
        1: _page_payload(3, ["1", "2"]),
        2: _page_payload(3, ["3"]),
        3: _page_payload(3, ["2", "4"]),
    }

    def fake_get_json(url, params, headers, timeout):
        calls.append(params["page"])
        return payloads[params["page"]]

    def fake_parse(api_json):
        store_products = api_json["data"]["searchInfo"]["storeProductInfos"]
        return [
            {
                "product_id": product["id"],
                "product_name": f"Product {product['id']}",
                "product_url": f"/p/{product['id']}",
            }
            for product in store_products
        ]

    monkeypatch.setattr(categories, "get_json", fake_get_json)
    monkeypatch.setattr(categories, "parse_migros_products", fake_parse)

    products = categories.get_migros_category_products("meyve-sebze-c-2")

    assert calls == [1, 2, 3]
    assert [product["product_id"] for product in products] == ["1", "2", "3", "4"]
