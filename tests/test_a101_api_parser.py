from scraper.a101.scraper import (
    _find_category_by_id,
    fetch_a101_category_api,
    parse_a101_api_products,
)


def test_a101_api_parser_extracts_products_from_category_children():
    api_json = {
        "id": "C01",
        "name": "Meyve, Sebze",
        "children": [
            {
                "id": "C0101",
                "name": "Meyve",
                "products": [
                    {
                        "id": "20000802",
                        "attributes": {
                            "name": "İthal Muz Kg",
                            "salesUnitOfMeasure": "KG",
                            "baseUnitOfMeasure": "KG",
                            "seoUrl": "https://www.a101.com.tr/kapida/meyve-sebze/ithal-muz-kg_p-20000802",
                        },
                        "price": {
                            "normal": 13490,
                            "discounted": 11990,
                        },
                    }
                ],
            }
        ],
    }

    products = parse_a101_api_products(api_json)

    assert products == [
        {
            "product_id": "20000802",
            "product_name": "İthal Muz Kg",
            "sku": "20000802",
            "shown_price_tl": 119.9,
            "regular_price_tl": 134.9,
            "discount_rate": 0.1112,
            "product_url": "https://www.a101.com.tr/kapida/meyve-sebze/ithal-muz-kg_p-20000802",
            "brand_name": None,
            "category_name": "Meyve",
            "unit": "KG",
            "unit_amount": 1.0,
        }
    ]


def test_find_category_by_id_returns_nested_a101_category():
    api_json = {
        "id": "C01",
        "name": "Meyve, Sebze",
        "children": [
            {"id": "C0101", "name": "Meyve", "products": []},
            {"id": "C0102", "name": "Sebze", "products": []},
        ],
    }

    assert _find_category_by_id(api_json, "C0102") == {
        "id": "C0102",
        "name": "Sebze",
        "products": [],
    }
    assert _find_category_by_id(api_json, "missing") is None


def test_fetch_a101_category_api_resolves_generic_nested_category(monkeypatch):
    parent_tree = {
        "id": "C05",
        "name": "Süt Ürünleri, Kahvaltılık",
        "children": [
            {"id": "C0502", "name": "Süt", "products": []},
            {"id": "C0503", "name": "Yumurta", "products": []},
        ],
    }

    requested = []

    def fake_request(category_id: str, category_slug: str):
        requested.append((category_id, category_slug))
        return parent_tree

    monkeypatch.setattr("scraper.a101.scraper._request_a101_category", fake_request)

    assert fetch_a101_category_api("sut-urunleri-kahvaltilik/yumurta") == {
        "id": "C0503",
        "name": "Yumurta",
        "products": [],
    }
    assert requested == [("C05", "sut-urunleri-kahvaltilik")]


def test_fetch_a101_category_api_resolves_temel_gida_nested_category(monkeypatch):
    parent_tree = {
        "id": "C07",
        "name": "Temel Gıda",
        "children": [
            {"id": "C0703", "name": "Şeker", "products": []},
            {"id": "C0705", "name": "Un", "products": []},
        ],
    }

    requested = []

    def fake_request(category_id: str, category_slug: str):
        requested.append((category_id, category_slug))
        return parent_tree

    monkeypatch.setattr("scraper.a101.scraper._request_a101_category", fake_request)

    assert fetch_a101_category_api("temel-gida/un") == {
        "id": "C0705",
        "name": "Un",
        "products": [],
    }
    assert requested == [("C07", "temel-gida")]


def test_fetch_a101_category_api_resolves_kagit_urunleri_nested_category(monkeypatch):
    parent_tree = {
        "id": "C13",
        "name": "Kagit Urunleri",
        "children": [
            {"id": "C1301", "name": "Tuvalet Kagidi", "products": []},
            {"id": "C1303", "name": "Kagit Havlu", "products": []},
        ],
    }

    requested = []

    def fake_request(category_id: str, category_slug: str):
        requested.append((category_id, category_slug))
        return parent_tree

    monkeypatch.setattr("scraper.a101.scraper._request_a101_category", fake_request)

    assert fetch_a101_category_api("kagit-urunleri/tuvalet-kagidi") == {
        "id": "C1301",
        "name": "Tuvalet Kagidi",
        "products": [],
    }
    assert requested == [("C13", "kagit-urunleri")]


def test_fetch_a101_category_api_resolves_kagit_havlu_nested_category(monkeypatch):
    parent_tree = {
        "id": "C13",
        "name": "Kagit Urunleri",
        "children": [
            {"id": "C1301", "name": "Tuvalet Kagidi", "products": []},
            {"id": "C1303", "name": "Kagit Havlu", "products": []},
        ],
    }

    requested = []

    def fake_request(category_id: str, category_slug: str):
        requested.append((category_id, category_slug))
        return parent_tree

    monkeypatch.setattr("scraper.a101.scraper._request_a101_category", fake_request)

    assert fetch_a101_category_api("kagit-urunleri/kagit-havlu") == {
        "id": "C1303",
        "name": "Kagit Havlu",
        "products": [],
    }
    assert requested == [("C13", "kagit-urunleri")]
