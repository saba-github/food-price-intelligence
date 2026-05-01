from scraper.migros.extract import parse_migros_products
from scraper.migros.http import get_json


CATEGORY_BASE_URL = "https://www.migros.com.tr/rest/search/screens"


def _get_category_page(category_slug: str, page: int) -> dict:
    url = f"{CATEGORY_BASE_URL}/{category_slug}"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "x-device-pwa": "true",
        "x-forwarded-rest": "true",
        "x-pwa": "true",
    }

    return get_json(
        url,
        params={"page": page},
        headers=headers,
        timeout=30,
    )


def _get_page_count(api_json: dict) -> int:
    search_info = (api_json.get("data") or {}).get("searchInfo") or {}
    try:
        return int(search_info.get("pageCount") or 1)
    except (TypeError, ValueError):
        return 1


def get_migros_category_products(category_slug: str = "meyve-sebze-c-2") -> list[dict]:
    first_page_json = _get_category_page(category_slug, 1)
    page_count = _get_page_count(first_page_json)

    products = parse_migros_products(first_page_json)

    for page in range(2, page_count + 1):
        api_json = _get_category_page(category_slug, page)
        products.extend(parse_migros_products(api_json))

    deduped_products = []
    seen_keys = set()
    for product in products:
        dedupe_key = product.get("product_id") or (
            product.get("product_name"),
            product.get("product_url"),
        )
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        deduped_products.append(product)

    return deduped_products


if __name__ == "__main__":
    products = get_migros_category_products("meyve-sebze-c-2")
    print(f"Toplam ürün: {len(products)}")

    for product in products[:5]:
        print(product)
