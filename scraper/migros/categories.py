import requests

from scraper.migros.extract import parse_migros_products


CATEGORY_BASE_URL = "https://www.migros.com.tr/rest/search/screens"


def get_migros_category_products(category_slug: str = "meyve-sebze-c-2") -> list[dict]:
    url = f"{CATEGORY_BASE_URL}/{category_slug}"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "x-device-pwa": "true",
        "x-forwarded-rest": "true",
        "x-pwa": "true",
    }

    response = requests.get(
        url,
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()

    api_json = response.json()

    products = parse_migros_products(api_json)
    return products


if __name__ == "__main__":
    products = get_migros_category_products("meyve-sebze-c-2")
    print(f"Toplam ürün: {len(products)}")

    for product in products[:5]:
        print(product)