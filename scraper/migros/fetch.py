import requests

from scraper.migros.extract import parse_migros_products


BASE_URL = "https://www.migros.com.tr/rest/search/screens/products"


def get_migros_products(query: str = "domates") -> list[dict]:
    params = {
        "q": query,
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    }

    response = requests.get(
        BASE_URL,
        params=params,
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()

    api_json = response.json()
    products = parse_migros_products(api_json)

    return products


if __name__ == "__main__":
    products = get_migros_products("domates")
    print(f"Toplam ürün: {len(products)}")

    for product in products[:5]:
        print(product)
