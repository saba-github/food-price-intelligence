from scraper.a101.categories import get_a101_category_products
from config.retailers import RETAILER_CONFIG

def run_pipeline(category_key: str):
    category_slug = RETAILER_CONFIG["a101"]["categories"][category_key]

    products = get_a101_category_products(category_slug)

    print("A101 products:", products)
