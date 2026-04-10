from scraper.a101.scraper import get_a101_products


def get_a101_category_products(category_slug: str):
    return get_a101_products(category_slug)