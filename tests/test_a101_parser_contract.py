from pathlib import Path

from scraper.a101.scraper import parse_products_from_body_text

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "a101"


def _read_fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


def test_a101_parser_non_empty_extraction():
    body_text = _read_fixture("body_meyve_basic.txt")

    products = parse_products_from_body_text(body_text, "taze-meyve-sebze/meyve")

    assert len(products) >= 2
    assert products[0]["product_name"] == "Elma 1 Kg"


def test_a101_parser_deduplicates_product_names():
    body_text = _read_fixture("body_meyve_dedup.txt")

    products = parse_products_from_body_text(body_text, "taze-meyve-sebze/meyve")

    names = [p["product_name"] for p in products]
    assert names.count("Çilek 250 g") == 1


def test_a101_parser_price_parsing_correctness():
    body_text = _read_fixture("body_meyve_basic.txt")

    products = parse_products_from_body_text(body_text, "taze-meyve-sebze/meyve")

    apple = next(p for p in products if p["product_name"] == "Elma 1 Kg")
    banana = next(p for p in products if p["product_name"] == "Muz 500 g")

    assert apple["shown_price_tl"] == 39.95
    assert banana["shown_price_tl"] == 54.5


def test_a101_parser_graceful_empty_when_section_missing():
    body_text = _read_fixture("body_section_missing.txt")

    products = parse_products_from_body_text(body_text, "taze-meyve-sebze/meyve")

    assert products == []
