from pathlib import Path

from scraper.a101.scraper import (
    extract_unit_info,
    is_valid_product_name,
    parse_price_from_lines,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "a101"


def load_fixture_lines(name: str) -> list[str]:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8").splitlines()


def load_fixture_text(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8").strip()


def test_parse_price_from_utf8_fixture():
    lines = load_fixture_lines("card_utf8.txt")
    assert parse_price_from_lines(lines) == 79.95


def test_parse_price_from_mojibake_fixture():
    lines = load_fixture_lines("card_mojibake.txt")
    assert parse_price_from_lines(lines) == 79.95


def test_parse_price_without_currency_indicator():
    assert parse_price_from_lines(["Ithal Muz 1 kg", "79,95"]) == 79.95


def test_parse_price_from_split_integer_and_fraction_lines():
    assert parse_price_from_lines(["Ithal Muz 1 kg", "79", "95"]) == 79.95


def test_extract_unit_info_from_utf8_fixture():
    product_name = load_fixture_lines("card_utf8.txt")[0]
    assert extract_unit_info(product_name) == ("KG", 1.0)


def test_is_valid_product_name_rejects_banner_fixture():
    banner_text = load_fixture_text("banner_utf8.txt")
    assert is_valid_product_name(banner_text) is False
