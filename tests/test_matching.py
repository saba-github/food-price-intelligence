from pipeline.optimizer.matching import normalize_input


def test_normalize_input_uses_product_standardization():
    assert normalize_input("Muz İthal 1 Kg") == "muz"


def test_normalize_input_matches_loose_produce_variants():
    assert normalize_input("Soğan Kuru Dökme Kg") == "sogan"


def test_normalize_input_resolves_sogan_kuru_to_canonical_sogan():
    assert normalize_input("so\u011fan kuru") == "sogan"


def test_normalize_input_resolves_hiyar_to_salatalik():
    assert normalize_input("hıyar") == "salatalik"
