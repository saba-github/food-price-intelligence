from pipeline.transforms import (
    calculate_price_per_unit,
    canonicalize_produce_name,
    detect_suspicious,
    infer_paper_product_profile,
    infer_roll_measurement_from_name,
    normalize_text,
    normalize_unit,
    standardize_product_name,
    transform_product,
)


def test_normalize_unit_gram_to_kg():
    unit, qty = normalize_unit("GRAM", 400)
    assert unit == "kg"
    assert qty == 0.4


def test_normalize_unit_piece_default():
    unit, qty = normalize_unit("PIECE", None)
    assert unit == "piece"
    assert qty == 1.0


def test_standardize_product_name():
    result = standardize_product_name("Kültür Mantarı 400 G Paket")
    assert result == "mantar"


def test_standardize_product_name_handles_turkish_dotted_i():
    result = standardize_product_name("Muz İthal Kg")
    assert result == "muz"


def test_normalize_text_transliterates_turkish_toilet_paper_query():
    assert normalize_text("Tuvalet Kağıdı") == "tuvalet kagidi"


def test_canonicalize_produce_name_matches_loose_onion_across_retailers():
    assert canonicalize_produce_name("Soğan Kg") == "sogan"
    assert canonicalize_produce_name("Soğan Kuru Dökme Kg") == "sogan"


def test_canonicalize_produce_name_removes_common_loose_produce_descriptors():
    assert canonicalize_produce_name("Domates Kg") == "domates"
    assert canonicalize_produce_name("Domates Salkım Kg") == "domates"
    assert canonicalize_produce_name("Patates Kg") == "patates"
    assert canonicalize_produce_name("Patates Taze Kg") == "patates"
    assert canonicalize_produce_name("Salatalık Kg") == "salatalik"
    assert canonicalize_produce_name("Salatalık Paket") == "salatalik"
    assert canonicalize_produce_name("Hıyar Kg") == "salatalik"


def test_canonicalize_produce_name_keeps_specific_pepper_variant():
    assert canonicalize_produce_name("Çarliston Biber Paket 300 G") == (
        "carliston biber"
    )
    assert standardize_product_name("Çarliston Biber Paket 300 G") == (
        "biber carliston"
    )


def test_canonicalize_produce_name_groups_regular_mushroom_as_base_mantar():
    assert canonicalize_produce_name("Mantar Tabak 300 G") == "mantar"
    assert canonicalize_produce_name("Kültür Mantarı 400 G Paket") == "mantar"
    assert standardize_product_name("Kültür Mantarı 400 G Paket") == "mantar"


def test_canonicalize_produce_name_keeps_mushroom_variants_separate():
    assert canonicalize_produce_name("Mantar İstiridye 200 G") == "istiridye mantar"
    assert canonicalize_produce_name("Mantar Kestane Paket 350 G") == "kestane mantar"
    assert canonicalize_produce_name("Müpa Mantar Shiitake 200 G") == "shiitake mantar"
    assert canonicalize_produce_name("Mantar Izgaralık 500 G") == "izgaralik mantar"


def test_calculate_price_per_unit():
    result = calculate_price_per_unit(80.0, 0.4)
    assert result == 200.0


def test_detect_suspicious_price_too_high():
    is_suspicious, reason = detect_suspicious("Domates", 600)
    assert is_suspicious is True
    assert reason == "price_too_high"


def test_detect_suspicious_valid_product():
    is_suspicious, reason = detect_suspicious("Domates", 50)
    assert is_suspicious is False
    assert reason is None


def test_transform_product():
    product = {
        "product_name": "Kültür Mantarı 400 G Paket",
        "shown_price_tl": 80.0,
        "regular_price_tl": 90.0,
        "unit": "GRAM",
        "unit_amount": 400,
        "discount_rate": 0.1111,
        "brand_name": "Migros",
        "category_name": "Mantar",
    }

    transformed = transform_product(product)

    assert transformed["canonical_product_name"] == "mantar"
    assert transformed["standardized_product_name"] == "mantar"
    assert transformed["normalized_unit"] == "kg"
    assert transformed["normalized_quantity"] == 0.4
    assert transformed["price_per_unit"] == 200.0
    assert transformed["unit_price_label"] == "TRY/kg"
    assert transformed["brand_name"] == "Migros"
    assert transformed["category_name"] == "Mantar"


def test_transform_product_infers_weight_from_piece_based_regular_mushroom_name():
    product = {
        "product_name": "Kültür Mantarı 400 G Paket",
        "shown_price_tl": 87.95,
        "regular_price_tl": 87.95,
        "unit": "PIECE",
        "unit_amount": 1,
        "discount_rate": 0,
        "brand_name": "Migros",
        "category_name": "Mantar",
    }

    transformed = transform_product(product)

    assert transformed["canonical_product_name"] == "mantar"
    assert transformed["normalized_unit"] == "kg"
    assert transformed["normalized_quantity"] == 0.4
    assert transformed["price_per_unit"] == 219.875


def test_infer_roll_measurement_from_name_parses_16li_toilet_paper():
    assert infer_roll_measurement_from_name("Papia Platinum Tuvalet Kagidi 16'li") == (
        "roll",
        16.0,
    )


def test_infer_roll_measurement_from_name_parses_turkish_16li_toilet_paper():
    assert infer_roll_measurement_from_name("Papia Platinum Tuvalet Kağıdı 16'lı") == (
        "roll",
        16.0,
    )


def test_infer_roll_measurement_from_name_parses_32li_toilet_paper():
    assert infer_roll_measurement_from_name("Solo Tuvalet Kagidi 32 li") == (
        "roll",
        32.0,
    )


def test_infer_roll_measurement_from_name_parses_multipack_roll_count():
    assert infer_roll_measurement_from_name(
        "Familia Tuvalet Kagidi 3x32"
    ) == ("roll", 96.0)


def test_infer_roll_measurement_from_name_parses_8li_kagit_havlu():
    assert infer_roll_measurement_from_name("Papia Kagit Havlu 8'li") == (
        "roll",
        8.0,
    )


def test_infer_roll_measurement_from_name_parses_turkish_havlu_kagidi():
    assert infer_roll_measurement_from_name("Solo Havlu Kağıdı 8'li") == (
        "roll",
        8.0,
    )


def test_infer_roll_measurement_from_name_parses_equivalent_roll_count():
    assert infer_roll_measurement_from_name(
        "Solo Asilabilir Havlu 1=7 Rulo 325 Yaprak"
    ) == ("roll", 7.0)


def test_infer_roll_measurement_from_name_does_not_treat_yaprak_count_as_roll():
    assert infer_roll_measurement_from_name("Papia Home Asmali Havlu 250'li") == (
        None,
        None,
    )


def test_infer_roll_measurement_from_name_does_not_treat_el_havlusu_count_as_roll():
    assert infer_roll_measurement_from_name("Sofia El ve Yuz Havlusu 100'lu") == (
        None,
        None,
    )


def test_infer_paper_product_profile_extracts_paper_towel_product_line_tokens():
    profile = infer_paper_product_profile("Papia Biocare Kağıt Havlu 8'li")

    assert profile["kind"] == "paper_towel"
    assert profile["product_line"] == "biocare"
    assert profile["product_line_tokens"] == ("biocare",)


def test_transform_product_normalizes_toilet_paper_as_roll():
    product = {
        "product_name": "Papia Platinum Tuvalet Kagidi 16'li",
        "shown_price_tl": 159.9,
        "regular_price_tl": 179.9,
        "unit": "PIECE",
        "unit_amount": 1,
        "discount_rate": 0.1111,
        "brand_name": "Papia",
        "category_name": "Tuvalet Kagidi",
    }

    transformed = transform_product(product)

    assert transformed["normalized_unit"] == "roll"
    assert transformed["normalized_quantity"] == 16.0
    assert transformed["price_per_unit"] == 9.9938
    assert transformed["unit_price_label"] == "TRY/roll"
