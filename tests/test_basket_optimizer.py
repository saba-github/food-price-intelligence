from decimal import Decimal

from pipeline.optimizer.engine import optimize_basket


def cross_row(
    product_name,
    a101_price,
    migros_price,
    cheaper_source,
    same_unit=True,
    same_quantity=True,
    confidence="high",
    a101_unit="kg",
    migros_unit="kg",
    a101_quantity=1,
    migros_quantity=1,
    a101_source_product_name=None,
    migros_source_product_name=None,
):
    return (
        product_name,
        product_name,
        a101_price,
        migros_price,
        cheaper_source,
        None,
        same_unit,
        same_quantity,
        confidence,
        a101_source_product_name or product_name,
        migros_source_product_name or product_name,
        a101_unit,
        migros_unit,
        a101_quantity,
        migros_quantity,
    )


def latest_row(
    product_name,
    a101_price,
    migros_price,
    confidence="single_source",
    a101_unit=None,
    migros_unit="kg",
    a101_quantity=None,
    migros_quantity=1,
):
    return (
        product_name,
        product_name,
        a101_price,
        migros_price,
        None,
        False,
        False,
        confidence,
        product_name if a101_price is not None else None,
        product_name if migros_price is not None else None,
        a101_unit,
        migros_unit,
        a101_quantity,
        migros_quantity,
    )


class FakeCursor:
    def __init__(self, cross_compare_rows, latest_price_rows=None):
        self.cross_compare_rows = cross_compare_rows
        self.latest_price_rows = latest_price_rows or []
        self.requested_products = []
        self.current_rows = []

    def execute(self, sql, params=None):
        self.requested_products = params[0] if params else []
        source_rows = (
            self.cross_compare_rows
            if "mart_cross_compare" in sql
            else self.latest_price_rows
        )
        if self.requested_products:
            self.current_rows = [
                row
                for row in source_rows
                if row[0] in self.requested_products
            ]
        else:
            self.current_rows = source_rows

    def fetchall(self):
        return self.current_rows


def test_optimize_basket_uses_mart_cross_compare_for_single_and_mixed_baskets():
    cursor = FakeCursor(
        [
            cross_row(
                "domates",
                Decimal("100.00"),
                Decimal("120.00"),
                "a101",
            ),
            cross_row(
                "avokado",
                Decimal("40.00"),
                Decimal("35.00"),
                "migros",
            ),
        ]
    )

    result = optimize_basket(cursor, ["domates", "avokado"])

    assert result["cheapest_single_retailer_basket"]["retailer"] == "a101"
    assert result["cheapest_single_retailer_basket"]["total_price"] == Decimal("140.00")
    assert result["cheapest_mixed_basket"]["total_price"] == Decimal("135.00")
    assert result["savings_amount"] == Decimal("5.00")
    assert [
        item["recommended_retailer"]
        for item in result["per_product_recommendations"]
    ] == ["a101", "migros"]


def test_optimize_basket_reports_products_missing_from_cross_compare():
    cursor = FakeCursor(
        [
            cross_row(
                "domates",
                Decimal("100.00"),
                Decimal("120.00"),
                "a101",
            ),
        ]
    )

    result = optimize_basket(cursor, ["domates", "olmayan urun"])

    assert result["unmatched_products"] == ["olmayan urun"]
    assert result["unavailable_products"] == [
        {
            "standardized_product_name": "olmayan urun",
            "reason": "not_found_in_price_history",
        }
    ]
    assert result["matched_products"][1]["match_type"] == "not_found_in_price_history"
    assert result["cheapest_mixed_basket"]["total_price"] == Decimal("100.00")


def test_optimize_basket_includes_single_source_products_in_optimized_total():
    cursor = FakeCursor(
        cross_compare_rows=[
            cross_row(
                "domates",
                Decimal("100.00"),
                Decimal("120.00"),
                "a101",
            ),
        ],
        latest_price_rows=[
            latest_row(
                "kereviz",
                None,
                Decimal("55.00"),
            ),
        ],
    )

    result = optimize_basket(cursor, ["domates", "kereviz", "olmayan urun"])

    assert result["optimized_basket_total"] == Decimal("155.00")
    assert result["single_source_only_products"] == [
        {
            "standardized_product_name": "kereviz",
            "canonical_name": "kereviz",
            "retailer": "migros",
            "availability_status": "only_available_at_migros",
            "price": Decimal("55.00"),
            "a101_price": None,
            "migros_price": Decimal("55.00"),
            "a101_source_product_name": None,
            "migros_source_product_name": "kereviz",
            "a101_normalized_unit": None,
            "migros_normalized_unit": "kg",
            "a101_normalized_quantity": None,
            "migros_normalized_quantity": 1,
            "a101_measurement_label": "-",
            "migros_measurement_label": "1 kg",
        }
    ]
    assert result["comparable_products"] == [
        {
            "standardized_product_name": "domates",
            "canonical_name": "domates",
            "a101_price": Decimal("100.00"),
            "migros_price": Decimal("120.00"),
            "cheaper_source": "a101",
            "comparison_confidence": "high",
            "a101_source_product_name": "domates",
            "migros_source_product_name": "domates",
            "a101_normalized_unit": "kg",
            "migros_normalized_unit": "kg",
            "a101_normalized_quantity": 1,
            "migros_normalized_quantity": 1,
            "a101_measurement_label": "1 kg",
            "migros_measurement_label": "1 kg",
        }
    ]
    assert result["unavailable_products"][0]["standardized_product_name"] == "olmayan urun"
    assert [
        item["recommended_retailer"]
        for item in result["per_product_recommendations"]
    ] == ["a101", "migros"]


def test_optimize_basket_excludes_review_required_comparisons_from_totals():
    cursor = FakeCursor(
        [
            cross_row(
                "domates",
                Decimal("100.00"),
                Decimal("120.00"),
                "a101",
            ),
            cross_row(
                "mersini yaban",
                Decimal("129.50"),
                Decimal("169.95"),
                "a101",
                same_unit=False,
                same_quantity=False,
                confidence="low",
                a101_unit="kg",
                migros_unit="piece",
                a101_quantity=Decimal("0.125"),
                migros_quantity=1,
                a101_source_product_name="Yaban Mersini Paket 125 G",
                migros_source_product_name="Yaban Mersini 125 G Paket",
            ),
        ]
    )

    result = optimize_basket(cursor, ["domates", "yaban mersini"])

    assert result["cheapest_mixed_basket"]["total_price"] == Decimal("100.00")
    assert result["cheapest_single_retailer_basket"] is None
    assert result["suspicious_comparison_products"] == [
        {
            "standardized_product_name": "mersini yaban",
            "canonical_name": "mersini yaban",
            "a101_price": Decimal("129.50"),
            "migros_price": Decimal("169.95"),
            "same_unit_flag": False,
            "same_quantity_flag": False,
            "comparison_confidence": "low",
            "comparison_review_reason": "unit_mismatch",
            "a101_source_product_name": "Yaban Mersini Paket 125 G",
            "migros_source_product_name": "Yaban Mersini 125 G Paket",
            "a101_normalized_unit": "kg",
            "migros_normalized_unit": "piece",
            "a101_normalized_quantity": Decimal("0.125"),
            "migros_normalized_quantity": 1,
            "a101_measurement_label": "125 g paket",
            "migros_measurement_label": "1 adet",
        }
    ]
    assert result["per_product_recommendations"][1]["recommended_retailer"] is None
    assert (
        result["per_product_recommendations"][1]["availability_status"]
        == "comparison_review_required"
    )


def test_optimize_basket_picks_one_cheapest_valid_product_from_family():
    cursor = FakeCursor(
        [
            cross_row(
                "salatalik",
                Decimal("39.90"),
                Decimal("44.90"),
                "a101",
            ),
            cross_row(
                "hiyar",
                Decimal("34.90"),
                Decimal("32.50"),
                "migros",
            ),
            cross_row(
                "badem hiyar",
                Decimal("9.90"),
                Decimal("10.90"),
                "a101",
                same_unit=False,
                same_quantity=False,
                confidence="low",
                a101_unit="piece",
                migros_unit="kg",
            ),
            cross_row(
                "cengelkoy salatalik",
                Decimal("49.90"),
                Decimal("47.50"),
                "migros",
            ),
        ]
    )

    result = optimize_basket(
        cursor,
        [
            {
                "type": "product_family",
                "family_id": "salatalik",
                "family_label": "Salatalık",
                "product_names": [
                    "salatalik",
                    "hiyar",
                    "badem hiyar",
                    "cengelkoy salatalik",
                ],
            }
        ],
    )

    assert result["standardized_products"] == ["salatalik"]
    assert len(result["per_product_recommendations"]) == 1
    assert (
        result["per_product_recommendations"][0]["standardized_product_name"]
        == "salatalik"
    )
    assert result["per_product_recommendations"][0]["family_label"] == "Salatalık"
    assert result["per_product_recommendations"][0]["family_option_count"] == 4
    assert result["per_product_recommendations"][0]["recommended_retailer"] == "a101"
    assert result["cheapest_mixed_basket"]["total_price"] == Decimal("39.90")


def test_optimize_basket_prefers_comparable_row_over_cheaper_single_source_within_family():
    cursor = FakeCursor(
        [
            cross_row(
                "aycicek yagi 1 l",
                Decimal("79.90"),
                Decimal("84.90"),
                "a101",
                a101_unit="liter",
                migros_unit="liter",
                a101_quantity=Decimal("1.0"),
                migros_quantity=Decimal("1.0"),
                a101_source_product_name="Vera Ayçiçek Yağı 1 L",
                migros_source_product_name="Migros Ayçiçek Yağı 1 L",
            ),
            cross_row(
                "aycicek yagi 2 l",
                Decimal("130.00"),
                None,
                "a101",
                confidence="single_source",
                a101_unit="liter",
                migros_unit=None,
                a101_quantity=Decimal("2.0"),
                migros_quantity=None,
                a101_source_product_name="Komili Ayçiçek Yağı 2 L",
                migros_source_product_name=None,
            ),
        ]
    )

    result = optimize_basket(
        cursor,
        [
            {
                "type": "product_family",
                "family_id": "oil_aycicek",
                "family_label": "Aycicek yagi",
                "product_names": ["aycicek yagi 1 l", "aycicek yagi 2 l"],
            }
        ],
    )

    assert result["standardized_products"] == ["aycicek yagi 1 l"]
    assert result["per_product_recommendations"][0]["standardized_product_name"] == "aycicek yagi 1 l"


def test_optimize_basket_marks_force_review_family_as_review_required():
    cursor = FakeCursor(
        [
            cross_row(
                "zeytinyagi 1 l",
                Decimal("299.00"),
                Decimal("322.95"),
                "a101",
                a101_unit="liter",
                migros_unit="liter",
                a101_quantity=Decimal("1.0"),
                migros_quantity=Decimal("1.0"),
                a101_source_product_name="Yudum Egemden Riviera Zeytinyağı 1 L",
                migros_source_product_name="Yudum Egemden Riviera Zeytinyağı 1 L",
            )
        ]
    )

    result = optimize_basket(
        cursor,
        [
            {
                "type": "product_family",
                "family_id": "oil_zeytinyagi",
                "family_label": "Zeytinyagi",
                "product_names": ["zeytinyagi 1 l"],
                "force_review": True,
            }
        ],
    )

    recommendation = result["per_product_recommendations"][0]
    assert recommendation["coverage_status"] == "comparison_review_required"
    assert recommendation["force_review"] is True
    assert recommendation["comparison_review_reason"] == "subtype_selection_required"


def test_optimize_basket_uses_canonicalized_latest_prices_for_loose_sogan():
    cursor = FakeCursor(
        cross_compare_rows=[],
        latest_price_rows=[
            (
                "sogan",
                "sogan",
                Decimal("24.90"),
                Decimal("15.50"),
                None,
                True,
                True,
                "high",
                "So\u011fan Kg",
                "So\u011fan Kuru D\u00f6kme Kg",
                "kg",
                "kg",
                Decimal("1.0"),
                Decimal("1.0"),
            )
        ],
    )

    result = optimize_basket(cursor, ["so\u011fan"])

    assert result["matched_products"] == [
        {
            "input": "sogan",
            "normalized_input": "sogan",
            "product_id": None,
            "found": True,
            "match_type": "latest_price_history_exact",
            "standardized_product_name": "sogan",
            "available_markets": ["a101", "migros"],
            "coverage_status": "comparable",
            "same_unit_flag": True,
            "same_quantity_flag": True,
            "comparison_confidence": "high",
            "comparison_review_reason": None,
            "a101_source_product_name": "So\u011fan Kg",
            "migros_source_product_name": "So\u011fan Kuru D\u00f6kme Kg",
            "a101_normalized_unit": "kg",
            "migros_normalized_unit": "kg",
            "a101_normalized_quantity": Decimal("1.0"),
            "migros_normalized_quantity": Decimal("1.0"),
            "a101_measurement_label": "1 kg",
            "migros_measurement_label": "1 kg",
        }
    ]
    assert result["per_product_recommendations"] == [
        {
            "standardized_product_name": "sogan",
            "canonical_name": "sogan",
            "recommended_retailer": "migros",
            "recommended_price": Decimal("15.50"),
            "a101_price": Decimal("24.90"),
            "migros_price": Decimal("15.50"),
            "availability_status": "ok",
            "coverage_status": "comparable",
            "same_unit_flag": True,
            "same_quantity_flag": True,
            "comparison_confidence": "high",
            "comparison_review_reason": None,
            "a101_source_product_name": "So\u011fan Kg",
            "migros_source_product_name": "So\u011fan Kuru D\u00f6kme Kg",
            "a101_normalized_unit": "kg",
            "migros_normalized_unit": "kg",
            "a101_normalized_quantity": Decimal("1.0"),
            "migros_normalized_quantity": Decimal("1.0"),
            "a101_measurement_label": "1 kg",
            "migros_measurement_label": "1 kg",
        }
    ]


def test_optimize_basket_uses_canonicalized_latest_prices_for_cucumber():
    cursor = FakeCursor(
        cross_compare_rows=[],
        latest_price_rows=[
            (
                "salatalik",
                "salatalik",
                Decimal("45.90"),
                Decimal("39.95"),
                None,
                True,
                True,
                "high",
                "Salatalık Kg",
                "Hıyar Kg",
                "kg",
                "kg",
                Decimal("1.0"),
                Decimal("1.0"),
            )
        ],
    )

    result = optimize_basket(cursor, ["salatalık"])

    assert result["matched_products"] == [
        {
            "input": "salatalik",
            "normalized_input": "salatalik",
            "product_id": None,
            "found": True,
            "match_type": "latest_price_history_exact",
            "standardized_product_name": "salatalik",
            "available_markets": ["a101", "migros"],
            "coverage_status": "comparable",
            "same_unit_flag": True,
            "same_quantity_flag": True,
            "comparison_confidence": "high",
            "comparison_review_reason": None,
            "a101_source_product_name": "Salatalık Kg",
            "migros_source_product_name": "Hıyar Kg",
            "a101_normalized_unit": "kg",
            "migros_normalized_unit": "kg",
            "a101_normalized_quantity": Decimal("1.0"),
            "migros_normalized_quantity": Decimal("1.0"),
            "a101_measurement_label": "1 kg",
            "migros_measurement_label": "1 kg",
        }
    ]
    assert result["per_product_recommendations"][0]["recommended_retailer"] == "migros"
    assert result["per_product_recommendations"][0]["a101_source_product_name"] == "Salatalık Kg"
    assert result["per_product_recommendations"][0]["migros_source_product_name"] == "Hıyar Kg"


def test_optimize_basket_treats_regular_mushroom_packages_as_comparable():
    cursor = FakeCursor(
        cross_compare_rows=[],
        latest_price_rows=[
            (
                "mantar",
                "mantar",
                Decimal("64.90"),
                Decimal("87.95"),
                None,
                True,
                True,
                "high",
                "Mantar Tabak 300 G",
                "Kültür Mantarı 400 G Paket",
                "kg",
                "kg",
                Decimal("0.3"),
                Decimal("0.4"),
            )
        ],
    )

    result = optimize_basket(cursor, ["mantar"])

    assert result["matched_products"] == [
        {
            "input": "mantar",
            "normalized_input": "mantar",
            "product_id": None,
            "found": True,
            "match_type": "latest_price_history_exact",
            "standardized_product_name": "mantar",
            "available_markets": ["a101", "migros"],
            "coverage_status": "comparable",
            "same_unit_flag": True,
            "same_quantity_flag": True,
            "comparison_confidence": "high",
            "comparison_review_reason": None,
            "a101_source_product_name": "Mantar Tabak 300 G",
            "migros_source_product_name": "Kültür Mantarı 400 G Paket",
            "a101_normalized_unit": "kg",
            "migros_normalized_unit": "kg",
            "a101_normalized_quantity": Decimal("0.3"),
            "migros_normalized_quantity": Decimal("0.4"),
            "a101_measurement_label": "300 g paket",
            "migros_measurement_label": "400 g paket",
        }
    ]


def test_optimize_basket_prefers_exact_size_specific_candidate_over_generic_review_row():
    cursor = FakeCursor(
        cross_compare_rows=[],
        latest_price_rows=[
            (
                "kola pepsi",
                "kola pepsi",
                Decimal("57.00"),
                Decimal("57.00"),
                None,
                False,
                False,
                "low",
                "Pepsi Kola 1 L",
                "Pepsi Kola 1 L",
                "liter",
                "piece",
                Decimal("1.0"),
                Decimal("1.0"),
            ),
            (
                "kola pepsi 1 l",
                "kola pepsi 1 l",
                Decimal("57.00"),
                Decimal("57.00"),
                None,
                True,
                True,
                "high",
                "Pepsi Kola 1 L",
                "Pepsi Kola 1 L",
                "liter",
                "liter",
                Decimal("1.0"),
                Decimal("1.0"),
            ),
        ],
    )

    result = optimize_basket(cursor, ["kola pepsi 1 l"])

    assert result["matched_products"][0]["standardized_product_name"] == "kola pepsi 1 l"
    assert result["matched_products"][0]["coverage_status"] == "comparable"
    assert result["per_product_recommendations"][0]["coverage_status"] == "comparable"


def test_optimize_basket_uses_piece_normalized_egg_pair():
    cursor = FakeCursor(
        cross_compare_rows=[],
        latest_price_rows=[
            (
                "yumurta 15 adet",
                "yumurta 15 adet",
                Decimal("79.00"),
                Decimal("79.50"),
                None,
                True,
                True,
                "high",
                "Yumurta L Boy 15'li",
                "Keskinoglu 15'li L Buyuk Boy Yumurta",
                "piece",
                "piece",
                Decimal("15"),
                Decimal("15"),
            )
        ],
    )

    result = optimize_basket(cursor, ["yumurta 15 adet"])

    assert result["matched_products"][0]["standardized_product_name"] == "yumurta 15 adet"
    assert result["matched_products"][0]["coverage_status"] == "comparable"
    assert result["matched_products"][0]["a101_normalized_unit"] == "piece"
    assert result["matched_products"][0]["migros_normalized_unit"] == "piece"
    assert result["matched_products"][0]["a101_normalized_quantity"] == Decimal("15")
    assert result["matched_products"][0]["migros_normalized_quantity"] == Decimal("15")
    assert result["per_product_recommendations"][0]["recommended_retailer"] == "a101"


def test_optimize_basket_pairs_base_ekmek_with_sofra_ekmek_as_review_required():
    cursor = FakeCursor(
        cross_compare_rows=[],
        latest_price_rows=[
            (
                "ekmek",
                "ekmek",
                Decimal("5.00"),
                Decimal("20.00"),
                None,
                False,
                False,
                "low",
                "Ekmek 200 G",
                "Sofra Ekmek Adet",
                "kg",
                "piece",
                Decimal("0.2"),
                Decimal("1.0"),
            )
        ],
    )

    result = optimize_basket(cursor, ["ekmek"])

    assert result["matched_products"] == [
        {
            "input": "ekmek",
            "normalized_input": "ekmek",
            "product_id": None,
            "found": True,
            "match_type": "comparison_review_required",
            "standardized_product_name": "ekmek",
            "available_markets": ["a101", "migros"],
            "coverage_status": "comparison_review_required",
            "same_unit_flag": False,
            "same_quantity_flag": False,
            "comparison_confidence": "low",
            "comparison_review_reason": "unit_mismatch",
            "a101_source_product_name": "Ekmek 200 G",
            "migros_source_product_name": "Sofra Ekmek Adet",
            "a101_normalized_unit": "kg",
            "migros_normalized_unit": "piece",
            "a101_normalized_quantity": Decimal("0.2"),
            "migros_normalized_quantity": Decimal("1.0"),
            "a101_measurement_label": "200 g",
            "migros_measurement_label": "1 adet",
        }
    ]
    assert result["per_product_recommendations"][0]["a101_source_product_name"] == "Ekmek 200 G"
    assert (
        result["per_product_recommendations"][0]["migros_source_product_name"]
        == "Sofra Ekmek Adet"
    )
    assert (
        result["per_product_recommendations"][0]["coverage_status"]
        == "comparison_review_required"
    )


def test_optimize_basket_matches_explicit_tava_ekmek_row_using_raw_query_fallback():
    cursor = FakeCursor(
        cross_compare_rows=[],
        latest_price_rows=[
            (
                "tava ekmek",
                "tava ekmek",
                Decimal("85.00"),
                Decimal("75.00"),
                None,
                True,
                True,
                "high",
                "Uno Tam Buğday Unlu Tava Ekmeği 450 G",
                "Uno %100 Tam Buğday Tava Ekmeği 450 G",
                "kg",
                "kg",
                Decimal("0.45"),
                Decimal("0.45"),
            )
        ],
    )

    result = optimize_basket(cursor, ["tava ekmek"])

    assert result["matched_products"] == [
        {
            "input": "tava ekmek",
            "normalized_input": "tava ekmek",
            "product_id": None,
            "found": True,
            "match_type": "latest_price_history_exact",
            "standardized_product_name": "tava ekmek",
            "available_markets": ["a101", "migros"],
            "coverage_status": "comparable",
            "same_unit_flag": True,
            "same_quantity_flag": True,
            "comparison_confidence": "high",
            "comparison_review_reason": None,
            "a101_source_product_name": "Uno Tam Buğday Unlu Tava Ekmeği 450 G",
            "migros_source_product_name": "Uno %100 Tam Buğday Tava Ekmeği 450 G",
            "a101_normalized_unit": "kg",
            "migros_normalized_unit": "kg",
            "a101_normalized_quantity": Decimal("0.45"),
            "migros_normalized_quantity": Decimal("0.45"),
            "a101_measurement_label": "450 g",
            "migros_measurement_label": "450 g",
        }
    ]
    assert result["per_product_recommendations"][0]["recommended_retailer"] == "migros"


def test_optimize_basket_keeps_same_brand_same_line_toilet_paper_comparable():
    cursor = FakeCursor(
        cross_compare_rows=[],
        latest_price_rows=[
            (
                "papia tuvalet kagidi 16 roll",
                "papia tuvalet kagidi 16 roll",
                Decimal("299.00"),
                Decimal("329.95"),
                None,
                True,
                True,
                "high",
                "Papia Platinum Tuvalet Kagidi 16'li",
                "Papia Platinum 4 Katli Tuvalet Kagidi 16'li",
                "roll",
                "roll",
                Decimal("16"),
                Decimal("16"),
            )
        ],
    )

    result = optimize_basket(cursor, ["papia tuvalet kagidi 16 roll"])

    assert result["matched_products"][0]["coverage_status"] == "comparable"
    assert result["matched_products"][0]["comparison_confidence"] == "high"
    assert result["matched_products"][0]["comparison_review_reason"] is None


def test_optimize_basket_downgrades_toilet_paper_when_product_line_differs():
    cursor = FakeCursor(
        cross_compare_rows=[],
        latest_price_rows=[
            (
                "papia tuvalet kagidi 16 roll",
                "papia tuvalet kagidi 16 roll",
                Decimal("299.00"),
                Decimal("329.95"),
                None,
                True,
                True,
                "high",
                "Papia Egzotik 3 Katli Tuvalet Kagidi 16'li",
                "Papia Platinum 4 Katli Tuvalet Kagidi 16'li",
                "roll",
                "roll",
                Decimal("16"),
                Decimal("16"),
            )
        ],
    )

    result = optimize_basket(cursor, ["papia tuvalet kagidi 16 roll"])

    assert result["matched_products"][0]["coverage_status"] == "comparison_review_required"
    assert result["matched_products"][0]["comparison_confidence"] == "medium"
    assert result["matched_products"][0]["comparison_review_reason"] == "product_line_mismatch"


def test_optimize_basket_downgrades_toilet_paper_when_brand_differs():
    cursor = FakeCursor(
        cross_compare_rows=[],
        latest_price_rows=[
            (
                "tuvalet kagidi 16 roll",
                "tuvalet kagidi 16 roll",
                Decimal("299.00"),
                Decimal("329.95"),
                None,
                True,
                True,
                "high",
                "Papia Platinum Tuvalet Kagidi 16'li",
                "Solo Bambu Katkili Tuvalet Kagidi 16'li",
                "roll",
                "roll",
                Decimal("16"),
                Decimal("16"),
            )
        ],
    )

    result = optimize_basket(cursor, ["tuvalet kagidi 16 roll"])

    assert result["matched_products"][0]["coverage_status"] == "comparison_review_required"
    assert result["matched_products"][0]["comparison_review_reason"] == "brand_mismatch"


def test_optimize_basket_keeps_same_brand_same_line_paper_towel_comparable():
    cursor = FakeCursor(
        cross_compare_rows=[],
        latest_price_rows=[
            (
                "papia kagit havlu 8 roll",
                "papia kagit havlu 8 roll",
                Decimal("26.25"),
                Decimal("23.74"),
                None,
                True,
                True,
                "high",
                "Papia BioCare 3 Katlı Kağıt Havlu 8'li",
                "Papia Biocare Havlu 8'li",
                "roll",
                "roll",
                Decimal("8"),
                Decimal("8"),
            )
        ],
    )

    result = optimize_basket(cursor, ["papia kagit havlu 8 roll"])

    assert result["matched_products"][0]["coverage_status"] == "comparable"
    assert result["matched_products"][0]["comparison_confidence"] == "high"
    assert result["matched_products"][0]["comparison_review_reason"] is None


def test_optimize_basket_downgrades_paper_towel_when_product_line_is_unclear():
    cursor = FakeCursor(
        cross_compare_rows=[],
        latest_price_rows=[
            (
                "papia kagit havlu 6 roll",
                "papia kagit havlu 6 roll",
                Decimal("21.65"),
                Decimal("24.99"),
                None,
                True,
                True,
                "high",
                "Papia 3 Katlı Kağıt Havlu 6'lı",
                "Papia Biocare Kağıt Havlu 6'lı",
                "roll",
                "roll",
                Decimal("6"),
                Decimal("6"),
            )
        ],
    )

    result = optimize_basket(cursor, ["papia kagit havlu 6 roll"])

    assert result["matched_products"][0]["coverage_status"] == "comparison_review_required"
    assert result["matched_products"][0]["comparison_confidence"] == "medium"
    assert result["matched_products"][0]["comparison_review_reason"] == "product_line_unknown"


def test_optimize_basket_downgrades_paper_towel_when_brand_differs():
    cursor = FakeCursor(
        cross_compare_rows=[],
        latest_price_rows=[
            (
                "kagit havlu 12 roll",
                "kagit havlu 12 roll",
                Decimal("19.99"),
                Decimal("18.50"),
                None,
                True,
                True,
                "high",
                "Aqua Bambu 3 Katlı Kağıt Havlu 12'li",
                "Familia Plus Natural Havlu 12'li",
                "roll",
                "roll",
                Decimal("12"),
                Decimal("12"),
            )
        ],
    )

    result = optimize_basket(cursor, ["kagit havlu 12 roll"])

    assert result["matched_products"][0]["coverage_status"] == "comparison_review_required"
    assert result["matched_products"][0]["comparison_review_reason"] == "brand_mismatch"


def test_optimize_basket_prefers_latest_high_confidence_row_over_stale_cross_compare_status():
    cursor = FakeCursor(
        cross_compare_rows=[
            cross_row(
                "mantar",
                Decimal("54.90"),
                Decimal("79.95"),
                "a101",
                same_unit=True,
                same_quantity=False,
                confidence="medium",
                a101_unit="kg",
                migros_unit="kg",
                a101_quantity=Decimal("0.3"),
                migros_quantity=Decimal("0.4"),
                a101_source_product_name="Mantar Tabak 300 G",
                migros_source_product_name="Kültür Mantarı 400 G Paket",
            )
        ],
        latest_price_rows=[
            (
                "mantar",
                "mantar",
                Decimal("54.90"),
                Decimal("79.95"),
                None,
                True,
                True,
                "high",
                "Mantar Tabak 300 G",
                "Kültür Mantarı 400 G Paket",
                "kg",
                "kg",
                Decimal("0.3"),
                Decimal("0.4"),
            )
        ],
    )

    result = optimize_basket(cursor, ["mantar"])

    assert result["matched_products"][0]["coverage_status"] == "comparable"
    assert result["matched_products"][0]["comparison_confidence"] == "high"
    assert result["matched_products"][0]["comparison_review_reason"] is None
    assert result["per_product_recommendations"][0]["coverage_status"] == "comparable"
    assert result["per_product_recommendations"][0]["comparison_confidence"] == "high"
