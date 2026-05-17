import pandas as pd
import pytest

from pipeline.optimizer.product_search import (
    build_search_group_sections,
    build_optimizer_input_from_group,
    build_product_family_groups,
    format_product_family_group,
    search_product_catalog,
)


def make_catalog(rows):
    normalized_rows = []
    for row in rows:
        if len(row) == 3:
            product_name, source_count, retailers = row
            coverage_status = "comparable" if source_count > 1 else "single_source"
            normalized_rows.append(
                (product_name, source_count, retailers, coverage_status)
            )
        else:
            normalized_rows.append(row)

    return pd.DataFrame(
        normalized_rows,
        columns=[
            "standardized_product_name",
            "source_count",
            "available_retailers",
            "coverage_status",
        ],
    )


def product_names(search_results):
    return search_results["standardized_product_name"].tolist()


def make_golden_query_catalog():
    return pd.DataFrame(
        [
            {
                "standardized_product_name": "domates",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Domates Kg",
                "migros_source_product_name": "Domates Kg",
            },
            {
                "standardized_product_name": "domates kokteyl",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparison_review_required",
                "a101_source_product_name": "Kokteyl Domates 500 G",
                "migros_source_product_name": "Kokteyl Domates 500 G",
            },
            {
                "standardized_product_name": "zeytinyagi 200 g",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Domatesli Zeytinyagi 200 G",
                "migros_source_product_name": "Domatesli Zeytinyagi 200 G",
            },
            {
                "standardized_product_name": "hiyar",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Salatalik Kg",
                "migros_source_product_name": "Hiyar Kg",
            },
            {
                "standardized_product_name": "sek sut",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Sek Sut 1 L",
                "migros_source_product_name": "Sek Sut 1 L",
            },
            {
                "standardized_product_name": "alpimilk cikolatali sut",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Alpimilk Cikolatali Sut 180 Ml",
            },
            {
                "standardized_product_name": "su 0.5 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Su 0.5 L",
                "migros_source_product_name": "Su 0.5 L",
            },
            {
                "standardized_product_name": "hayat su 1.5 l",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Hayat Su 1.5 L",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "kola coca-cola 1 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Coca-Cola 1 L",
                "migros_source_product_name": "Coca-Cola 1 L",
            },
            {
                "standardized_product_name": "maden suyu 0.2 l",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Maden Suyu 200 Ml",
            },
            {
                "standardized_product_name": "fairy bulasik deterjani 0.65 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Sivi Bulasik Deterjani 650 ml",
                "migros_source_product_name": "Fairy Sivi Bulasik Deterjani Limon 650 Ml",
            },
            {
                "standardized_product_name": "fairy power sprey 0.5 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Power Sprey 500 Ml",
                "migros_source_product_name": "Fairy Power Sprey 500 Ml",
            },
            {
                "standardized_product_name": "30'lu fairy platinum tablet",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Fairy Platinum Bulasik Makinesi Tableti 30'lu",
            },
            {
                "standardized_product_name": "domestos ultra camasir suyu",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Domestos Ultra Camasir Suyu 750 Ml",
                "migros_source_product_name": "Domestos Ultra Camasir Suyu 750 Ml",
            },
            {
                "standardized_product_name": "domestos kopuk mutfak",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Domestos Banyo Mutfak Kopuk 500 Ml",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "solo tuvalet kagidi 16 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Solo Tuvalet Kagidi 16'li",
                "migros_source_product_name": "Solo Tuvalet Kagidi 16'li",
            },
            {
                "standardized_product_name": "finish bulasik makinesi tableti 101 li",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Finish Bulasik Makinesi Tableti 101'li",
                "migros_source_product_name": "Finish Bulasik Makinesi Tableti 101'li",
            },
            {
                "standardized_product_name": "finish bulasik makinesi tuzu 1300 g",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Finish Bulasik Makinesi Tuzu 1300 G",
                "migros_source_product_name": "Finish Bulasik Makinesi Tuzu 1300 G",
            },
        ]
    )


def test_search_uses_aliases_and_synonyms():
    catalog_df = make_catalog(
        [
            ("hiyar", 2, "a101, migros"),
            ("sogan taze", 1, "migros"),
            ("sogan kuru", 1, "a101"),
            ("domates", 2, "a101, migros"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "salatalik")) == ["hiyar"]
    assert product_names(search_product_catalog(catalog_df, "cucumber")) == ["hiyar"]
    assert product_names(search_product_catalog(catalog_df, "taze sogan")) == [
        "sogan kuru",
        "sogan taze",
    ]
    assert product_names(search_product_catalog(catalog_df, "kuru sogan")) == [
        "sogan kuru",
        "sogan taze",
    ]


def test_search_expands_muz_to_local_and_imported_bananas():
    catalog_df = make_catalog(
        [
            ("ithal muz", 1, "a101"),
            ("muz yerli", 2, "a101, migros"),
            ("muz organik", 1, "migros"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "muz")) == [
        "muz yerli",
        "ithal muz",
        "muz organik",
    ]


def test_search_tolerates_typos():
    catalog_df = make_catalog(
        [
            ("hiyar", 2, "a101, migros"),
            ("domates", 2, "a101, migros"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "salatlik")) == ["hiyar"]
    assert product_names(search_product_catalog(catalog_df, "domats")) == ["domates"]


def test_search_prefers_comparable_water_size_for_generic_su():
    catalog_df = pd.DataFrame(
        [
            ("su 0.5 l", 2, "a101, migros", "comparable"),
            ("hayat su 1.5 l", 1, "a101", "only_a101"),
            ("maden suyu 0.2 l", 1, "migros", "only_migros"),
        ],
        columns=[
            "standardized_product_name",
            "source_count",
            "available_retailers",
            "coverage_status",
        ],
    )

    assert product_names(search_product_catalog(catalog_df, "su"))[0] == "su 0.5 l"


def test_search_prefers_comparable_table_salt_for_generic_tuz():
    catalog_df = pd.DataFrame(
        [
            ("tuz 750 g", 2, "a101, migros", "comparable"),
            ("himalaya tuzu 500 g", 1, "migros", "only_migros"),
            ("billur tuz tuzluklu", 1, "a101", "only_a101"),
        ],
        columns=[
            "standardized_product_name",
            "source_count",
            "available_retailers",
            "coverage_status",
        ],
    )

    assert product_names(search_product_catalog(catalog_df, "tuz"))[0] == "tuz 750 g"


def test_search_generic_kola_prefers_regular_bottles_then_normal_sizes():
    catalog_df = pd.DataFrame(
        [
            ("kola coca-cola 1 l", 2, "a101, migros", "comparable", 60.0, 60.0),
            ("kola pepsi 1 l", 2, "a101, migros", "comparable", 57.0, 57.0),
            ("kola coca-cola 1.5 l", 2, "a101, migros", "comparable", 58.0, 58.0),
            ("kola pepsi 1.5 l", 2, "a101, migros", "comparable", 56.0, 56.0),
            ("kola coca-cola light 1 l", 2, "a101, migros", "comparable", 55.0, 55.0),
            ("kola pepsi zero 1 l", 2, "a101, migros", "comparable", 54.0, 54.0),
            ("kola coca-cola kutu 0.33 l", 2, "a101, migros", "comparable", 75.0, 75.0),
            ("kola coca-cola 0.2 l", 2, "a101, migros", "comparable", 95.0, 95.0),
            ("kola coca-cola 2.5 l", 2, "a101, migros", "comparable", 36.0, 36.0),
            ("4x1 kola pepsi", 1, "a101", "only_a101", 45.0, None),
            ("kola kristal 1 l", 1, "a101", "only_a101", 52.0, None),
        ],
        columns=[
            "standardized_product_name",
            "source_count",
            "available_retailers",
            "coverage_status",
            "a101_comparison_price",
            "migros_comparison_price",
        ],
    )

    ranked = product_names(search_product_catalog(catalog_df, "kola"))
    assert ranked[:4] == [
        "kola coca-cola 1 l",
        "kola pepsi 1 l",
        "kola coca-cola 1.5 l",
        "kola pepsi 1.5 l",
    ]
    assert ranked.index("kola coca-cola light 1 l") > ranked.index("kola pepsi 1.5 l")
    assert ranked.index("kola pepsi zero 1 l") > ranked.index("kola pepsi 1.5 l")
    assert ranked.index("kola coca-cola kutu 0.33 l") > ranked.index("kola coca-cola 1.5 l")
    assert ranked.index("4x1 kola pepsi") > ranked.index("kola coca-cola kutu 0.33 l")
    assert ranked.index("kola kristal 1 l") > ranked.index("kola pepsi 1 l")


def test_search_generic_kola_is_deterministic_even_if_row_order_changes():
    rows = [
        ("kola coca-cola 1 l", 2, "a101, migros", "comparable", 60.0, 60.0),
        ("kola pepsi 1 l", 2, "a101, migros", "comparable", 57.0, 57.0),
        ("kola coca-cola 1.5 l", 2, "a101, migros", "comparable", 58.0, 58.0),
        ("kola pepsi 1.5 l", 2, "a101, migros", "comparable", 56.0, 56.0),
        ("kola coca-cola light 1 l", 2, "a101, migros", "comparable", 55.0, 55.0),
    ]
    columns = [
        "standardized_product_name",
        "source_count",
        "available_retailers",
        "coverage_status",
        "a101_comparison_price",
        "migros_comparison_price",
    ]
    catalog_df = pd.DataFrame(rows, columns=columns)
    reversed_df = pd.DataFrame(list(reversed(rows)), columns=columns)

    assert product_names(search_product_catalog(catalog_df, "kola"))[0] == (
        "kola coca-cola 1 l"
    )
    assert product_names(search_product_catalog(reversed_df, "kola"))[0] == (
        "kola coca-cola 1 l"
    )


def test_search_explicit_coca_cola_matches_hyphenated_products():
    catalog_df = make_catalog(
        [
            ("kola coca-cola 1 l", 2, "a101, migros"),
            ("kola pepsi 1 l", 2, "a101, migros"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "coca cola"))[0] == (
        "kola coca-cola 1 l"
    )


def test_search_explicit_coca_cola_zero_allows_zero_variant():
    catalog_df = make_catalog(
        [
            ("coca-cola pet sugar zero", 1, "migros"),
            ("kola coca-cola 1 l", 2, "a101, migros"),
            ("kola pepsi 1 l", 2, "a101, migros"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "coca cola zero"))[0] == (
        "coca-cola pet sugar zero"
    )


def test_search_explicit_coca_cola_light_allows_light_variant():
    catalog_df = make_catalog(
        [
            ("coca-cola light", 1, "a101"),
            ("kola coca-cola 1 l", 2, "a101, migros"),
            ("kola pepsi 1 l", 2, "a101, migros"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "coca cola light"))[0] == (
        "coca-cola light"
    )


def test_search_explicit_pepsi_max_allows_max_variant():
    catalog_df = make_catalog(
        [
            ("kola pepsi 1 l", 2, "a101, migros"),
            ("4x1 max pepsi", 1, "a101"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "pepsi max"))[0] == (
        "4x1 max pepsi"
    )


def test_search_explicit_kola_2_5_l_prefers_matching_volume():
    catalog_df = make_catalog(
        [
            ("kola pepsi 1 l", 2, "a101, migros"),
            ("kola coca-cola 2.5 l", 2, "a101, migros"),
            ("kola pepsi 2.5 l", 2, "a101, migros"),
            ("kola coca-cola light 2.5 l", 2, "a101, migros"),
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "kola 2.5 l"))
    assert ranked[0] in {"kola coca-cola 2.5 l", "kola pepsi 2.5 l"}
    assert ranked.index("kola coca-cola light 2.5 l") > ranked.index(ranked[0])
    assert ranked.index("kola pepsi 1 l") > ranked.index(ranked[0])


def test_search_explicit_kutu_kola_prefers_can_over_bottle():
    catalog_df = make_catalog(
        [
            ("kola coca-cola 1 l", 2, "a101, migros"),
            ("kola coca-cola kutu 0.33 l", 2, "a101, migros"),
            ("4x1 kola pepsi", 1, "a101"),
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "kutu kola"))
    assert ranked[0] == "kola coca-cola kutu 0.33 l"
    assert "kola coca-cola 1 l" not in ranked


def test_search_explicit_coca_cola_prefers_same_brand_and_normal_volume():
    catalog_df = make_catalog(
        [
            ("kola coca-cola 0.2 l", 2, "a101, migros"),
            ("kola coca-cola 1 l", 2, "a101, migros"),
            ("kola pepsi 1 l", 2, "a101, migros"),
            ("kola coca-cola light 1 l", 2, "a101, migros"),
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "coca cola"))
    assert ranked[0] == "kola coca-cola 1 l"
    assert ranked.index("kola coca-cola 0.2 l") > ranked.index("kola coca-cola 1 l")
    assert ranked.index("kola coca-cola light 1 l") > ranked.index(
        "kola coca-cola 1 l"
    )


def test_search_explicit_queries_remain_unchanged_for_cola_variants():
    catalog_df = pd.DataFrame(
        [
            ("kola coca-cola 1 l", 2, "a101, migros", "comparable", 60.0, 60.0),
            ("coca-cola light", 1, "a101", "only_a101", 55.0, None),
            ("4x1 max pepsi", 1, "a101", "only_a101", 110.0, None),
            ("kola coca-cola 2.5 l", 2, "a101, migros", "comparable", 36.0, 36.0),
        ],
        columns=[
            "standardized_product_name",
            "source_count",
            "available_retailers",
            "coverage_status",
            "a101_comparison_price",
            "migros_comparison_price",
        ],
    )

    assert product_names(search_product_catalog(catalog_df, "coca cola light"))[0] == (
        "coca-cola light"
    )
    assert product_names(search_product_catalog(catalog_df, "pepsi max"))[0] == (
        "4x1 max pepsi"
    )
    assert product_names(search_product_catalog(catalog_df, "kola 2.5 l"))[0] == (
        "kola coca-cola 2.5 l"
    )


def test_search_ranks_comparable_and_exact_matches_first():
    catalog_df = make_catalog(
        [
            ("hiyar organik", 2, "a101, migros"),
            ("hiyar", 1, "migros"),
            ("hiyar sera", 1, "migros"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "hiyar")) == [
        "hiyar",
        "hiyar organik",
        "hiyar sera",
    ]


def test_search_promotes_comparable_products_with_same_match_quality():
    catalog_df = make_catalog(
        [
            ("ithal muz", 1, "a101"),
            ("muz yerli", 2, "a101, migros"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "muz")) == [
        "muz yerli",
        "ithal muz",
    ]


def test_search_groups_generic_salatalik_intent_as_one_family():
    catalog_df = make_catalog(
        [
            ("hiyar", 2, "a101, migros"),
            ("salatalik", 2, "a101, migros"),
            ("badem hiyar", 1, "a101"),
            ("cengelkoy salatalik", 1, "migros"),
            ("domates", 2, "a101, migros"),
        ]
    )

    search_results = search_product_catalog(catalog_df, "salatalik")
    groups = build_product_family_groups(search_results)

    assert len(groups) == 1
    assert format_product_family_group(groups[0]) == "Salatal\u0131k (4 se\u00e7enek)"
    assert set(groups[0]["product_names"]) == {
        "salatalik",
        "hiyar",
        "badem hiyar",
        "cengelkoy salatalik",
    }
    assert build_optimizer_input_from_group(groups[0]) == {
        "type": "product_family",
        "family_id": "salatalik",
        "family_label": "Salatal\u0131k",
        "product_names": groups[0]["product_names"],
    }


def test_search_matches_canonical_sogan_group_for_unicode_and_ascii_queries():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "sogan",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "So\u011fan Kg",
                "migros_source_product_name": "So\u011fan Kuru D\u00f6kme Kg",
            },
            {
                "standardized_product_name": "sogan taze",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparison_review_required",
                "a101_source_product_name": "Taze So\u011fan Adet",
                "migros_source_product_name": "So\u011fan Taze Demet",
            },
        ]
    )

    for query in ["so\u011fan", "sogan", "so\u011fan kuru"]:
        results = search_product_catalog(catalog_df, query)
        top_row = results.iloc[0]
        assert top_row["standardized_product_name"] == "sogan"
        assert top_row["a101_source_product_name"] == "So\u011fan Kg"
        assert top_row["migros_source_product_name"] == "So\u011fan Kuru D\u00f6kme Kg"


def test_search_mantar_returns_base_comparable_result_and_related_variants():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "mantar",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Mantar Tabak 300 G",
                "migros_source_product_name": "Kültür Mantarı 400 G Paket",
            },
            {
                "standardized_product_name": "istridye mantar",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Mantar İstiridye 200 G",
            },
            {
                "standardized_product_name": "kestane mantar",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Mantar Kestane Paket 350 G",
            },
            {
                "standardized_product_name": "shiitake mantar",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Müpa Mantar Shiitake 200 G",
            },
            {
                "standardized_product_name": "izgaralik mantar",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Mantar Izgaralık 500 G",
            },
        ]
    )

    search_results = search_product_catalog(catalog_df, "mantar")
    assert product_names(search_results)[0] == "mantar"

    sections = build_search_group_sections(catalog_df, "mantar")
    assert sections["safe_groups"] == [
        {
            "selection_id": "product:mantar",
            "selection_type": "product",
            "family_id": None,
            "family_label": None,
            "product_names": ["mantar"],
        }
    ]

    related_product_names = {
        product_name
        for group in sections["related_groups"]
        for product_name in group["product_names"]
    }
    assert related_product_names == {
        "istridye mantar",
        "kestane mantar",
        "shiitake mantar",
        "izgaralik mantar",
    }


def test_search_generic_sut_prefers_plain_milk_over_derivatives():
    catalog_df = make_catalog(
        [
            ("eker sutlac", 1, "migros"),
            ("sek sut", 1, "migros"),
            ("sutlu tatli", 1, "migros"),
            ("sutas tereyagi", 1, "migros"),
            ("ari sutu shot", 1, "migros"),
            ("krema sutlu", 1, "migros"),
            ("puding sutlu", 1, "migros"),
            ("alpimilk cikolatali sut", 1, "migros"),
            ("pinar laktozsuz sut", 1, "migros"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "sut"))[0] == "sek sut"


def test_search_generic_sut_prefers_plain_milk_over_flavored_milk():
    catalog_df = make_catalog(
        [
            ("sek sut", 1, "migros"),
            ("alpimilk cikolatali sut", 1, "migros"),
            ("pinar cilekli sut", 1, "migros"),
            ("proteinli sut", 1, "migros"),
            ("pinar laktozsuz sut", 1, "migros"),
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "sut"))
    assert ranked[0] == "sek sut"
    assert ranked.index("alpimilk cikolatali sut") > ranked.index("sek sut")
    assert ranked.index("pinar cilekli sut") > ranked.index("sek sut")


def test_search_explicit_cikolatali_sut_allows_flavored_milk():
    catalog_df = make_catalog(
        [
            ("sek sut", 1, "migros"),
            ("alpimilk cikolatali sut", 1, "migros"),
            ("pinar cilekli sut", 1, "migros"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "cikolatali sut"))[0] == (
        "alpimilk cikolatali sut"
    )


def test_search_explicit_laktozsuz_sut_allows_lactose_free_milk():
    catalog_df = make_catalog(
        [
            ("sek sut", 1, "migros"),
            ("pinar laktozsuz sut", 1, "migros"),
            ("proteinli sut", 1, "migros"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "laktozsuz sut"))[0] == (
        "pinar laktozsuz sut"
    )


def test_search_generic_yumurta_penalizes_bildircin_when_plain_eggs_exist():
    catalog_df = make_catalog(
        [
            ("bildircin yumurta", 1, "migros"),
            ("keskinoglu yumurta", 1, "migros"),
            ("organik yumurta", 1, "migros"),
            ("gezen yumurta", 1, "migros"),
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "yumurta"))
    assert ranked[0] == "keskinoglu yumurta"
    assert ranked.index("bildircin yumurta") > ranked.index("keskinoglu yumurta")


def test_search_generic_yumurta_prefers_15_then_10_pack_over_larger_or_variant_packs():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "yumurta 15 adet",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Yumurta L Boy 15'li",
                "migros_source_product_name": "Keskinoglu 15'li L Buyuk Boy Yumurta",
            },
            {
                "standardized_product_name": "yumurta 10 adet",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Gezen Tavuk Yumurtasi M Boy 10'lu",
                "migros_source_product_name": "Keskinoglu Omega-3 M 10'lu Yumurta",
            },
            {
                "standardized_product_name": "yumurta 20 adet",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Evrenkaya Yumurta 20'li 73 G",
                "migros_source_product_name": "Keskinoglu 20'li Beyaz L Yumurta 63-73 G",
            },
            {
                "standardized_product_name": "bildircin yumurta 12 adet",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Gures Bildircin Yumurtasi 12'li",
                "migros_source_product_name": "Gures Bildircin Yumurta 12'li",
            },
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "yumurta"))
    assert ranked[:3] == [
        "yumurta 15 adet",
        "yumurta 10 adet",
        "yumurta 20 adet",
    ]
    assert ranked.index("bildircin yumurta 12 adet") > ranked.index("yumurta 20 adet")


def test_search_explicit_bildircin_yumurta_allows_bildircin_variant():
    catalog_df = make_catalog(
        [
            ("bildircin yumurta", 1, "migros"),
            ("keskinoglu yumurta", 1, "migros"),
            ("organik yumurta", 1, "migros"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "bildircin yumurta"))[0] == (
        "bildircin yumurta"
    )


def test_search_generic_pirinc_excludes_cereal_and_prefers_plain_rice():
    catalog_df = make_catalog(
        [
            ("baldo pirinc", 2, "a101, migros"),
            ("nestle crunch pirinc gevregi", 2, "a101, migros"),
            ("special k pirinc gevregi", 1, "migros"),
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "pirinc"))
    assert ranked == ["baldo pirinc"]


def test_search_explicit_aycicek_yagi_prefers_sunflower_oil_and_excludes_olive_oil():
    catalog_df = make_catalog(
        [
            ("aycicek yagi 5 l", 2, "a101, migros"),
            ("sivi yag 2 l", 1, "a101"),
            ("zeytinyagi 1 l", 2, "a101, migros"),
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "aycicek yagi"))
    assert ranked == ["aycicek yagi 5 l"]
    assert "zeytinyagi 1 l" not in ranked


def test_search_explicit_zeytinyagi_prefers_olive_oil_and_avoids_sunflower_oil():
    catalog_df = make_catalog(
        [
            ("aycicek yagi 5 l", 2, "a101, migros"),
            ("zeytinyagi 1 l", 2, "a101, migros"),
            ("riviera zeytinyagi 2 l", 1, "migros"),
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "zeytinyagi"))
    assert ranked[:2] == ["zeytinyagi 1 l", "riviera zeytinyagi 2 l"]
    assert "aycicek yagi 5 l" not in ranked


def test_search_generic_yag_requires_actual_oil_tokens():
    catalog_df = make_catalog(
        [
            ("zeytinyagi", 1, "migros"),
            ("aycicek yagi", 1, "a101"),
            ("yari yagli sut", 2, "a101, migros"),
            ("tereyagi", 1, "migros"),
            ("surulebilir yag", 1, "migros"),
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "yag"))
    assert ranked == ["zeytinyagi", "aycicek yagi"]


def test_search_generic_yag_returns_related_family_groups_instead_of_safe_product_pick():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "aycicek yagi 1 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Vera Ayçiçek Yağı 1 L",
                "migros_source_product_name": "Migros Ayçiçek Yağı 1 L",
            },
            {
                "standardized_product_name": "zeytinyagi 1 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Yudum Egemden Riviera Zeytinyağı 1 L",
                "migros_source_product_name": "Yudum Egemden Riviera Zeytinyağı 1 L",
            },
            {
                "standardized_product_name": "misir yagi 2 l",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Orkide Mısırözü Yağı 2 L",
                "migros_source_product_name": None,
            },
        ]
    )

    sections = build_search_group_sections(catalog_df, "yag")

    assert sections["safe_groups"] == []
    assert [group["family_label"] for group in sections["related_groups"][:3]] == [
        "Aycicek yagi",
        "Zeytinyagi",
        "Misirozu yagi",
    ]
    assert all(group.get("force_review") for group in sections["related_groups"][:3])


def test_search_explicit_turk_kahvesi_does_not_mix_with_filter_or_latte():
    catalog_df = make_catalog(
        [
            ("turk kahvesi 100 g", 2, "a101, migros"),
            ("filtre kahve 250 g", 2, "a101, migros"),
            ("hazir kahve 80 g", 1, "migros"),
            ("kahveli sutlu icecek latte", 2, "a101, migros"),
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "turk kahvesi"))
    assert ranked == ["turk kahvesi 100 g"]


def test_search_explicit_filtre_kahve_does_not_mix_with_turk_or_latte():
    catalog_df = make_catalog(
        [
            ("turk kahvesi 100 g", 2, "a101, migros"),
            ("filtre kahve 250 g", 2, "a101, migros"),
            ("hazir kahve 80 g", 1, "migros"),
            ("kahveli sutlu icecek latte", 2, "a101, migros"),
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "filtre kahve"))
    assert ranked == ["filtre kahve 250 g"]


def test_search_generic_kahve_excludes_latte_and_creamer_and_prefers_real_coffee():
    catalog_df = make_catalog(
        [
            ("turk kahvesi", 2, "a101, migros"),
            ("filtre kahve", 1, "migros"),
            ("kahve kremasi", 1, "migros"),
            ("kahveli sutlu icecek latte", 2, "a101, migros"),
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "kahve"))
    assert ranked[:2] == ["turk kahvesi", "filtre kahve"]
    assert "kahve kremasi" not in ranked
    assert "kahveli sutlu icecek latte" not in ranked


def test_search_generic_kahve_returns_related_subtype_groups_in_order():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "turk kahvesi 100 g",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Kocatepe Türk Kahvesi 100 G",
                "migros_source_product_name": "Kocatepe Türk Kahvesi 100 G",
            },
            {
                "standardized_product_name": "filtre kahve 250 g",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Jacobs Selection Filtre Kahve 250 G",
                "migros_source_product_name": "Jacobs Selection Filtre Kahve 250 G",
            },
            {
                "standardized_product_name": "gold kahve 80 g",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Nescafe Gold Kahve 80 G",
                "migros_source_product_name": None,
            },
        ]
    )

    sections = build_search_group_sections(catalog_df, "kahve")

    assert sections["safe_groups"] == []
    assert [group["family_label"] for group in sections["related_groups"][:3]] == [
        "Turk kahvesi",
        "Filtre kahve",
        "Instant kahve",
    ]
    assert all(group.get("force_review") for group in sections["related_groups"][:3])


def test_search_generic_toilet_paper_excludes_wet_wipes_and_prefers_standard_roll_counts():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "papia tuvalet kagidi 16 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Papia Platinum Tuvalet Kagidi 16'li",
                "migros_source_product_name": "Papia Platinum 4 Katli Tuvalet Kagidi 16'li",
            },
            {
                "standardized_product_name": "solo tuvalet kagidi 32 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Solo Tuvalet Kagidi 32'li",
                "migros_source_product_name": "Solo Bambu Katkili Tuvalet Kagidi 32'li",
            },
            {
                "standardized_product_name": "islak tuvalet kagidi 40 roll",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Islak Tuvalet Kagidi 40'li",
                "migros_source_product_name": None,
            },
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "tuvalet kagidi"))
    assert ranked[0] == "papia tuvalet kagidi 16 roll"
    assert "islak tuvalet kagidi 40 roll" in ranked
    assert ranked.index("islak tuvalet kagidi 40 roll") > ranked.index(
        "solo tuvalet kagidi 32 roll"
    )


def test_search_explicit_papia_toilet_paper_matches_grouped_row_via_source_names():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "papia tuvalet kagidi 16 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Papia Platinum Tuvalet Kagidi 16'li",
                "migros_source_product_name": "Papia Platinum 4 Katli Tuvalet Kagidi 16'li",
            },
            {
                "standardized_product_name": "solo tuvalet kagidi 16 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Solo Tuvalet Kagidi 16'li",
                "migros_source_product_name": "Solo Bambu Katkili Tuvalet Kagidi 16'li",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "papia tuvalet kagidi"))[
        0
    ] == "papia tuvalet kagidi 16 roll"


def test_search_turkish_toilet_paper_query_matches_same_safe_pair_as_ascii():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "solo tuvalet kagidi 16 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Solo Bambu Katkılı 2 Katlı Tuvalet Kağıdı 16'lı",
                "migros_source_product_name": "Solo Bambu Katkılı Tuvalet Kağıdı 16'lı",
            },
            {
                "standardized_product_name": "papia tuvalet kagidi 16 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparison_review_required",
                "a101_source_product_name": "Papia Egzotik 3 Katlı Tuvalet Kağıdı 16'lı",
                "migros_source_product_name": "Papia Platinum 4 Katlı Tuvalet Kağıdı 16'lı",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "tuvalet kağıdı"))[0] == (
        "solo tuvalet kagidi 16 roll"
    )
    assert product_names(search_product_catalog(catalog_df, "tuvalet kagidi"))[0] == (
        "solo tuvalet kagidi 16 roll"
    )


def test_search_turkish_papia_toilet_paper_query_finds_review_only_group():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "papia tuvalet kagidi 12 roll",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Papia Egzotik 3 Katlı Tuvalet Kağıdı 12'li",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "papia tuvalet kagidi 16 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparison_review_required",
                "a101_source_product_name": "Papia Egzotik 3 Katlı Tuvalet Kağıdı 16'lı",
                "migros_source_product_name": "Papia Platinum 4 Katlı Tuvalet Kağıdı 16'lı",
            },
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "papia tuvalet kağıdı"))
    assert ranked[0] == "papia tuvalet kagidi 12 roll"
    assert "papia tuvalet kagidi 16 roll" in ranked


def test_search_explicit_16li_toilet_paper_prefers_matching_roll_count():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "papia tuvalet kagidi 16 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Papia Platinum Tuvalet Kagidi 16'li",
                "migros_source_product_name": "Papia Platinum 4 Katli Tuvalet Kagidi 16'li",
            },
            {
                "standardized_product_name": "papia tuvalet kagidi 32 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Papia Platinum Tuvalet Kagidi 32'li",
                "migros_source_product_name": "Papia Platinum 4 Katli Tuvalet Kagidi 32'li",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "16'li tuvalet kagidi"))[
        0
    ] == "papia tuvalet kagidi 16 roll"


def test_search_explicit_turkish_16li_toilet_paper_prefers_matching_roll_count():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "solo tuvalet kagidi 16 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Solo Bambu Katkılı 2 Katlı Tuvalet Kağıdı 16'lı",
                "migros_source_product_name": "Solo Bambu Katkılı Tuvalet Kağıdı 16'lı",
            },
            {
                "standardized_product_name": "solo tuvalet kagidi 32 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Solo Bambu Katkılı 2 Katlı Tuvalet Kağıdı 32'li",
                "migros_source_product_name": "Solo Bambu Katkılı Tuvalet Kağıdı 32'li",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "16'lı tuvalet kağıdı"))[
        0
    ] == "solo tuvalet kagidi 16 roll"


def test_search_explicit_turkish_solo_toilet_paper_matches_ascii_group():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "solo tuvalet kagidi 16 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Solo Bambu Katkılı 2 Katlı Tuvalet Kağıdı 16'lı",
                "migros_source_product_name": "Solo Bambu Katkılı Tuvalet Kağıdı 16'lı",
            },
            {
                "standardized_product_name": "solo tuvalet kagidi 40 roll",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Solo Tuvalet Kağıdı 40'lı",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "solo tuvalet kağıdı"))[
        0
    ] == "solo tuvalet kagidi 16 roll"


def test_search_generic_toilet_paper_returns_comparable_pair_when_available():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "papia tuvalet kagidi 16 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Papia Platinum Tuvalet Kagidi 16'li",
                "migros_source_product_name": "Papia Platinum 4 Katli Tuvalet Kagidi 16'li",
            },
            {
                "standardized_product_name": "solo tuvalet kagidi 32 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Solo Tuvalet Kagidi 32'li",
                "migros_source_product_name": "Solo Bambu Katkili Tuvalet Kagidi 32'li",
            },
        ]
    )

    sections = build_search_group_sections(catalog_df, "tuvalet kagidi")

    assert sections["safe_groups"][0]["selection_id"] == "product:papia tuvalet kagidi 16 roll"


def test_search_generic_kagit_havlu_excludes_toilet_paper_and_prefers_standard_roll_counts():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "papia kagit havlu 8 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Papia Kagit Havlu 8'li",
                "migros_source_product_name": "Papia Kagit Havlu 8'li",
            },
            {
                "standardized_product_name": "solo kagit havlu 16 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Solo Kagit Havlu 16'li",
                "migros_source_product_name": "Solo Bambu Katkili Kagit Havlu 16'li",
            },
            {
                "standardized_product_name": "papia tuvalet kagidi 16 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Papia Platinum Tuvalet Kagidi 16'li",
                "migros_source_product_name": "Papia Platinum 4 Katli Tuvalet Kagidi 16'li",
            },
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "kagit havlu"))
    assert ranked[0] == "papia kagit havlu 8 roll"
    assert "papia tuvalet kagidi 16 roll" not in ranked[:2]


def test_search_havlu_kagit_query_matches_kagit_havlu_group():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "solo kagit havlu 8 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Solo Kagit Havlu 8'li",
                "migros_source_product_name": "Solo Kagit Havlu 8'li",
            },
            {
                "standardized_product_name": "solo tuvalet kagidi 8 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Solo Tuvalet Kagidi 8'li",
                "migros_source_product_name": "Solo Tuvalet Kagidi 8'li",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "havlu kagit"))[0] == (
        "solo kagit havlu 8 roll"
    )
    assert product_names(search_product_catalog(catalog_df, "havlu kağıdı"))[0] == (
        "solo kagit havlu 8 roll"
    )
    assert product_names(search_product_catalog(catalog_df, "kağıt havlu"))[0] == (
        "solo kagit havlu 8 roll"
    )


def test_search_explicit_8li_kagit_havlu_prefers_matching_roll_count():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "papia kagit havlu 8 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Papia Kagit Havlu 8'li",
                "migros_source_product_name": "Papia Kagit Havlu 8'li",
            },
            {
                "standardized_product_name": "papia kagit havlu 12 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Papia Kagit Havlu 12'li",
                "migros_source_product_name": "Papia Kagit Havlu 12'li",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "8'li kağıt havlu"))[0] == (
        "papia kagit havlu 8 roll"
    )


def test_search_explicit_8li_kagit_havlu_does_not_fall_back_to_safe_6_roll_pair():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "papia kagit havlu 6 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Papia Kagit Havlu 6'lı",
                "migros_source_product_name": "Papia Kagit Havlu 6'lı",
            },
            {
                "standardized_product_name": "papia kagit havlu 8 roll",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Papia BioCare 3 Katlı Kağıt Havlu 8'li",
                "migros_source_product_name": None,
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "8'li kağıt havlu")) == [
        "papia kagit havlu 8 roll"
    ]


def test_search_explicit_6li_kagit_havlu_prefers_matching_roll_count():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "papia kagit havlu 6 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparison_review_required",
                "a101_source_product_name": "Papia 3 Katlı Kağıt Havlu 6'lı",
                "migros_source_product_name": "Papia Biocare Kağıt Havlu 6'lı",
            },
            {
                "standardized_product_name": "papia kagit havlu 8 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Papia BioCare 3 Katlı Kağıt Havlu 8'li",
                "migros_source_product_name": "Papia Biocare Havlu 8'li",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "6'lı kağıt havlu"))[0] == (
        "papia kagit havlu 6 roll"
    )


def test_search_brand_query_prefers_brand_specific_paper_towel_rows_over_generic_count_rows():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "kagit havlu 6 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparison_review_required",
                "a101_source_product_name": "Papia 3 Katlı Kağıt Havlu 6'lı",
                "migros_source_product_name": "Papia Biocare Kağıt Havlu 6'lı",
            },
            {
                "standardized_product_name": "papia kagit havlu 6 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparison_review_required",
                "a101_source_product_name": "Papia 3 Katlı Kağıt Havlu 6'lı",
                "migros_source_product_name": "Papia Biocare Kağıt Havlu 6'lı",
            },
            {
                "standardized_product_name": "papia kagit havlu 8 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparison_review_required",
                "a101_source_product_name": "Papia BioCare 3 Katlı Kağıt Havlu 8'li",
                "migros_source_product_name": "Papia Biocare Havlu 8'li",
            },
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "papia kağıt havlu"))
    assert ranked[0].startswith("papia kagit havlu")
    assert ranked.index("kagit havlu 6 roll") > ranked.index("papia kagit havlu 6 roll")


def test_search_generic_kagit_havlu_returns_comparable_pair_when_available():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "papia kagit havlu 8 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Papia Kagit Havlu 8'li",
                "migros_source_product_name": "Papia Kagit Havlu 8'li",
            },
            {
                "standardized_product_name": "solo kagit havlu 16 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Solo Kagit Havlu 16'li",
                "migros_source_product_name": "Solo Bambu Katkili Kagit Havlu 16'li",
            },
        ]
    )

    sections = build_search_group_sections(catalog_df, "kağıt havlu")

    assert sections["safe_groups"][0]["selection_id"] == "product:papia kagit havlu 8 roll"


def test_search_generic_sogan_prefers_kuru_kg_over_taze_and_arpacik():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "sogan",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Sogan Kg",
                "migros_source_product_name": "Sogan Kuru Dokme Kg",
            },
            {
                "standardized_product_name": "sogan taze",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparison_review_required",
                "a101_source_product_name": "Taze Sogan Adet",
                "migros_source_product_name": "Sogan Taze Demet",
            },
            {
                "standardized_product_name": "arpacik sogan",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Sogan Arpacik Kg",
            },
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "sogan"))
    assert ranked[0] == "sogan"
    assert ranked.index("sogan taze") > ranked.index("sogan")


def test_search_generic_ekmek_prefers_plain_bread_over_variants():
    catalog_df = make_catalog(
        [
            ("ekmek", 1, "a101"),
            ("ekmek sofra", 1, "migros"),
            ("cavdarli ekmek", 1, "migros"),
            ("ekmek kepekli", 1, "migros"),
            ("ekmek eksi mayali", 1, "migros"),
            ("ekmek nimet tost", 1, "a101"),
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "ekmek"))
    assert ranked[:2] == ["ekmek", "ekmek sofra"]
    assert ranked.index("cavdarli ekmek") > ranked.index("ekmek sofra")
    assert ranked.index("ekmek nimet tost") > ranked.index("ekmek sofra")


def test_search_explicit_cavdarli_ekmek_allows_variant():
    catalog_df = make_catalog(
        [
            ("ekmek", 1, "a101"),
            ("cavdarli ekmek", 1, "migros"),
            ("ekmek kepekli", 1, "migros"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "cavdarli ekmek"))[0] == (
        "cavdarli ekmek"
    )


def test_search_explicit_tost_ekmek_allows_tost_variant():
    catalog_df = make_catalog(
        [
            ("ekmek", 1, "a101"),
            ("ekmek nimet tost", 1, "a101"),
            ("ekmek sofra", 1, "migros"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "tost ekmek"))[0] == (
        "ekmek nimet tost"
    )


def test_search_explicit_tava_ekmek_prefers_comparable_tava_pair_over_generic_ekmek():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "ekmek",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparison_review_required",
                "a101_source_product_name": "Ekmek 200 G",
                "migros_source_product_name": "Sofra Ekmek Adet",
            },
            {
                "standardized_product_name": "tava ekmek",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Uno Tam Bu\u011fday Unlu Tava Ekme\u011fi 450 G",
                "migros_source_product_name": "Uno %100 Tam Bu\u011fday Tava Ekme\u011fi 450 G",
            },
        ]
    )

    results = search_product_catalog(catalog_df, "tava ekmek")
    top_row = results.iloc[0]

    assert top_row["standardized_product_name"] == "tava ekmek"
    assert top_row["a101_source_product_name"] == "Uno Tam Bu\u011fday Unlu Tava Ekme\u011fi 450 G"
    assert top_row["migros_source_product_name"] == "Uno %100 Tam Bu\u011fday Tava Ekme\u011fi 450 G"


def test_build_search_group_sections_keeps_generic_ekmek_base_result_over_tava_variant():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "ekmek",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparison_review_required",
                "a101_source_product_name": "Ekmek 200 G",
                "migros_source_product_name": "Sofra Ekmek Adet",
            },
            {
                "standardized_product_name": "tava ekmek",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Uno Tam Bu\u011fday Unlu Tava Ekme\u011fi 450 G",
                "migros_source_product_name": "Uno %100 Tam Bu\u011fday Tava Ekme\u011fi 450 G",
            },
        ]
    )

    sections = build_search_group_sections(catalog_df, "ekmek")

    assert sections["safe_groups"] == []
    assert sections["related_groups"][0]["selection_id"] == "product:ekmek"
    related_ids = [group["selection_id"] for group in sections["related_groups"]]
    assert "product:tava ekmek" in related_ids


def test_search_generic_un_does_not_match_unal_or_unlu_products():
    catalog_df = make_catalog(
        [
            ("toz un", 1, "migros"),
            ("unal beyaz peynir", 1, "a101"),
            ("unlu tava ekmek", 1, "a101"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "un")) == ["toz un"]


def test_search_generic_su_does_not_match_sut_or_yogurt():
    catalog_df = make_catalog(
        [
            ("icme su", 1, "migros"),
            ("sut", 1, "migros"),
            ("laktozsuz yogurt", 1, "a101"),
            ("domates sos", 1, "migros"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "su")) == ["icme su"]


def test_search_el_havlusu_does_not_match_water():
    catalog_df = make_catalog(
        [
            ("su 0.5 l", 2, "a101, migros"),
            ("solo kagit havlu 6 roll", 2, "a101, migros"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "el havlusu")) == []


def test_search_havlusu_does_not_match_water():
    catalog_df = make_catalog(
        [
            ("su 0.5 l", 2, "a101, migros"),
            ("solo kagit havlu 6 roll", 2, "a101, migros"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "havlusu")) == []


def test_search_su_0_5_l_still_returns_water():
    catalog_df = make_catalog(
        [
            ("su 0.5 l", 2, "a101, migros"),
            ("solo kagit havlu 6 roll", 2, "a101, migros"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "su 0.5 l"))[0] == (
        "su 0.5 l"
    )


def test_search_generic_su_excludes_other_beverages():
    catalog_df = make_catalog(
        [
            ("su 0.5 l", 2, "a101, migros"),
            ("kola coca-cola 1 l", 2, "a101, migros"),
            ("meyve suyu 1 l", 1, "migros"),
            ("maden suyu 0.2 l", 1, "migros"),
            ("enerji icecegi 0.25 l", 1, "a101"),
            ("gazoz 1 l", 1, "a101"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "su")) == ["su 0.5 l"]


def test_search_kagit_havlu_still_returns_paper_towel_and_not_water():
    catalog_df = make_catalog(
        [
            ("su 0.5 l", 2, "a101, migros"),
            ("solo kagit havlu 6 roll", 2, "a101, migros"),
            ("papia tuvalet kagidi 16 roll", 2, "a101, migros"),
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "kağıt havlu"))
    assert ranked[0] == "solo kagit havlu 6 roll"
    assert "su 0.5 l" not in ranked[:2]


def test_search_generic_tuz_does_not_match_tuzlu_tereyagi():
    catalog_df = make_catalog(
        [
            ("kaya tuzu", 1, "migros"),
            ("tuzlu tereyagi", 1, "a101"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "tuz")) == ["kaya tuzu"]


def test_search_generic_seker_does_not_match_seker_domates():
    catalog_df = make_catalog(
        [
            ("toz seker", 1, "migros"),
            ("seker domates", 1, "a101"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "seker")) == ["toz seker"]


def test_search_explicit_seker_domates_still_works():
    catalog_df = make_catalog(
        [
            ("toz seker", 1, "migros"),
            ("seker domates", 1, "a101"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "seker domates"))[0] == (
        "seker domates"
    )


def test_search_explicit_unlu_tava_ekmegi_still_works():
    catalog_df = make_catalog(
        [
            ("toz un", 1, "migros"),
            ("unlu tava ekmek", 1, "a101"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "unlu tava ekmegi"))[0] == (
        "unlu tava ekmek"
    )


def test_search_explicit_tuzlu_tereyagi_still_works():
    catalog_df = make_catalog(
        [
            ("kaya tuzu", 1, "migros"),
            ("tuzlu tereyagi", 1, "a101"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "tuzlu tereyagi"))[0] == (
        "tuzlu tereyagi"
    )


def test_search_generic_kola_does_not_match_cikolatali_sos():
    catalog_df = make_catalog(
        [
            ("cola turka kola", 1, "migros"),
            ("bitter cikolatali sos", 1, "a101"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "kola")) == [
        "cola turka kola"
    ]


def test_search_omo_does_not_match_homojenize():
    catalog_df = make_catalog(
        [
            ("omo active fresh", 1, "migros"),
            ("sut homojenize", 1, "a101"),
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "omo")) == [
        "omo active fresh"
    ]


def test_search_fairy_does_not_collapse_related_group_to_elma_family():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "bulasik deterjani elma fairy",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Elma Bulaşık Deterjanı 1500 Ml",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "bulasik deterjani fairy sivi",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 2600 Ml",
                "migros_source_product_name": None,
            },
        ]
    )

    sections = build_search_group_sections(catalog_df, "fairy")
    assert all(
        not (
            group.get("selection_type") == "product_family"
            and group.get("family_label") == "Elma"
        )
        for group in sections["related_groups"]
    )


def test_search_pril_does_not_collapse_related_group_to_elma_family():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "bulasik deterjani elma pril sivi",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Pril Sıvı Bulaşık Deterjanı Elma 650 Ml",
            },
            {
                "standardized_product_name": "bulasik deterjani pril sivi",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Pril Sıvı Bulaşık Deterjanı 2,5 Kg",
                "migros_source_product_name": None,
            },
        ]
    )

    sections = build_search_group_sections(catalog_df, "pril")
    assert all(
        not (
            group.get("selection_type") == "product_family"
            and group.get("family_label") == "Elma"
        )
        for group in sections["related_groups"]
    )


def test_search_generic_finish_prefers_tablet_before_salt_rinse_aid_and_cleaner():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "tuz 1300 g",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Finish Bulaşık Makinesi Tuzu 1300 G",
                "migros_source_product_name": "Finish Bulaşık Makinesi Tuzu 1.3 Kg",
            },
            {
                "standardized_product_name": "101li bulasik finish makinesi tableti",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Finish Bulaşık Makinesi Tableti 101'li",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "bulasik finish makinesi parlaticisi",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Finish Bulaşık Makinesi Parlatıcısı 400 Ml",
            },
            {
                "standardized_product_name": "bulasik finish makinesi temizleyici",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Finish Bulaşık Makinesi Temizleyici 250 Ml",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "finish"))[0] == (
        "101li bulasik finish makinesi tableti"
    )


def test_search_finish_tuz_prefers_finish_dishwasher_salt_and_blocks_grocery_salt():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "tuz 1300 g",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Finish Bulaşık Makinesi Tuzu 1300 G",
                "migros_source_product_name": "Finish Bulaşık Makinesi Tuzu 1.3 Kg",
            },
            {
                "standardized_product_name": "tuz 750 g",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Billur İyotlu Tuz 750 G",
                "migros_source_product_name": "Salina İyotlu Sofra Tuzu 750 G",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "finish tuz")) == [
        "tuz 1300 g"
    ]


def test_search_bulasik_tableti_does_not_select_salt_first():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "tuz 1300 g",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Finish Bulaşık Makinesi Tuzu 1300 G",
                "migros_source_product_name": "Finish Bulaşık Makinesi Tuzu 1.3 Kg",
            },
            {
                "standardized_product_name": "bulasik makinesi migros tableti ultra",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Migros Ultra Bulaşık Makinesi Tableti 30'lu",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "bulasik tableti"))[0] == (
        "bulasik makinesi migros tableti ultra"
    )


def test_search_bulasik_deterjani_does_not_select_tablet_first():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "bulasik deterjani fairy sivi",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 2600 Ml",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "bulasik makinesi migros tableti ultra",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Migros Ultra Bulaşık Makinesi Tableti 30'lu",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "bulasik deterjani"))[0] == (
        "bulasik deterjani fairy sivi"
    )


def test_search_generic_fairy_prefers_single_pack_liquid_over_bundles_and_spray():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "2x1500 bulasik deterjani fairy",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Bulaşık Deterjanı 2x1500 Ml",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "6x650 bulasik deterjani fairy",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Fairy Bulaşık Deterjanı 6x650 Ml",
            },
            {
                "standardized_product_name": "bulasik deterjani fairy sivi",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 1500 Ml",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "fairy power sprey",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Fairy Power Sprey 500 Ml",
            },
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "fairy"))
    assert ranked[0] == "bulasik deterjani fairy sivi"
    assert ranked.index("2x1500 bulasik deterjani fairy") > ranked.index(
        "bulasik deterjani fairy sivi"
    )
    assert ranked.index("6x650 bulasik deterjani fairy") > ranked.index(
        "bulasik deterjani fairy sivi"
    )
    assert ranked.index("fairy power sprey") > ranked.index(
        "bulasik deterjani fairy sivi"
    )


def test_search_generic_pril_prefers_single_pack_liquid_over_bundle():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "2x675 bulasik deterjani pril",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Pril Bulaşık Deterjanı 2x675 Ml",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "bulasik deterjani pril",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Pril Bulaşık Deterjanı 653 Ml",
            },
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "pril"))
    assert ranked[0] == "bulasik deterjani pril"
    assert ranked.index("2x675 bulasik deterjani pril") > ranked.index(
        "bulasik deterjani pril"
    )


def test_search_explicit_fairy_2x1500_prefers_matching_bundle():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "2x1500 bulasik deterjani fairy",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Bulaşık Deterjanı 2x1500 Ml",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "bulasik deterjani fairy sivi",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 1500 Ml",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "fairy 2x1500"))[0] == (
        "2x1500 bulasik deterjani fairy"
    )


def test_search_explicit_fairy_6x650_prefers_matching_bundle():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "6x650 bulasik deterjani fairy",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Fairy Bulaşık Deterjanı 6x650 Ml",
            },
            {
                "standardized_product_name": "bulasik deterjani fairy sivi",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 1500 Ml",
                "migros_source_product_name": None,
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "fairy 6x650"))[0] == (
        "6x650 bulasik deterjani fairy"
    )


def test_search_explicit_fairy_sprey_prefers_spray():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "fairy power sprey",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Fairy Power Sprey 500 Ml",
            },
            {
                "standardized_product_name": "bulasik deterjani fairy sivi",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 1500 Ml",
                "migros_source_product_name": None,
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "fairy sprey"))[0] == (
        "fairy power sprey"
    )


def test_search_explicit_fairy_tablet_prefers_tablet():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "22'li fairy kokulu limon platinum plus tablet",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Platinum Plus Bulaşık Makinesi Tableti 22'li",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "bulasik deterjani fairy sivi",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 1500 Ml",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "fairy tablet"))[0] == (
        "22'li fairy kokulu limon platinum plus tablet"
    )


def test_search_fairy_650_prefers_650_ml_single_pack_product():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "6x650 bulasik deterjani fairy",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Fairy Bulaşık Deterjanı 6x650 Ml",
            },
            {
                "standardized_product_name": "bulasik deterjani fairy sivi",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 650 Ml",
                "migros_source_product_name": None,
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "fairy 650"))[0] == (
        "bulasik deterjani fairy sivi"
    )


def test_search_fairy_1500_prefers_1500_ml_single_pack_over_2x1500_bundle():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "2x1500 bulasik deterjani fairy",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Bulaşık Deterjanı 2x1500 Ml",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "bulasik deterjani fairy sivi",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 1500 Ml",
            },
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "fairy 1500"))
    assert ranked[0] == "bulasik deterjani fairy sivi"
    assert ranked.index("2x1500 bulasik deterjani fairy") > ranked.index(
        "bulasik deterjani fairy sivi"
    )


def test_search_bulasik_tableti_turkish_and_ascii_queries_have_equivalent_top_intent():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "bulasik makinesi migros tableti ultra",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Migros Ultra Bulaşık Makinesi Tableti 30'lu",
            },
            {
                "standardized_product_name": "tuz 1300 g",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Finish Bulaşık Makinesi Tuzu 1300 G",
                "migros_source_product_name": "Finish Bulaşık Makinesi Tuzu 1.3 Kg",
            },
        ]
    )

    top_ascii = product_names(search_product_catalog(catalog_df, "bulasik tableti"))[0]
    top_turkish = product_names(
        search_product_catalog(catalog_df, "bulaşık tableti")
    )[0]
    top_ascii_singular = product_names(
        search_product_catalog(catalog_df, "bulasik tablet")
    )[0]
    top_turkish_singular = product_names(
        search_product_catalog(catalog_df, "bulaşık tablet")
    )[0]

    assert top_ascii == "bulasik makinesi migros tableti ultra"
    assert top_turkish == top_ascii
    assert top_ascii_singular == top_ascii
    assert top_turkish_singular == top_ascii


def test_search_fairy_elma_1500_prefers_comparable_grouped_pair():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "fairy elma bulasik deterjani 1.5 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Elma Bulaşık Deterjanı 1500 Ml",
                "migros_source_product_name": "Fairy Temiz & Ferah Elma Kokulu Elde Yıkama 1.5 L",
            },
            {
                "standardized_product_name": "fairy limon bulasik deterjani 0.65 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Limon Bulaşık Deterjanı 650 Ml",
                "migros_source_product_name": "Fairy Limon Kokulu Elde Yıkama 650 Ml",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "fairy elma 1500"))[0] == (
        "fairy elma bulasik deterjani 1.5 l"
    )


def test_search_fairy_elma_prefers_comparable_grouped_pair():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "fairy elma bulasik deterjani 1.5 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Elma BulaÅŸÄ±k DeterjanÄ± 1500 Ml",
                "migros_source_product_name": "Fairy Temiz & Ferah Elma Kokulu Elde YÄ±kama 1.5 L",
            },
            {
                "standardized_product_name": "fairy limon bulasik deterjani 0.65 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Limon BulaÅŸÄ±k DeterjanÄ± 650 Ml",
                "migros_source_product_name": "Fairy Limon Kokulu Elde YÄ±kama 650 Ml",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "fairy elma"))[0] == (
        "fairy elma bulasik deterjani 1.5 l"
    )


def test_search_query_normalization_handles_fairy_limon_and_fairy_lemon():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "fairy limon bulasik deterjani 0.65 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Limon Bulaşık Deterjanı 650 Ml",
                "migros_source_product_name": "Fairy Lemon Kokulu Elde Yıkama 650 Ml",
            },
            {
                "standardized_product_name": "fairy elma bulasik deterjani 1.5 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Elma Bulaşık Deterjanı 1500 Ml",
                "migros_source_product_name": "Fairy Elma Kokulu Elde Yıkama 1.5 L",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "fairy limon"))[0] == (
        "fairy limon bulasik deterjani 0.65 l"
    )
    assert product_names(search_product_catalog(catalog_df, "fairy lemon"))[0] == (
        "fairy limon bulasik deterjani 0.65 l"
    )


def test_search_query_normalization_handles_finish_power_bingo_limon_and_pril_sirke():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "finish power bulasik tableti 40 li",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Finish Power Bulaşık Makinesi Tableti 40'lı",
                "migros_source_product_name": "Finish Power Tablet 40'lı",
            },
            {
                "standardized_product_name": "bingo limon bulasik deterjani 1.5 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Bingo Limon Bulaşık Deterjanı 1.5 L",
                "migros_source_product_name": "Bingo Limon Kokulu Elde Yıkama 1500 Ml",
            },
            {
                "standardized_product_name": "pril sirke bulasik deterjani 0.65 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Pril Sirke Gücü Bulaşık Deterjanı 650 Ml",
                "migros_source_product_name": "Pril Vinegar 650 Ml",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "finish power"))[0] == (
        "finish power bulasik tableti 40 li"
    )
    assert product_names(search_product_catalog(catalog_df, "bingo limon"))[0] == (
        "bingo limon bulasik deterjani 1.5 l"
    )
    assert product_names(search_product_catalog(catalog_df, "pril sirke"))[0] == (
        "pril sirke bulasik deterjani 0.65 l"
    )


def test_search_query_normalization_handles_ascii_and_turkish_cleaning_queries():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "domestos ultra camasir suyu",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Domestos Ultra Çamaşır Suyu 750 Ml",
                "migros_source_product_name": "Domestos Ultra Çamaşır Suyu 750 Ml",
            },
            {
                "standardized_product_name": "fairy bulasik deterjani 0.65 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 650 ml",
                "migros_source_product_name": "Fairy Sıvı Bulaşık Deterjanı Limon 650 Ml",
            },
            {
                "standardized_product_name": "solo kagit havlu 6 roll",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Solo Bambu Kağıt Havlu 2 Katlı 6'lı",
                "migros_source_product_name": "Solo Bambu Katkılı Havlu 6'lı",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "çamaşır suyu"))[0] == (
        "domestos ultra camasir suyu"
    )
    assert product_names(search_product_catalog(catalog_df, "camasir suyu"))[0] == (
        "domestos ultra camasir suyu"
    )
    assert product_names(search_product_catalog(catalog_df, "bulaşık deterjanı"))[0] == (
        "fairy bulasik deterjani 0.65 l"
    )
    assert product_names(search_product_catalog(catalog_df, "bulasik deterjani"))[0] == (
        "fairy bulasik deterjani 0.65 l"
    )
    assert product_names(search_product_catalog(catalog_df, "kağıt havlu"))[0] == (
        "solo kagit havlu 6 roll"
    )
    assert product_names(search_product_catalog(catalog_df, "kagit havlu"))[0] == (
        "solo kagit havlu 6 roll"
    )


def test_search_generic_fairy_prefers_comparable_group_over_single_market_rows():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "fairy bulasik deterjani 0.65 l",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 650 ml",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "fairy bulasik deterjani 1 l",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 1 L",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "fairy elma bulasik deterjani 1.5 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Elma Bulaşık Deterjanı 1500 Ml",
                "migros_source_product_name": "Fairy Temiz & Ferah Elma Kokulu Elde Yıkama 1.5 L",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "fairy"))[0] == (
        "fairy elma bulasik deterjani 1.5 l"
    )


def test_build_search_group_sections_generic_fairy_keeps_comparable_group_as_main_selection():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "fairy bulasik deterjani 0.65 l",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 650 ml",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "fairy elma bulasik deterjani 1.5 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Elma Bulaşık Deterjanı 1500 Ml",
                "migros_source_product_name": "Fairy Temiz & Ferah Elma Kokulu Elde Yıkama 1.5 L",
            },
        ]
    )

    sections = build_search_group_sections(catalog_df, "fairy")

    assert sections["safe_groups"][0]["selection_id"] == (
        "product:fairy elma bulasik deterjani 1.5 l"
    )
    assert sections["related_groups"][0]["selection_id"] == (
        "product:fairy bulasik deterjani 0.65 l"
    )


def test_search_generic_domestos_prefers_bleach_over_surface_cleaner():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "domestos kopuk mutfak",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Domestos Banyo & Mutfak Köpük 500 Ml",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "domestos ultra camasir suyu",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Domestos Ultra Çamaşır Suyu 750 Ml",
            },
        ]
    )

    assert product_names(search_product_catalog(catalog_df, "domestos"))[0] == (
        "domestos ultra camasir suyu"
    )
def test_search_domates_keeps_exact_produce_matches_and_excludes_domatesli_oil_rows():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "domates",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Domates Kg",
                "migros_source_product_name": "Domates Kg",
            },
            {
                "standardized_product_name": "domates kokteyl",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparison_review_required",
                "a101_source_product_name": "Kokteyl Domates 500 G",
                "migros_source_product_name": "Kokteyl Domates 500 G",
            },
            {
                "standardized_product_name": "zeytinyagi 200 g",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Domatesli Zeytinyağı 200 G",
                "migros_source_product_name": "Domatesli Zeytinyağı 200 G",
            },
        ]
    )

    ranked = product_names(search_product_catalog(catalog_df, "domates"))

    assert ranked[:2] == ["domates", "domates kokteyl"]
    assert "zeytinyagi 200 g" not in ranked


@pytest.mark.parametrize(
    ("query", "expected_top"),
    [
        ("domates", "domates"),
        ("salatalik", "hiyar"),
        ("hiyar", "hiyar"),
        ("sut", "sek sut"),
        ("su", "su 0.5 l"),
        ("domestos", "domestos ultra camasir suyu"),
        ("tuvalet kagidi", "solo tuvalet kagidi 16 roll"),
        ("bulasik tableti", "finish bulasik makinesi tableti 101 li"),
        ("finish", "finish bulasik makinesi tableti 101 li"),
    ],
)
def test_golden_queries_return_expected_non_empty_top_matches(query, expected_top):
    catalog_df = make_golden_query_catalog()

    ranked = product_names(search_product_catalog(catalog_df, query))

    assert ranked, f"Expected non-empty search results for query: {query}"
    assert ranked[0] == expected_top


@pytest.mark.parametrize(
    ("query", "expected_brand_token"),
    [
        ("fairy", "fairy"),
        ("finish", "finish"),
        ("domestos", "domestos"),
    ],
)
def test_golden_brand_queries_keep_brand_relevant_groups(query, expected_brand_token):
    catalog_df = make_golden_query_catalog()

    sections = build_search_group_sections(catalog_df, query)
    grouped_results = sections["safe_groups"] + sections["related_groups"]

    assert grouped_results, f"Expected non-empty grouped results for query: {query}"
    top_group = grouped_results[0]
    top_product_names = " ".join(top_group.get("product_names") or [])
    assert expected_brand_token in top_product_names
