from decimal import Decimal

from pipeline.optimizer.measurement import (
    format_measurement_label,
    get_comparison_status_label,
    get_measurement_mismatch_label,
)


def test_format_measurement_label_humanizes_common_pack_sizes():
    assert format_measurement_label(1, "kg") == "1 kg"
    assert format_measurement_label(Decimal("0.3"), "kg") == "300 g"
    assert (
        format_measurement_label(
            Decimal("0.125"),
            "kg",
            "Yaban Mersini Paket 125 G",
        )
        == "125 g paket"
    )
    assert format_measurement_label(1, "piece") == "1 adet"
    assert format_measurement_label(1, "demet") == "1 demet"


def test_format_measurement_label_handles_missing_pandas_values():
    assert format_measurement_label(float("nan"), "kg") == "-"
    assert format_measurement_label(1, float("nan")) == "-"
    assert format_measurement_label(Decimal("0.125"), "kg", float("nan")) == "125 g"


def test_measurement_mismatch_label_includes_pair_and_reason():
    row = {
        "a101_measurement_label": "125 g paket",
        "migros_measurement_label": "1 adet",
        "comparison_review_reason": "unit_mismatch",
    }

    assert get_measurement_mismatch_label(row) == "125 g paket vs 1 adet (unit_mismatch)"


def test_measurement_mismatch_label_is_suppressed_for_comparable_normalized_rows():
    row = {
        "coverage_status": "comparable",
        "comparison_confidence": "medium",
        "same_unit_flag": True,
        "same_quantity_flag": False,
        "comparison_review_reason": "quantity_mismatch",
        "a101_measurement_label": "300 g paket",
        "migros_measurement_label": "400 g paket",
    }

    assert get_measurement_mismatch_label(row) is None


def test_comparison_status_label_uses_conservative_safety_flags():
    assert (
        get_comparison_status_label(
            {
                "comparison_confidence": "high",
                "same_unit_flag": True,
                "same_quantity_flag": True,
            }
        )
        == "Güvenli karşılaştırma"
    )
    assert get_comparison_status_label({"same_unit_flag": False}) == "Ölçü uyumsuz"
    assert (
        get_comparison_status_label(
            {
                "same_unit_flag": True,
                "same_quantity_flag": False,
            }
        )
        == "Kontrol gerekli"
    )
    assert (
        get_comparison_status_label({"comparison_confidence": "single_source"})
        == "Tek markette var"
    )
