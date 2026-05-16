from config.retailers import RETAILER_CONFIG
from pipeline.run_a101_pipeline import DEFAULT_CATEGORY_KEY
from scraper.a101.scraper import A101_CATEGORY_IDS


def test_a101_default_category_key_is_configured_for_parser():
    category_slug = RETAILER_CONFIG["a101"]["categories"][DEFAULT_CATEGORY_KEY]

    assert category_slug == "meyve-sebze"
    assert A101_CATEGORY_IDS[category_slug] == "C01"


def test_a101_additional_category_keys_are_configured():
    categories = RETAILER_CONFIG["a101"]["categories"]

    assert categories["dairy_breakfast"] == "sut-urunleri-kahvaltilik"
    assert categories["bakery"] == "firindan"
    assert categories["flour"] == "temel-gida/un"
    assert categories["water"] == "su-icecek/su"
    assert categories["salt"] == "temel-gida/tuz-baharat-harc"
    assert categories["sugar"] == "temel-gida/seker"
    assert categories["cola"] == "su-icecek/gazli-icecekler"
    assert categories["rice"] == "temel-gida/bakliyat"
    assert categories["edible_oils"] == "temel-gida/sivi-yaglar"
    assert categories["coffee"] == "su-icecek/kahve"
    assert categories["toilet_paper"] == "kagit-urunleri/tuvalet-kagidi"
    assert categories["paper_towel"] == "kagit-urunleri/kagit-havlu"
    assert categories["dishwashing"] == "temizlik-urunleri/bulasik"
    assert categories["dishwasher_tablets"] == "temizlik-urunleri/bulasik"
    assert categories["laundry_detergent"] == "temizlik-urunleri/camasir"
    assert categories["bleach"] == "temizlik-urunleri/genel-temizlik"
    assert categories["surface_cleaner"] == "temizlik-urunleri/genel-temizlik"
    assert A101_CATEGORY_IDS["sut-urunleri-kahvaltilik"] == "C05"
    assert A101_CATEGORY_IDS["firindan"] == "C02"
    assert A101_CATEGORY_IDS["kagit-urunleri"] == "C13"
    assert A101_CATEGORY_IDS["kagit-urunleri/tuvalet-kagidi"] == "C1301"
    assert A101_CATEGORY_IDS["kagit-urunleri/kagit-havlu"] == "C1303"
    assert A101_CATEGORY_IDS["temizlik-urunleri"] == "C11"
    assert A101_CATEGORY_IDS["temizlik-urunleri/camasir"] == "C1101"
    assert A101_CATEGORY_IDS["temizlik-urunleri/bulasik"] == "C1102"
    assert A101_CATEGORY_IDS["temizlik-urunleri/genel-temizlik"] == "C1103"
    assert A101_CATEGORY_IDS["temel-gida"] == "C07"
    assert A101_CATEGORY_IDS["temel-gida/sivi-yaglar"] == "C0701"
    assert A101_CATEGORY_IDS["temel-gida/bakliyat"] == "C0702"
    assert A101_CATEGORY_IDS["temel-gida/un"] == "C0705"
    assert A101_CATEGORY_IDS["temel-gida/seker"] == "C0703"
    assert A101_CATEGORY_IDS["temel-gida/tuz-baharat-harc"] == "C0709"
    assert A101_CATEGORY_IDS["su-icecek"] == "C08"
    assert A101_CATEGORY_IDS["su-icecek/su"] == "C0805"
    assert A101_CATEGORY_IDS["su-icecek/gazli-icecekler"] == "C0801"
    assert A101_CATEGORY_IDS["su-icecek/kahve"] == "C0809"


def test_migros_bakery_category_key_is_configured():
    categories = RETAILER_CONFIG["migros"]["categories"]

    assert categories["bakery"] == "ekmek-c-455"
    assert categories["flour"] == "sade-un-c-289b"
    assert categories["water"] == "su-c-84"
    assert categories["salt"] == "tuz-c-436"
    assert categories["sugar_powdered"] == "toz-seker-c-544"
    assert categories["sugar_cube"] == "kup-seker-c-543"
    assert categories["cola"] == "kola-c-465"
    assert categories["rice_baldo"] == "baldo-pirincler-c-2788"
    assert categories["sunflower_oil"] == "aycicek-yagi-c-42d"
    assert categories["olive_oil"] == "zeytinyagi-c-433"
    assert categories["turkish_coffee"] == "turk-kahvesi-c-28c4"
    assert categories["filter_coffee"] == "filtre-kahve-c-11223"
    assert categories["toilet_paper"] == "tuvalet-kagidi-c-49c"
    assert categories["paper_towel"] == "kagit-havlu-c-49d"
    assert categories["dishwashing"] == "sivi-bulasik-deterjani-c-489"
    assert categories["dishwasher_tablets"] == "bulasik-makinesi-tableti-c-28eb"
    assert categories["laundry_detergent"] == "camasir-deterjani-c-486"
    assert categories["bleach"] == "camasir-suyu-c-483"
    assert categories["surface_cleaner"] == "sivi-yuzey-temizleyiciler-c-2912"
    assert categories["kitchen_bath_cleaner"] == "mutfak-ve-banyo-temizleyiciler-c-491"
