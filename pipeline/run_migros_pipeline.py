import logging
import os
import re
from typing import Any, Optional, Tuple

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from scraper.migros.categories import get_migros_category_products

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_CATEGORY_SLUG = "meyve-sebze-c-2"
SOURCE_NAME = "migros"
CURRENCY = "TRY"


# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------
def get_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set.")
    return psycopg2.connect(database_url)


# ---------------------------------------------------------------------------
# Run lifecycle
# ---------------------------------------------------------------------------
def start_run(cursor, category_slug: str) -> int:
    cursor.execute(
        """
        INSERT INTO scrape_runs (source_name, status)
        VALUES (%s, 'running')
        RETURNING run_id
        """,
        (SOURCE_NAME,),
    )
    run_id = cursor.fetchone()[0]
    logger.info("Run started — run_id=%s  category=%s", run_id, category_slug)
    return run_id


def finish_run(cursor, run_id: int, records: int):
    cursor.execute(
        """
        UPDATE scrape_runs
        SET finished_at = NOW(),
            status = 'success',
            records_scraped = %s
        WHERE run_id = %s
        """,
        (records, run_id),
    )
    logger.info("Run finished — run_id=%s  records=%s", run_id, records)


def fail_run(cursor, run_id: int, error_message: str):
    cursor.execute(
        """
        UPDATE scrape_runs
        SET finished_at = NOW(),
            status = 'failed',
            error_message = %s
        WHERE run_id = %s
        """,
        (error_message[:5000], run_id),
    )
    logger.error("Run failed — run_id=%s  error=%s", run_id, error_message[:200])


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------
def normalize_unit(
    unit: Optional[str], quantity: Any
) -> Tuple[Optional[str], Optional[float]]:
    """
    Kaynak birimi ve miktarı analiz için standartlaştırır.

    Örnekler:
        GRAM + 1000  → ("kg", 1.0)
        GRAM + 400   → ("g", 400.0)
        PIECE + 6    → ("piece", 6.0)
        None + any   → (None, None)
    """
    if unit is None:
        return None, None

    unit_upper = str(unit).strip().upper()

    qty: Optional[float] = None
    if quantity is not None:
        try:
            qty = float(quantity)
        except (TypeError, ValueError):
            logger.warning(
                "normalize_unit: could not parse quantity=%r for unit=%s",
                quantity,
                unit,
            )

    if unit_upper == "GRAM":
        if qty is None:
            logger.warning("normalize_unit: GRAM unit but quantity is None")
            return "g", None
        return ("kg", 1.0) if qty == 1000 else ("g", qty)

    if unit_upper == "PIECE":
        return "piece", qty if qty is not None else 1.0

    # Fallback — unknown unit, keep as-is but lowercase
    return unit.lower(), qty


def standardize_product_name(product_name: Optional[str]) -> Optional[str]:
    """
    Ürün adını karşılaştırma ve gruplama için sadeleştirir.

    Strateji: sayıyı çıkardıktan sonra birim kelimesini sil —
    böylece "\\bg\\b" "göçmen" gibi kelimelerdeki harfleri etkilemez.
    """
    if not product_name:
        return None

    name = product_name.lower().strip()

    # Türkçe → ASCII
    tr_map = {"ı": "i", "ğ": "g", "ü": "u", "ş": "s", "ö": "o", "ç": "c"}
    for old, new in tr_map.items():
        name = name.replace(old, new)

    # "200g", "1kg", "500 gram" gibi sayı+birim bloklarını sil
    name = re.sub(r"\b\d+\s*(?:kg|g|gram|ml|l|lt|adet|demet|paket)\b", " ", name)

    # Sayı olmadan kalan serbest birim kelimelerini de temizle
    standalone_units = r"\b(?:kg|gram|adet|demet|paket)\b"
    name = re.sub(standalone_units, " ", name)

    # Çoklu boşlukları temizle
    name = re.sub(r"\s+", " ", name).strip()

    return name or None

def calculate_price_per_unit(
    price: Optional[float], normalized_quantity: Optional[float]
) -> Optional[float]:
    if price is None or normalized_quantity is None:
        return None

    try:
        price_value = float(price)
        quantity_value = float(normalized_quantity)
    except (TypeError, ValueError):
        return None

    if quantity_value <= 0:
        return None

    return round(price_value / quantity_value, 4)


def build_unit_price_label(normalized_unit: Optional[str]) -> Optional[str]:
    if normalized_unit is None:
        return None
    return f"TRY/{normalized_unit}"
# ---------------------------------------------------------------------------
# Data quality helpers
# ---------------------------------------------------------------------------
def detect_suspicious(
    product_name: Optional[str], price: Optional[float]
) -> Tuple[bool, Optional[str]]:
    name = (product_name or "").lower()

    if price is None:
        return True, "price_null"

    if price <= 0:
        return True, "price_invalid"

    if price > 500:
        return True, "price_too_high"

    has_small_package_hint = (
        " gr" in name
        or "g paket" in name
        or " g paket" in name
        or "paket" in name
    )

    if has_small_package_hint and price > 200:
        return True, "small_package_price_too_high"

    return False, None


# ---------------------------------------------------------------------------
# Insert helpers
# ---------------------------------------------------------------------------
def insert_raw_event(
    cursor, run_id: int, product: dict[str, Any], category_slug: str
) -> int:
    raw_payload = {"category_slug": category_slug, **product}

    cursor.execute(
        """
        INSERT INTO raw_price_events
            (run_id, source_name, product_name, product_url,
             price, currency, raw_payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING event_id
        """,
        (
            run_id,
            SOURCE_NAME,
            product.get("product_name"),
            product.get("product_url"),
            product.get("shown_price_tl"),
            CURRENCY,
            psycopg2.extras.Json(raw_payload),
        ),
    )
    return cursor.fetchone()[0]


def insert_stg_observation(
    cursor, event_id: int, run_id: int, product: dict[str, Any]
) -> int:
    normalized_unit, normalized_quantity = normalize_unit(
        product.get("unit"), product.get("unit_amount")
    )
    price = product.get("shown_price_tl")
    price_per_unit = calculate_price_per_unit(price, normalized_quantity)
    unit_price_label = build_unit_price_label(normalized_unit)
    standardized_name = standardize_product_name(product.get("product_name"))

    regular_price = product.get("regular_price_tl")
    discount_rate = product.get("discount_rate")
    brand_name = product.get("brand_name")
    category_name = product.get("category_name")

    is_suspicious, suspicious_reason = detect_suspicious(
        product.get("product_name"),
        price,
    )

    cursor.execute(
        """
        INSERT INTO stg_price_observations
            (event_id, run_id, source_name, source_product_id, source_sku,
             product_name, product_url, price, currency,
             normalized_unit, normalized_quantity, price_per_unit,unit_price_label,
             standardized_product_name,
             regular_price, discount_rate, brand_name, category_name,
             is_suspicious, suspicious_reason,
             observed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, NOW())
        RETURNING observation_id
        """,
        (
            event_id,
            run_id,
            SOURCE_NAME,
            str(product["product_id"]) if product.get("product_id") is not None else None,
            product.get("sku"),
            product.get("product_name"),
            product.get("product_url"),
            price,
            CURRENCY,
            normalized_unit,
            normalized_quantity,
            price_per_unit,
            unit_price_label,
            standardized_name,
            regular_price,
            discount_rate,
            brand_name,
            category_name,
            is_suspicious,
            suspicious_reason,
        ),
    )
    return cursor.fetchone()[0]


def insert_fact_observation(
    cursor, observation_id: int, run_id: int, product: dict[str, Any]
):
    price = product.get("shown_price_tl")

    is_suspicious, _ = detect_suspicious(
        product.get("product_name"),
        price,
    )

    if is_suspicious:
        logger.info(
            "Skipping suspicious record for fact table — product=%r price=%r",
            product.get("product_name"),
            price,
        )
        return

    normalized_unit, normalized_quantity = normalize_unit(
        product.get("unit"), product.get("unit_amount")
    )
    price_per_unit = calculate_price_per_unit(price, normalized_quantity)
    unit_price_label = build_unit_price_label(normalized_unit)
    standardized_name = standardize_product_name(product.get("product_name"))

    regular_price = product.get("regular_price_tl")
    discount_rate = product.get("discount_rate")
    brand_name = product.get("brand_name")
    category_name = product.get("category_name")

    cursor.execute(
        """
        INSERT INTO fact_price_observations
            (observation_id, run_id, source_name, source_product_id, source_sku,
             product_name, standardized_product_name, product_url,
             normalized_unit, normalized_quantity, price_per_unit, unit_price_label,
             price, currency, observed_at,
             regular_price, discount_rate, brand_name, category_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s)
        """,
        (
            observation_id,
            run_id,
            SOURCE_NAME,
            str(product["product_id"]) if product.get("product_id") is not None else None,
            product.get("sku"),
            product.get("product_name"),
            standardized_name,
            product.get("product_url"),
            normalized_unit,
            normalized_quantity,
            price_per_unit,
            unit_price_label,
            price,
            CURRENCY,
            regular_price,
            discount_rate,
            brand_name,
            category_name,
        ),
    )

# ---------------------------------------------------------------------------
# Per-product insert (kendi transaction'ı var)
# ---------------------------------------------------------------------------
def process_product(
    conn,
    run_id: int,
    product: dict[str, Any],
    category_slug: str,
) -> bool:
    """
    Tek bir ürünü raw → stg → fact olarak yazar.
    Başarılı → True, hatalı → False (ve hata loglanır).

    Her ürün kendi küçük transaction'ında commit edilir.
    Böylece 500. üründe hata olursa önceki 499 kaybolmaz.
    """
    cursor = conn.cursor()
    try:
        event_id = insert_raw_event(cursor, run_id, product, category_slug)
        observation_id = insert_stg_observation(cursor, event_id, run_id, product)
        insert_fact_observation(cursor, observation_id, run_id, product)
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        logger.exception(
            "Product skipped — name=%r",
            product.get("product_name"),
        )
        return False
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def run_pipeline(category_slug: str = DEFAULT_CATEGORY_SLUG):
    conn = None
    run_id = None

    try:
        conn = get_connection()

        # run_id'yi ayrı bir cursor + commit ile aç
        with conn.cursor() as cur:
            run_id = start_run(cur, category_slug)
            conn.commit()

        # Scrape
        scraped_products = get_migros_category_products(category_slug)
        logger.info(
            "Scraped %d products from category=%s",
            len(scraped_products),
            category_slug,
        )

        # Her ürünü tek tek işle — kısmi başarı mümkün
        success_count = 0
        failed_products: list[dict] = []

        for product in scraped_products:
            ok = process_product(conn, run_id, product, category_slug)
            if ok:
                success_count += 1
            else:
                failed_products.append(product)

        # Run'ı kapat
        with conn.cursor() as cur:
            finish_run(cur, run_id, success_count)
            conn.commit()

        # Özet
        logger.info("=" * 50)
        logger.info("Run ID       : %s", run_id)
        logger.info("Category     : %s", category_slug)
        logger.info("Success      : %d", success_count)
        logger.info("Failed       : %d", len(failed_products))

        if failed_products:
            logger.warning("Failed products:")
            for p in failed_products:
                logger.warning("  - %s", p.get("product_name"))

        # Eğer hiç başarılı kayıt yoksa pipeline'ı başarısız say
        if success_count == 0 and scraped_products:
            raise RuntimeError("All products failed to insert — check logs above.")

    except Exception as e:
        if conn is not None:
            conn.rollback()

        if run_id is not None:
            try:
                with conn.cursor() as cur:
                    fail_run(cur, run_id, str(e))
                    conn.commit()
            except Exception:
                conn.rollback()

        logger.exception("Pipeline failed: %s", e)
        raise  # GitHub Actions bu exception'ı yakalar → workflow kırmızı olur

    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    run_pipeline(DEFAULT_CATEGORY_SLUG)
