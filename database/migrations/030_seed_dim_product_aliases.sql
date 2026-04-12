INSERT INTO dim_product_aliases (
    product_id,
    alias_text,
    normalized_alias
)
SELECT
    product_id,
    standardized_product_name AS alias_text,
    standardized_product_name AS normalized_alias
FROM dim_products
WHERE standardized_product_name IS NOT NULL
  AND standardized_product_name <> ''
  AND NOT EXISTS (
      SELECT 1
      FROM dim_product_aliases dpa
      WHERE dpa.normalized_alias = dim_products.standardized_product_name
  );
