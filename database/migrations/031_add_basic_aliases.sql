INSERT INTO dim_product_aliases (
    product_id,
    alias_text,
    normalized_alias
)
SELECT
    candidate.product_id,
    'muz' AS alias_text,
    'muz' AS normalized_alias
FROM (
    SELECT product_id
    FROM dim_products
    WHERE standardized_product_name IN ('muz ithal', 'muz yerli')
    ORDER BY
        CASE standardized_product_name
            WHEN 'muz yerli' THEN 1
            WHEN 'muz ithal' THEN 2
            ELSE 3
        END,
        product_id
    LIMIT 1
) AS candidate
WHERE NOT EXISTS (
    SELECT 1
    FROM dim_product_aliases
    WHERE normalized_alias = 'muz'
);

INSERT INTO dim_product_aliases (
    product_id,
    alias_text,
    normalized_alias
)
SELECT
    candidate.product_id,
    'domates' AS alias_text,
    'domates' AS normalized_alias
FROM (
    SELECT product_id
    FROM dim_products
    WHERE standardized_product_name LIKE '%domates%'
    ORDER BY
        CASE
            WHEN standardized_product_name = 'domates' THEN 1
            WHEN standardized_product_name LIKE 'domates %' THEN 2
            ELSE 3
        END,
        product_id
    LIMIT 1
) AS candidate
WHERE NOT EXISTS (
    SELECT 1
    FROM dim_product_aliases
    WHERE normalized_alias = 'domates'
);
