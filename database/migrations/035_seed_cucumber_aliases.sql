INSERT INTO dim_product_aliases (product_id, alias_text, normalized_alias)
SELECT
    candidate.product_id,
    'salatalık' AS alias_text,
    'salatalik' AS normalized_alias
FROM (
    SELECT product_id
    FROM dim_products
    WHERE standardized_product_name IN (
        'salatalik',
        'hiyar',
        'hiyar kg',
        'hiyar badem paket kg',
        'salatalik yerli',
        'cengel koy salatalik'
    )
       OR standardized_product_name LIKE '%salatalik%'
       OR standardized_product_name LIKE '%hiyar%'
    ORDER BY
        CASE
            WHEN standardized_product_name = 'salatalik' THEN 1
            WHEN standardized_product_name = 'hiyar' THEN 2
            WHEN standardized_product_name = 'hiyar kg' THEN 3
            WHEN standardized_product_name LIKE 'hiyar %' THEN 4
            WHEN standardized_product_name LIKE '%salatalik%' THEN 5
            ELSE 6
        END,
        product_id
    LIMIT 1
) AS candidate
ON CONFLICT (normalized_alias) DO NOTHING;


INSERT INTO dim_product_aliases (product_id, alias_text, normalized_alias)
SELECT
    candidate.product_id,
    'hıyar' AS alias_text,
    'hiyar' AS normalized_alias
FROM (
    SELECT product_id
    FROM dim_products
    WHERE standardized_product_name IN (
        'hiyar',
        'hiyar kg',
        'hiyar badem paket kg',
        'salatalik'
    )
       OR standardized_product_name LIKE '%hiyar%'
       OR standardized_product_name LIKE '%salatalik%'
    ORDER BY
        CASE
            WHEN standardized_product_name = 'hiyar' THEN 1
            WHEN standardized_product_name = 'hiyar kg' THEN 2
            WHEN standardized_product_name LIKE 'hiyar %' THEN 3
            WHEN standardized_product_name = 'salatalik' THEN 4
            ELSE 5
        END,
        product_id
    LIMIT 1
) AS candidate
ON CONFLICT (normalized_alias) DO NOTHING;


INSERT INTO dim_product_aliases (product_id, alias_text, normalized_alias)
SELECT
    candidate.product_id,
    'salatalik' AS alias_text,
    'salatalik' AS normalized_alias
FROM (
    SELECT product_id
    FROM dim_products
    WHERE standardized_product_name IN (
        'salatalik',
        'hiyar',
        'hiyar kg',
        'hiyar badem paket kg'
    )
       OR standardized_product_name LIKE '%salatalik%'
       OR standardized_product_name LIKE '%hiyar%'
    ORDER BY
        CASE
            WHEN standardized_product_name = 'salatalik' THEN 1
            WHEN standardized_product_name = 'hiyar' THEN 2
            WHEN standardized_product_name = 'hiyar kg' THEN 3
            ELSE 4
        END,
        product_id
    LIMIT 1
) AS candidate
ON CONFLICT (normalized_alias) DO NOTHING;
