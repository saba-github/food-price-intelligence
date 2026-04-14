
INSERT INTO dim_product_aliases (
    product_id,
    alias_text,
    normalized_alias
)
SELECT
    candidate.product_id,
    'süt' AS alias_text,
    'sut' AS normalized_alias
FROM (
    SELECT product_id
    FROM dim_products
    WHERE standardized_product_name IN (
        'sut',
        'uht sut',
        'tam yagli sut',
        'yarim yagli sut'
    )
       OR standardized_product_name LIKE '%sut%'
    ORDER BY
        CASE
            WHEN standardized_product_name = 'sut' THEN 1
            WHEN standardized_product_name = 'uht sut' THEN 2
            WHEN standardized_product_name = 'tam yagli sut' THEN 3
            WHEN standardized_product_name = 'yarim yagli sut' THEN 4
            WHEN standardized_product_name LIKE 'sut %' THEN 5
            ELSE 6
        END,
        product_id
    LIMIT 1
) AS candidate
ON CONFLICT (normalized_alias) DO NOTHING;

INSERT INTO dim_product_aliases (
    product_id,
    alias_text,
    normalized_alias
)
SELECT
    candidate.product_id,
    'yoğurt' AS alias_text,
    'yogurt' AS normalized_alias
FROM (
    SELECT product_id
    FROM dim_products
    WHERE standardized_product_name IN (
        'yogurt',
        'ev tipi yogurt',
        'suzme yogurt'
    )
       OR standardized_product_name LIKE '%yogurt%'
    ORDER BY
        CASE
            WHEN standardized_product_name = 'yogurt' THEN 1
            WHEN standardized_product_name = 'ev tipi yogurt' THEN 2
            WHEN standardized_product_name = 'suzme yogurt' THEN 3
            WHEN standardized_product_name LIKE 'yogurt %' THEN 4
            ELSE 5
        END,
        product_id
    LIMIT 1
) AS candidate
ON CONFLICT (normalized_alias) DO NOTHING;

INSERT INTO dim_product_aliases (
    product_id,
    alias_text,
    normalized_alias
)
SELECT
    candidate.product_id,
    'peynir' AS alias_text,
    'peynir' AS normalized_alias
FROM (
    SELECT product_id
    FROM dim_products
    WHERE standardized_product_name IN (
        'peynir',
        'beyaz peynir',
        'kasar peynir',
        'kasar peyniri'
    )
       OR standardized_product_name LIKE '%peynir%'
    ORDER BY
        CASE
            WHEN standardized_product_name = 'peynir' THEN 1
            WHEN standardized_product_name = 'beyaz peynir' THEN 2
            WHEN standardized_product_name = 'kasar peynir' THEN 3
            WHEN standardized_product_name = 'kasar peyniri' THEN 4
            WHEN standardized_product_name LIKE 'peynir %' THEN 5
            ELSE 6
        END,
        product_id
    LIMIT 1
) AS candidate
ON CONFLICT (normalized_alias) DO NOTHING;

INSERT INTO dim_product_aliases (
    product_id,
    alias_text,
    normalized_alias
)
SELECT
    candidate.product_id,
    'ekmek' AS alias_text,
    'ekmek' AS normalized_alias
FROM (
    SELECT product_id
    FROM dim_products
    WHERE standardized_product_name IN (
        'ekmek',
        'beyaz ekmek',
        'tam bugday ekmek',
        'cavdar ekmek'
    )
       OR standardized_product_name LIKE '%ekmek%'
    ORDER BY
        CASE
            WHEN standardized_product_name = 'ekmek' THEN 1
            WHEN standardized_product_name = 'beyaz ekmek' THEN 2
            WHEN standardized_product_name = 'tam bugday ekmek' THEN 3
            WHEN standardized_product_name = 'cavdar ekmek' THEN 4
            WHEN standardized_product_name LIKE 'ekmek %' THEN 5
            ELSE 6
        END,
        product_id
    LIMIT 1
) AS candidate
ON CONFLICT (normalized_alias) DO NOTHING;
