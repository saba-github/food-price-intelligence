INSERT INTO dim_product_aliases (product_id, alias_text, normalized_alias)
SELECT
    candidate.product_id,
    'yumurta' AS alias_text,
    'yumurta' AS normalized_alias
FROM (
    SELECT product_id
    FROM dim_products
    WHERE standardized_product_name IN (
        'yumurta',
        'gezen tavuk yumurta',
        'organik yumurta'
    )
       OR standardized_product_name LIKE '%yumurta%'
    ORDER BY
        CASE
            WHEN standardized_product_name = 'yumurta' THEN 1
            WHEN standardized_product_name LIKE 'yumurta %' THEN 2
            ELSE 3
        END,
        product_id
    LIMIT 1
) AS candidate
ON CONFLICT (normalized_alias) DO NOTHING;


INSERT INTO dim_product_aliases (product_id, alias_text, normalized_alias)
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


INSERT INTO dim_product_aliases (product_id, alias_text, normalized_alias)
SELECT
    candidate.product_id,
    'elma' AS alias_text,
    'elma' AS normalized_alias
FROM (
    SELECT product_id
    FROM dim_products
    WHERE standardized_product_name IN (
        'elma',
        'elma starking',
        'elma granny smith'
    )
       OR standardized_product_name LIKE '%elma%'
    ORDER BY
        CASE
            WHEN standardized_product_name = 'elma' THEN 1
            WHEN standardized_product_name LIKE 'elma %' THEN 2
            ELSE 3
        END,
        product_id
    LIMIT 1
) AS candidate
ON CONFLICT (normalized_alias) DO NOTHING;


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
        'salatalik yerli',
        'cengel koy salatalik'
    )
       OR standardized_product_name LIKE '%salatalik%'
    ORDER BY
        CASE
            WHEN standardized_product_name = 'salatalik' THEN 1
            WHEN standardized_product_name LIKE 'salatalik %' THEN 2
            ELSE 3
        END,
        product_id
    LIMIT 1
) AS candidate
ON CONFLICT (normalized_alias) DO NOTHING;


INSERT INTO dim_product_aliases (product_id, alias_text, normalized_alias)
SELECT
    candidate.product_id,
    'limon' AS alias_text,
    'limon' AS normalized_alias
FROM (
    SELECT product_id
    FROM dim_products
    WHERE standardized_product_name IN (
        'limon'
    )
       OR standardized_product_name LIKE '%limon%'
    ORDER BY
        CASE
            WHEN standardized_product_name = 'limon' THEN 1
            WHEN standardized_product_name LIKE 'limon %' THEN 2
            ELSE 3
        END,
        product_id
    LIMIT 1
) AS candidate
ON CONFLICT (normalized_alias) DO NOTHING;


INSERT INTO dim_product_aliases (product_id, alias_text, normalized_alias)
SELECT
    candidate.product_id,
    'su' AS alias_text,
    'su' AS normalized_alias
FROM (
    SELECT product_id
    FROM dim_products
    WHERE standardized_product_name IN (
        'su',
        'icme suyu',
        'dogal kaynak suyu',
        'maden suyu'
    )
       OR standardized_product_name LIKE '% su%'
       OR standardized_product_name LIKE 'su %'
       OR standardized_product_name = 'su'
    ORDER BY
        CASE
            WHEN standardized_product_name = 'su' THEN 1
            WHEN standardized_product_name = 'icme suyu' THEN 2
            WHEN standardized_product_name = 'dogal kaynak suyu' THEN 3
            ELSE 4
        END,
        product_id
    LIMIT 1
) AS candidate
ON CONFLICT (normalized_alias) DO NOTHING;


INSERT INTO dim_product_aliases (product_id, alias_text, normalized_alias)
SELECT
    candidate.product_id,
    'çay' AS alias_text,
    'cay' AS normalized_alias
FROM (
    SELECT product_id
    FROM dim_products
    WHERE standardized_product_name IN (
        'cay',
        'siyah cay',
        'demlik cay'
    )
       OR standardized_product_name LIKE '%cay%'
    ORDER BY
        CASE
            WHEN standardized_product_name = 'cay' THEN 1
            WHEN standardized_product_name = 'siyah cay' THEN 2
            ELSE 3
        END,
        product_id
    LIMIT 1
) AS candidate
ON CONFLICT (normalized_alias) DO NOTHING;


INSERT INTO dim_product_aliases (product_id, alias_text, normalized_alias)
SELECT
    candidate.product_id,
    'kahve' AS alias_text,
    'kahve' AS normalized_alias
FROM (
    SELECT product_id
    FROM dim_products
    WHERE standardized_product_name IN (
        'kahve',
        'turk kahvesi',
        'filtre kahve',
        'hazir kahve'
    )
       OR standardized_product_name LIKE '%kahve%'
    ORDER BY
        CASE
            WHEN standardized_product_name = 'kahve' THEN 1
            WHEN standardized_product_name = 'turk kahvesi' THEN 2
            WHEN standardized_product_name = 'filtre kahve' THEN 3
            ELSE 4
        END,
        product_id
    LIMIT 1
) AS candidate
ON CONFLICT (normalized_alias) DO NOTHING;


INSERT INTO dim_product_aliases (product_id, alias_text, normalized_alias)
SELECT
    candidate.product_id,
    'pirinç' AS alias_text,
    'pirinc' AS normalized_alias
FROM (
    SELECT product_id
    FROM dim_products
    WHERE standardized_product_name IN (
        'pirinc',
        'baldo pirinc',
        'osmancik pirinc'
    )
       OR standardized_product_name LIKE '%pirinc%'
    ORDER BY
        CASE
            WHEN standardized_product_name = 'pirinc' THEN 1
            WHEN standardized_product_name = 'baldo pirinc' THEN 2
            WHEN standardized_product_name = 'osmancik pirinc' THEN 3
            ELSE 4
        END,
        product_id
    LIMIT 1
) AS candidate
ON CONFLICT (normalized_alias) DO NOTHING;


INSERT INTO dim_product_aliases (product_id, alias_text, normalized_alias)
SELECT
    candidate.product_id,
    'makarna' AS alias_text,
    'makarna' AS normalized_alias
FROM (
    SELECT product_id
    FROM dim_products
    WHERE standardized_product_name IN (
        'makarna',
        'spagetti',
        'burgu makarna',
        'kalem makarna'
    )
       OR standardized_product_name LIKE '%makarna%'
    ORDER BY
        CASE
            WHEN standardized_product_name = 'makarna' THEN 1
            WHEN standardized_product_name = 'spagetti' THEN 2
            WHEN standardized_product_name = 'burgu makarna' THEN 3
            ELSE 4
        END,
        product_id
    LIMIT 1
) AS candidate
ON CONFLICT (normalized_alias) DO NOTHING;


INSERT INTO dim_product_aliases (product_id, alias_text, normalized_alias)
SELECT
    candidate.product_id,
    'patates' AS alias_text,
    'patates' AS normalized_alias
FROM (
    SELECT product_id
    FROM dim_products
    WHERE standardized_product_name IN (
        'patates'
    )
       OR standardized_product_name LIKE '%patates%'
    ORDER BY
        CASE
            WHEN standardized_product_name = 'patates' THEN 1
            WHEN standardized_product_name LIKE 'patates %' THEN 2
            ELSE 3
        END,
        product_id
    LIMIT 1
) AS candidate
ON CONFLICT (normalized_alias) DO NOTHING;


INSERT INTO dim_product_aliases (product_id, alias_text, normalized_alias)
SELECT
    candidate.product_id,
    'soğan' AS alias_text,
    'sogan' AS normalized_alias
FROM (
    SELECT product_id
    FROM dim_products
    WHERE standardized_product_name IN (
        'sogan',
        'kuru sogan'
    )
       OR standardized_product_name LIKE '%sogan%'
    ORDER BY
        CASE
            WHEN standardized_product_name = 'sogan' THEN 1
            WHEN standardized_product_name = 'kuru sogan' THEN 2
            ELSE 3
        END,
        product_id
    LIMIT 1
) AS candidate
ON CONFLICT (normalized_alias) DO NOTHING;
