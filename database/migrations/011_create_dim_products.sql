CREATE TABLE IF NOT EXISTS dim_products (
    product_id SERIAL PRIMARY KEY,

    standardized_product_name TEXT UNIQUE,
    canonical_name TEXT,

    category_level_1 TEXT,
    category_level_2 TEXT,

    created_at TIMESTAMP DEFAULT NOW()
);
