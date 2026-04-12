CREATE TABLE IF NOT EXISTS dim_product_aliases (
    alias_id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES dim_products(product_id),
    alias_text TEXT NOT NULL,
    normalized_alias TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_dim_product_aliases_normalized_alias'
    ) THEN
        ALTER TABLE dim_product_aliases
        ADD CONSTRAINT uq_dim_product_aliases_normalized_alias
        UNIQUE (normalized_alias);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_dim_product_aliases_product_id
    ON dim_product_aliases(product_id);
