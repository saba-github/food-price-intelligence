DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_product'
    ) THEN
        ALTER TABLE fact_price_observations
        ADD CONSTRAINT fk_product
        FOREIGN KEY (product_id)
        REFERENCES dim_products(product_id);
    END IF;
END $$;
