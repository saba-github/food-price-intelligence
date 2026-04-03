ALTER TABLE fact_price_observations
ADD CONSTRAINT fk_product
FOREIGN KEY (product_id)
REFERENCES dim_products(product_id);
