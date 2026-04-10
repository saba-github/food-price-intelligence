ALTER TABLE scrape_runs
ADD COLUMN IF NOT EXISTS category_key TEXT;
