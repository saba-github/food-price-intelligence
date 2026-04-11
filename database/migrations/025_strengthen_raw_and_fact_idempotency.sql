DROP INDEX IF EXISTS uniq_fact_event;

ALTER TABLE fact_price_observations
ALTER COLUMN event_id SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uniq_fact_event_id
ON fact_price_observations (
    event_id
); 