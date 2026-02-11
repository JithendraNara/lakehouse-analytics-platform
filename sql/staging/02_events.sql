DROP TABLE IF EXISTS staging_events;
CREATE TABLE staging_events AS
SELECT
  CAST(event_id AS INTEGER) AS event_id,
  CAST(user_id AS INTEGER) AS user_id,
  event_ts AS event_ts,
  event_type AS event_type,
  feature_name AS feature_name,
  experiment_name AS experiment_name,
  experiment_variant AS experiment_variant,
  CAST(session_duration_sec AS INTEGER) AS session_duration_sec
FROM raw_events
WHERE user_id IS NOT NULL;
