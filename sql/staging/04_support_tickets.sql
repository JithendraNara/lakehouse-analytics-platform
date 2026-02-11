DROP TABLE IF EXISTS staging_support_tickets;
CREATE TABLE staging_support_tickets AS
SELECT
  CAST(ticket_id AS INTEGER) AS ticket_id,
  CAST(user_id AS INTEGER) AS user_id,
  created_ts AS created_ts,
  resolved_ts AS resolved_ts,
  severity AS severity,
  CAST(csat_score AS INTEGER) AS csat_score
FROM raw_support_tickets
WHERE user_id IS NOT NULL;
