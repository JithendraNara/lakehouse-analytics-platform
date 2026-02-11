DROP TABLE IF EXISTS marts_experiment_performance;
CREATE TABLE marts_experiment_performance AS
WITH exposures AS (
  SELECT
    experiment_name,
    experiment_variant,
    user_id
  FROM staging_events
  WHERE experiment_name IS NOT NULL
  GROUP BY 1, 2, 3
),
conversions AS (
  SELECT DISTINCT user_id
  FROM staging_events
  WHERE event_type = 'subscription_started'
),
revenue_per_user AS (
  SELECT
    user_id,
    SUM(CASE WHEN payment_status = 'success' THEN amount_usd ELSE 0 END)
      - SUM(CASE WHEN payment_status = 'refund' THEN amount_usd ELSE 0 END) AS net_revenue_usd
  FROM staging_payments
  GROUP BY 1
)
SELECT
  e.experiment_name,
  e.experiment_variant,
  COUNT(DISTINCT e.user_id) AS users_exposed,
  COUNT(DISTINCT c.user_id) AS users_converted,
  ROUND(
    CAST(COUNT(DISTINCT c.user_id) AS REAL) / NULLIF(COUNT(DISTINCT e.user_id), 0),
    4
  ) AS conversion_rate,
  ROUND(COALESCE(AVG(r.net_revenue_usd), 0), 2) AS avg_revenue_per_user
FROM exposures e
LEFT JOIN conversions c ON e.user_id = c.user_id
LEFT JOIN revenue_per_user r ON e.user_id = r.user_id
GROUP BY 1, 2
ORDER BY users_exposed DESC;
