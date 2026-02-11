DROP TABLE IF EXISTS marts_customer_health;
CREATE TABLE marts_customer_health AS
WITH sessions_30d AS (
  SELECT
    user_id,
    COUNT(*) AS sessions_last_30d
  FROM staging_events
  WHERE event_type = 'session_start'
    AND datetime(event_ts) >= datetime('now', '-30 day')
  GROUP BY 1
),
revenue AS (
  SELECT
    user_id,
    SUM(CASE WHEN payment_status = 'success' THEN amount_usd ELSE 0 END) -
    SUM(CASE WHEN payment_status = 'refund' THEN amount_usd ELSE 0 END) AS net_revenue_usd
  FROM staging_payments
  GROUP BY 1
),
open_tickets AS (
  SELECT
    user_id,
    COUNT(*) AS open_ticket_count
  FROM staging_support_tickets
  WHERE resolved_ts IS NULL
  GROUP BY 1
),
churn_signals AS (
  SELECT DISTINCT user_id, 1 AS churn_signal
  FROM staging_events
  WHERE event_type = 'churned'
)
SELECT
  u.user_id,
  u.signup_ts,
  u.acquisition_channel,
  u.plan_tier,
  COALESCE(s.sessions_last_30d, 0) AS sessions_last_30d,
  ROUND(COALESCE(r.net_revenue_usd, 0), 2) AS net_revenue_usd,
  COALESCE(o.open_ticket_count, 0) AS open_ticket_count,
  COALESCE(c.churn_signal, 0) AS churn_signal,
  CAST(MAX(
    0,
    MIN(
      100,
      55
      + MIN(COALESCE(s.sessions_last_30d, 0), 20) * 2
      + MIN(COALESCE(r.net_revenue_usd, 0) / 20.0, 20)
      - COALESCE(o.open_ticket_count, 0) * 7
      - COALESCE(c.churn_signal, 0) * 25
    )
  ) AS INTEGER) AS customer_health_score
FROM staging_users u
LEFT JOIN sessions_30d s USING (user_id)
LEFT JOIN revenue r USING (user_id)
LEFT JOIN open_tickets o USING (user_id)
LEFT JOIN churn_signals c USING (user_id);
