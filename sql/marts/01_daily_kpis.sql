DROP TABLE IF EXISTS marts_daily_kpis;
CREATE TABLE marts_daily_kpis AS
WITH signups AS (
  SELECT DATE(signup_ts) AS metric_date, COUNT(*) AS new_users
  FROM staging_users
  GROUP BY 1
),
active_users AS (
  SELECT DATE(event_ts) AS metric_date, COUNT(DISTINCT user_id) AS active_users
  FROM staging_events
  WHERE event_type = 'session_start'
  GROUP BY 1
),
conversions AS (
  SELECT DATE(event_ts) AS metric_date, COUNT(DISTINCT user_id) AS paid_conversions
  FROM staging_events
  WHERE event_type = 'subscription_started'
  GROUP BY 1
),
revenue AS (
  SELECT
    DATE(payment_ts) AS metric_date,
    SUM(CASE WHEN payment_status = 'success' THEN amount_usd ELSE 0 END) AS gross_revenue_usd,
    SUM(CASE WHEN payment_status = 'refund' THEN amount_usd ELSE 0 END) AS refunded_usd
  FROM staging_payments
  GROUP BY 1
),
tickets AS (
  SELECT DATE(created_ts) AS metric_date, COUNT(*) AS tickets_opened
  FROM staging_support_tickets
  GROUP BY 1
),
all_dates AS (
  SELECT metric_date FROM signups
  UNION SELECT metric_date FROM active_users
  UNION SELECT metric_date FROM conversions
  UNION SELECT metric_date FROM revenue
  UNION SELECT metric_date FROM tickets
)
SELECT
  d.metric_date,
  COALESCE(s.new_users, 0) AS new_users,
  COALESCE(a.active_users, 0) AS active_users,
  COALESCE(c.paid_conversions, 0) AS paid_conversions,
  COALESCE(r.gross_revenue_usd, 0) AS gross_revenue_usd,
  COALESCE(r.refunded_usd, 0) AS refunded_usd,
  COALESCE(r.gross_revenue_usd, 0) - COALESCE(r.refunded_usd, 0) AS net_revenue_usd,
  COALESCE(t.tickets_opened, 0) AS tickets_opened,
  CASE
    WHEN COALESCE(s.new_users, 0) = 0 THEN 0
    ELSE CAST(COALESCE(c.paid_conversions, 0) AS REAL) / s.new_users
  END AS conversion_rate,
  CASE
    WHEN COALESCE(r.gross_revenue_usd, 0) = 0 THEN 0
    ELSE COALESCE(r.refunded_usd, 0) / r.gross_revenue_usd
  END AS refund_rate
FROM all_dates d
LEFT JOIN signups s USING (metric_date)
LEFT JOIN active_users a USING (metric_date)
LEFT JOIN conversions c USING (metric_date)
LEFT JOIN revenue r USING (metric_date)
LEFT JOIN tickets t USING (metric_date)
ORDER BY d.metric_date;
