DROP TABLE IF EXISTS marts_channel_performance;
CREATE TABLE marts_channel_performance AS
WITH paid_users AS (
  SELECT DISTINCT user_id
  FROM staging_payments
  WHERE payment_status = 'success'
),
revenue_by_user AS (
  SELECT
    user_id,
    SUM(CASE WHEN payment_status = 'success' THEN amount_usd ELSE 0 END) -
    SUM(CASE WHEN payment_status = 'refund' THEN amount_usd ELSE 0 END) AS net_revenue_usd
  FROM staging_payments
  GROUP BY 1
)
SELECT
  u.acquisition_channel,
  COUNT(*) AS signups,
  COUNT(DISTINCT p.user_id) AS paying_users,
  ROUND(
    CAST(COUNT(DISTINCT p.user_id) AS REAL) / NULLIF(COUNT(*), 0),
    4
  ) AS paid_conversion_rate,
  ROUND(COALESCE(SUM(r.net_revenue_usd), 0), 2) AS net_revenue_usd,
  ROUND(COALESCE(SUM(r.net_revenue_usd), 0) / NULLIF(COUNT(*), 0), 2) AS arpu
FROM staging_users u
LEFT JOIN paid_users p ON u.user_id = p.user_id
LEFT JOIN revenue_by_user r ON u.user_id = r.user_id
GROUP BY 1
ORDER BY net_revenue_usd DESC;
