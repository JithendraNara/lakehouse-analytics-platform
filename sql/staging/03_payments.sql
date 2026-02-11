DROP TABLE IF EXISTS staging_payments;
CREATE TABLE staging_payments AS
SELECT
  CAST(payment_id AS INTEGER) AS payment_id,
  CAST(user_id AS INTEGER) AS user_id,
  payment_ts AS payment_ts,
  CAST(amount_usd AS REAL) AS amount_usd,
  payment_status AS payment_status,
  invoice_type AS invoice_type
FROM raw_payments
WHERE user_id IS NOT NULL;
