DROP TABLE IF EXISTS staging_users;
CREATE TABLE staging_users AS
SELECT
  CAST(user_id AS INTEGER) AS user_id,
  signup_ts AS signup_ts,
  acquisition_channel AS acquisition_channel,
  country AS country,
  plan_tier AS plan_tier,
  company_size AS company_size
FROM raw_users
WHERE user_id IS NOT NULL;
