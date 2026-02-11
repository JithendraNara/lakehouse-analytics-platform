"""Spark parity job for lakehouse-analytics-platform marts.

Run with Databricks or local Spark:
  spark-submit spark_jobs/lakehouse_parity_job.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, expr, to_date


def main() -> None:
    spark = SparkSession.builder.appName("lakehouse-parity-job").getOrCreate()

    users = spark.read.option("header", True).csv("data/raw/users.csv")
    events = spark.read.option("header", True).csv("data/raw/events.csv")
    payments = spark.read.option("header", True).csv("data/raw/payments.csv")

    staging_users = users.select(
        col("user_id").cast("long"),
        col("signup_ts"),
        col("acquisition_channel"),
        col("country"),
        col("plan_tier"),
        col("company_size"),
    )

    staging_events = events.select(
        col("event_id").cast("long"),
        col("user_id").cast("long"),
        col("event_ts"),
        col("event_type"),
        col("experiment_name"),
        col("experiment_variant"),
    )

    staging_payments = payments.select(
        col("payment_id").cast("long"),
        col("user_id").cast("long"),
        col("payment_ts"),
        col("amount_usd").cast("double"),
        col("payment_status"),
    )

    signups = staging_users.groupBy(to_date("signup_ts").alias("metric_date")).count().withColumnRenamed("count", "new_users")
    active = (
        staging_events.filter(col("event_type") == "session_start")
        .groupBy(to_date("event_ts").alias("metric_date"))
        .agg(countDistinct("user_id").alias("active_users"))
    )
    conversions = (
        staging_events.filter(col("event_type") == "subscription_started")
        .groupBy(to_date("event_ts").alias("metric_date"))
        .agg(countDistinct("user_id").alias("paid_conversions"))
    )

    revenue = staging_payments.groupBy(to_date("payment_ts").alias("metric_date")).agg(
        expr("sum(case when payment_status='success' then amount_usd else 0 end)").alias("gross_revenue_usd"),
        expr("sum(case when payment_status='refund' then amount_usd else 0 end)").alias("refunded_usd"),
    )

    daily = signups.join(active, ["metric_date"], "full").join(conversions, ["metric_date"], "full").join(revenue, ["metric_date"], "full")

    daily.write.mode("overwrite").format("parquet").save("data/marts/spark_daily_kpis")
    spark.stop()


if __name__ == "__main__":
    main()
