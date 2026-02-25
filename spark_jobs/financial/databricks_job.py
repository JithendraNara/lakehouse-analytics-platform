"""
Databricks job for financial transaction processing.
Run this on Databricks clusters or locally with Databricks Connect.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, stddev, count, when, lit, to_date, hour, dayofweek,
    percentile_approx, expr
)
from pyspark.sql.window import Window


def create_spark_session():
    """Create Databricks-optimized Spark session."""
    return SparkSession.builder \
        .appName("FinancialTransactionPipeline") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "auto") \
        .config("spark.speculation", "true") \
        .config("spark.databricks.delta.autoOptimize.enabled", "true") \
        .getOrCreate()


def load_from_delta(spark, path: str):
    """Load data from Delta Lake."""
    return spark.read.format("delta").load(path)


def save_to_delta(df, path: str, mode: str = "overwrite"):
    """Save DataFrame to Delta Lake."""
    df.write.format("delta").mode(mode).option("mergeSchema", "true").save(path)


def process_transactions(spark, input_path: str, output_path: str):
    """Main processing pipeline."""
    
    # Load transactions
    df = spark.read \
        .option("header", "true") \
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
        .csv(input_path)
    
    # Add derived features
    df = df.withColumn("txn_date", to_date(col("timestamp")))
    df = df.withColumn("txn_hour", hour(col("timestamp")))
    df = df.withColumn("txn_dayofweek", dayofweek(col("timestamp")))
    
    # Anomaly detection using statistical methods
    stats = df.groupBy("account_id").agg(
        avg("amount").alias("avg_amount"),
        stddev("amount").alias("stddev_amount"),
        count("transaction_id").alias("txn_count")
    )
    
    # Join stats back to main dataframe
    df = df.join(stats, on="account_id", how="left")
    
    # Flag anomalies
    df = df.withColumn(
        "is_anomaly",
        when(col("amount") > col("avg_amount") + 3 * col("stddev_amount"), lit(True))
        .when(col("amount") > 10000, lit(True))
        .otherwise(lit(False))
    )
    
    # Aggregate by day
    daily_agg = df.groupBy("txn_date").agg(
        count("transaction_id").alias("total_transactions"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        count(col("is_anomaly") == True).alias("anomaly_count")
    )
    
    # Write outputs
    save_to_delta(df, f"{output_path}/transactions")
    save_to_delta(daily_agg, f"{output_path}/daily_aggregation")
    
    return {"transactions": df.count(), "anomalies": df.filter(col("is_anomaly") == True).count()}


def main():
    spark = create_spark_session()
    
    # Databricks widget for parameters
    input_path = spark.sparkContext.getConf().get("spark.input.path", "dbfs:/data/transactions")
    output_path = spark.sparkConf().get("spark.output.path", "dbfs:/output/financial")
    
    results = process_transactions(spark, input_path, output_path)
    print(f"Pipeline complete: {results}")


if __name__ == "__main__":
    main()
