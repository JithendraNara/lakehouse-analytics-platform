"""
PySpark pipeline for processing financial transactions.
Handles 10M+ records with anomaly detection and aggregation.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, stddev, count, min as spark_min, max as spark_max,
    when, lit, row_number, window, to_timestamp, hour, dayofweek
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FinancialPipeline:
    def __init__(self, app_name="FinancialDataPipeline"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.speculation", "true") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

    def load_transactions(self, path, format="csv", header=True):
        """Load transaction data from S3 or local."""
        logger.info(f"Loading transactions from {path}")
        
        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("account_id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("amount", DoubleType(), False),
            StructField("merchant_category", StringType(), True),
            StructField("location", StringType(), True),
            StructField("currency", StringType(), True),
        ])
        
        df = self.spark.read \
            .format(format) \
            .schema(schema) \
            .option("header", header) \
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
            .load(path)
        
        logger.info(f"Loaded {df.count()} transactions")
        return df

    def detect_anomalies(self, df):
        """Detect anomalous transactions based on multiple rules."""
        logger.info("Running anomaly detection...")
        
        # Calculate rolling statistics per account
        window_spec = Window.partitionBy("account_id") \
            .orderBy("timestamp") \
            .rowsBetween(-1000, 0)
        
        df_with_stats = df.withColumn(
            "avg_amount", avg("amount").over(window_spec)
        ).withColumn(
            "stddev_amount", stddev("amount").over(window_spec)
        ).withColumn(
            "txn_count", count("transaction_id").over(window_spec)
        )
        
        # Anomaly rules
        anomalies = df_with_stats.filter(
            # High amount anomaly (3 std devs above mean)
            (col("amount") > col("avg_amount") + 3 * col("stddev_amount")) |
            # Unusual velocity (>10 txns in window)
            (col("txn_count") > 10) |
            # Very high single transaction (>10000)
            (col("amount") > 10000)
        ).withColumn(
            "anomaly_type",
            when(col("amount") > col("avg_amount") + 3 * col("stddev_amount"), "high_amount")
            .when(col("txn_count") > 10, "high_velocity")
            .when(col("amount") > 10000, "very_high_amount")
            .otherwise("other")
        )
        
        logger.info(f"Detected {anomalies.count()} anomalies")
        return anomalies

    def aggregate_daily(self, df):
        """Generate daily aggregation summaries."""
        logger.info("Computing daily aggregations...")
        
        daily_agg = df.groupBy(
            to_timestamp(col("timestamp"), "yyyy-MM-dd").alias("date"),
            "account_id"
        ).agg(
            count("transaction_id").alias("txn_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
            spark_min("amount").alias("min_amount"),
            spark_max("amount").alias("max_amount"),
            stddev("amount").alias("stddev_amount")
        ).orderBy("date", "account_id")
        
        return daily_agg

    def aggregate_by_merchant(self, df):
        """Aggregate by merchant category."""
        return df.groupBy("merchant_category").agg(
            count("transaction_id").alias("txn_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount")
        ).orderBy(col("txn_count").desc())

    def write_to_s3(self, df, output_path, partition_cols=None):
        """Write DataFrame to S3 as Parquet."""
        logger.info(f"Writing to {output_path}")
        
        writer = df.write \
            .mode("overwrite") \
            .format("parquet")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.save(output_path)
        logger.info(f"Successfully wrote to {output_path}")

    def run_pipeline(self, input_path, output_path):
        """Execute full pipeline."""
        logger.info("Starting pipeline...")
        
        # Load data
        df = self.load_transactions(input_path)
        
        # Detect anomalies
        anomalies = self.detect_anomalies(df)
        self.write_to_s3(anomalies, f"{output_path}/anomalies", ["date"])
        
        # Daily aggregation
        daily = self.aggregate_daily(df)
        self.write_to_s3(daily, f"{output_path}/daily_agg", ["date"])
        
        # Merchant aggregation
        merchant = self.aggregate_by_merchant(df)
        self.write_to_s3(merchant, f"{output_path}/merchant_agg")
        
        logger.info("Pipeline complete!")
        return {
            "total_transactions": df.count(),
            "anomalies": anomalies.count()
        }


def main():
    if len(sys.argv) < 3:
        print("Usage: python -m pipelines.transaction_processor --input <path> --output <path>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    pipeline = FinancialPipeline()
    results = pipeline.run_pipeline(input_path, output_path)
    print(f"Pipeline results: {results}")


if __name__ == "__main__":
    main()
