"""
AWS S3 and Athena integration for financial data pipeline.
"""

import boto3
import pandas as pd
import logging
from typing import List, Dict, Optional
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3Client:
    """S3 client for uploading processed data."""
    
    def __init__(self, bucket: str, region: str = "us-east-1"):
        self.s3 = boto3.client("s3", region_name=region)
        self.bucket = bucket
        self.region = region
    
    def upload_parquet(self, local_path: str, s3_key: str) -> str:
        """Upload Parquet file to S3."""
        logger.info(f"Uploading {local_path} to s3://{self.bucket}/{s3_key}")
        self.s3.upload_file(local_path, self.bucket, s3_key)
        return f"s3://{self.bucket}/{s3_key}"
    
    def upload_dataframe(self, df: pd.DataFrame, s3_key: str, partition_cols: List[str] = None) -> str:
        """Upload DataFrame as Parquet to S3."""
        import tempfile
        import os
        
        # Write to temp file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        df.to_parquet(temp_file.name, index=False)
        
        try:
            return self.upload_parquet(temp_file.name, s3_key)
        finally:
            os.unlink(temp_file.name)
    
    def list_objects(self, prefix: str = "") -> List[Dict]:
        """List objects in S3 bucket with prefix."""
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        return response.get("Contents", [])


class AthenaClient:
    """Athena client for querying data."""
    
    def __init__(self, database: str, region: str = "us-east-1"):
        self.athena = boto3.client("athena", region_name=region)
        self.database = database
        self.region = region
    
    def execute_query(self, query: str) -> str:
        """Execute Athena query and return query execution ID."""
        logger.info(f"Executing query: {query[:100]}...")
        response = self.athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": f"s3://{self.database}-results/"}
        )
        return response["QueryExecutionId"]
    
    def get_results(self, query_execution_id: str) -> pd.DataFrame:
        """Get results of executed query."""
        # Wait for completion
        while True:
            result = self.athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = result["QueryExecution"]["Status"]["State"]
            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break
        
        if status != "SUCCEEDED":
            raise Exception(f"Query failed: {result['QueryExecution']['Status']['StateChangeReason']}")
        
        # Get results
        result = self.athena.get_query_results(QueryExecutionId=query_execution_id)
        
        # Parse to DataFrame
        columns = [col["Label"] for col in result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
        rows = []
        for row in result["ResultSet"]["Rows"][1:]:  # Skip header
            rows.append([datum.get("VarCharValue") for datum in row["Data"]])
        
        return pd.DataFrame(rows, columns=columns)
    
    def create_table(self, table_name: str, s3_location: str, schema: Dict[str, str]):
        """Create Athena table from S3 data."""
        columns = ", ".join([f"{name} {dtype}" for name, dtype in schema.items()])
        
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns}
        )
        PARTITIONED BY (year int, month int, day int)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS PARQUET
        LOCATION '{s3_location}'
        """
        
        self.execute_query(query)
        logger.info(f"Created table {table_name}")


# Example usage
if __name__ == "__main__":
    # Initialize clients
    s3 = S3Bucket("financial-data-pipeline")
    athena = AthenaClient("financial_db")
    
    # Example: Query daily transaction volume
    query = """
    SELECT 
        day,
        COUNT(*) as transaction_count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM financial_db.transactions
    GROUP BY day
    ORDER BY day DESC
    LIMIT 30
    """
    
    execution_id = athena.execute_query(query)
    results = athena.get_results(execution_id)
    print(results)
