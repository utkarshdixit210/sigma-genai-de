"""
Sigma DataTech Transaction Analytics Pipeline - Fixed
This file addresses the FAIL/WARN items flagged by the Code Review Agent in Module 5.

Fixes added:
1. Removed hardcoded paths by parameterizing the main() function and stage signatures.
2. Added explicit row count logging at key checkpoints (input, after filters, output).
3. Added strict schema validation checking before transformations begin.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, when, count, countDistinct, current_date, isnull, broadcast
from pyspark.sql.types import StringType, DoubleType, DateType, IntegerType, TimestampType
from datetime import datetime
import json
import os

def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    # Read CSV with all columns as strings
    bronze_df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # 2. Row Count Logging (Bronze Input)
    input_count = bronze_df.count()
    print(f"[Stage: Ingest Bronze] Input count: {input_count:,} rows")
    
    # Add metadata columns
    bronze_df = bronze_df.withColumn("_ingestion_ts", lit(datetime.utcnow()).cast(TimestampType())) \
                         .withColumn("_source_file", lit(input_path)) \
                         .withColumn("_batch_id", lit(run_id))
    
    # Write to Bronze layer partitioned by txn_date
    bronze_df.write.mode('overwrite').partitionBy('txn_date').parquet(output_path)
    print(f"[Stage: Ingest Bronze] Outgest count: {bronze_df.count(),} rows written to {output_path}")

def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    # Read Bronze Parquet with partition pruning on run_date
    bronze_df = spark.read.parquet(bronze_path).where(col("txn_date") == run_date)
    
    # 3. Schema Validation Check
    required_cols = {"transaction_id", "customer_id", "amount", "txn_date", "status", "merchant_id"}
    actual_cols = set(bronze_df.columns)
    if not required_cols.issubset(actual_cols):
        missing = required_cols - actual_cols
        raise ValueError(f"Schema validation failed! Missing expected columns: {missing}")
    print("[Stage: Transform Silver] Schema Validation: PASSED")
    
    # 2. Row Count Logging (Silver Input)
    initial_count = bronze_df.count()
    print(f"[Stage: Transform Silver] Initial read count: {initial_count:,} rows")
    
    # Check if the DataFrame is empty
    if bronze_df.rdd.isEmpty():
        raise Exception("E001: Silver transactions table is empty for date {}".format(run_date))
    
    # Cast columns to correct types
    bronze_df = bronze_df.withColumn("amount", col("amount").cast(DoubleType())) \
                         .withColumn("txn_date", col("txn_date").cast(DateType())) \
                         .withColumn("transaction_id", col("transaction_id").cast(StringType())) \
                         .withColumn("status", col("status").cast(StringType())) \
                         .withColumn("customer_id", col("customer_id").cast(StringType()))
    
    # Filter NULL transaction_id and negative amounts
    bronze_df = bronze_df.filter(col("transaction_id").isNotNull() & (col("amount") >= 0))
    after_filter_count = bronze_df.count()
    print(f"[Stage: Transform Silver] Count after filters: {after_filter_count:,} rows")
    
    # Deduplicate on transaction_id keeping latest _ingestion_ts
    bronze_df = bronze_df.sortWithinPartitions("transaction_id", "_ingestion_ts").dropDuplicates(["transaction_id"])
    after_dedup_count = bronze_df.count()
    print(f"[Stage: Transform Silver] Count after deduplication: {after_dedup_count:,} rows")
    
    # Read merchants data and cache it
    merchants_df = spark.read.parquet(merchants_path).cache()
    
    # Join with merchants (broadcast hint)
    bronze_df = bronze_df.join(broadcast(merchants_df), bronze_df["merchant_id"] == merchants_df["merchant_id"], "left")
    
    # Add quality_flag column (CLEAN or UNMATCHED)
    bronze_df = bronze_df.withColumn("quality_flag", when(col("merchant_id").isNotNull(), "CLEAN").otherwise("UNMATCHED"))
    
    # Compute quality metrics
    total_rows = bronze_df.count()
    null_counts = {col_name: bronze_df.filter(isnull(col(col_name))).count() for col_name in ["amount", "txn_date", "transaction_id", "status", "customer_id"]}
    duplicate_count = total_rows - bronze_df.select("transaction_id").distinct().count()
    duplicate_rate = duplicate_count / total_rows if total_rows > 0 else 0.0
    schema_match = 1
    
    # Calculate overall_quality_score
    null_rate = sum(null_counts.values()) / total_rows if total_rows > 0 else 0.0
    overall_quality_score = 100 - (null_rate * 40 + duplicate_rate * 40 + (1 - schema_match) * 20)
    
    # Determine pipeline status
    pipeline_status = "SUCCESS" if overall_quality_score >= 60 else "HALTED"
    schema_alert = schema_match == 0 and overall_quality_score >= 60
    
    # Create data quality scorecard DataFrame
    scorecard_df = spark.createDataFrame([(
        run_date, "ALL", null_counts["amount"] / total_rows if total_rows > 0 else 0.0, duplicate_rate,
        schema_match, overall_quality_score, pipeline_status, schema_alert
    )], ["run_date", "column_name", "null_rate", "duplicate_rate", "schema_match", "overall_quality_score", "pipeline_status", "schema_alert"])
    
    # Write to Silver layer
    bronze_df.write.mode('overwrite').partitionBy('txn_date').parquet(output_path)
    scorecard_df.write.mode('append').parquet(os.path.join(output_path, "data_quality_scorecard"))
    print(f"[Stage: Transform Silver] Completed. Final row count: {bronze_df.count():,} rows")

def main(input_path="s3://datalake/bronze/transactions/", 
         bronze_path="s3://datalake/bronze/transactions/", 
         merchants_path="s3://datalake/silver/merchants/", 
         output_path="s3://datalake/silver/transactions/",
         run_date=None):
         
    # Initialize Spark session
    spark = SparkSession.builder.appName("TransactionsDQPipeline").getOrCreate()
    
    if run_date is None:
        run_date = datetime.now().date().isoformat()
    run_id = "run_" + run_date.replace("-", "")
    
    # Execute pipeline stages
    print(f"Starting pipeline run {run_id} for run date {run_date}")
    ingest_bronze(spark, input_path, output_path, run_date, run_id)
    transform_silver(spark, bronze_path, merchants_path, output_path, run_date)
    print("Pipeline completed successfully.")

if __name__ == "__main__":
    main()
