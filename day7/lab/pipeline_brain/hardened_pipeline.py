import shutil
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    try:
        logging.info("Starting ingest_bronze stage")
        
        bronze_df = spark.read.csv(input_path, header=True, inferSchema=True)
        
        bronze_df = bronze_df.withColumn("_ingestion_ts", lit(datetime.utcnow()).cast(TimestampType())) \
                             .withColumn("_source_file", lit(input_path)) \
                           .withColumn("_batch_id", lit(run_id))
        
        logging.info(f"[Stage: ingest_bronze] input_count: {bronze_df.count():,} rows")
        
        partition_path = f"{output_path}/txn_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)  # Idempotency: delete partition before write
        
        bronze_df.write.mode('overwrite').partitionBy('txn_date').parquet(output_path)
        
        logging.info("Completed ingest_bronze stage")
        
    except Exception as e:
        logging.error(f"Error in ingest_bronze: {e}")
        raise

def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    try:
        logging.info("Starting transform_silver stage")
        
        bronze_df = spark.read.parquet(bronze_path).where(col("txn_date") == run_date)  # Partition pruning
        
        if bronze_df.rdd.isEmpty():
            raise Exception("E001: Silver transactions table is empty for date {}".format(run_date))
        
        logging.info(f"[Stage: transform_silver] input_count: {bronze_df.count():,} rows")
        
        bronze_df = bronze_df.withColumn("amount", col("amount").cast(DoubleType())) \
                             .withColumn("txn_date", col("txn_date").cast(DateType())) \
                            .withColumn("transaction_id", col("transaction_id").cast(StringType())) \
                           .withColumn("status", col("status").cast(StringType())) \
                           .withColumn("customer_id", col("customer_id").cast(StringType()))
        
        bronze_df = bronze_df.filter(col("transaction_id").isNotNull() & (col("amount") >= 0))
        
        logging.info(f"[Stage: transform_silver] after_filter_count: {bronze_df.count():,} rows")
        
        bronze_df = bronze_df.sortWithinPartitions("transaction_id", "_ingestion_ts").dropDuplicates(["transaction_id"])
        
        logging.info(f"[Stage: transform_silver] after_dedup_count: {bronze_df.count():,} rows")
        
        merchants_df = spark.read.parquet(merchants_path).cache()
        
        bronze_df = bronze_df.join(broadcast(merchants_df), bronze_df["merchant_id"] == merchants_df["merchant_id"], "left")
        
        bronze_df = bronze_df.withColumn("quality_flag", when(col("merchant_id").isNotNull(), "CLEAN").otherwise("UNMATCHED"))
        
        total_rows = bronze_df.count()
        null_counts = {col: bronze_df.filter(isnull(col)).count() for col in ["amount", "txn_date", "transaction_id", "status", "customer_id"]}
        duplicate_count = total_rows - bronze_df.select("transaction_id").distinct().count()
        duplicate_rate = duplicate_count / total_rows
        schema_match = 1 if set(["transaction_id", "customer_id", "amount", "txn_date", "status"]).issubset(bronze_df.columns) else 0
        
        null_rate = sum(null_counts.values()) / total_rows
        overall_quality_score = 100 - (null_rate * 40 + duplicate_rate * 40 + (1 - schema_match) * 20)
        
        pipeline_status = "SUCCESS" if overall_quality_score >= 60 else "HALTED"
        schema_alert = schema_match == 0 and overall_quality_score >= 60
        
        scorecard_df = spark.createDataFrame([(
            run_date, "ALL", null_counts["amount"] / total_rows, duplicate_rate,
            schema_match, overall_quality_score, pipeline_status, schema_alert
        )], ["run_date", "column_name", "null_rate", "duplicate_rate", "schema_match", "overall_quality_score", "pipeline_status", "schema_alert"])
        
        partition_path = f"{output_path}/txn_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)  # Idempotency: delete partition before write
        
        bronze_df.write.mode('overwrite').partitionBy('txn_date').parquet(output_path)
        scorecard_df.write.mode('append').parquet(output_path + "/data_quality_scorecard")
        
        logging.info(f"[Stage: transform_silver] output_count: {bronze_df.count():,} rows")
        logging.info("Completed transform_silver stage")
        
    except Exception as e:
        logging.error(f"Error in transform_silver: {e}")
        raise

def run_gold(spark, silver_path, gold_output_dir, run_date):
    try:
        logging.info("Starting run_gold stage")
        
        build_merchant_performance(spark, silver_path, f"{gold_output_dir}/merchant_performance", run_date)
        build_customer_ltv(spark, silver_path, f"{gold_output_dir}/customer_ltv")
        build_daily_summary(spark, silver_path, f"{gold_output_dir}/daily_summary", run_date)
        
        run_metadata = {
            "run_date": run_date,
            "status": "SUCCESS",
            "error_message": ""
        }
        
        spark.sparkContext.parallelize([run_metadata]).write.json(f"{gold_output_dir}/run_metadata")
        
        logging.info("Completed run_gold stage")
        
    except Exception as e:
        logging.error(f"Error in run_gold: {e}")
        raise

def build_merchant_performance(spark, silver_path, output_path, run_date):
    try:
        logging.info("Starting build_merchant_performance stage")
        
        silver_df = (spark.read.format("delta").load(silver_path)
                    .where(col("txn_date") == run_date)  # Partition pruning
                    .cache())
        
        if silver_df.rdd.isEmpty():
            raise Exception("E001: silver.transactions is missing or empty")
        
        logging.info(f"[Stage: build_merchant_performance] input_count: {silver_df.count():,} rows")
        
        merchant_performance_df = (silver_df.groupBy("merchant_id", "merchant_name", "category", "city", "date")
                                  .agg(
                                      total_revenue=_sum(when(col("status") == "COMPLETED", col("amount")).otherwise(0)).alias("total_revenue"),
                                      txn_count=count("*"),
                                      failed_txns=count(when(col("status") == "FAILED", 1))
                                  )
                                .withColumn("failure_rate_pct", (col("failed_txns") / col("txn_count")) * 100)
                                .drop("failed_txns"))
        
        partition_path = f"{output_path}/date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)  # Idempotency: delete partition before write
        
        merchant_performance_df.write.partitionBy("date").mode("overwrite").parquet(output_path)
        
        logging.info(f"[Stage: build_merchant_performance] output_count: {merchant_performance_df.count():,} rows")
        logging.info("Completed build_merchant_performance stage")
        
    except Exception as e:
        logging.error(f"Error in build_merchant_performance: {e}")
        raise

def build_customer_ltv(spark, silver_path, output_path):
    try:
        logging.info("Starting build_customer_ltv stage")
        
        silver_df = (spark.read.format("delta").load(silver_path)
                    .where(col("status") == "COMPLETED")
                   .cache())
        
        if silver_df.rdd.isEmpty():
            raise Exception("E002: silver.transactions is missing or empty")
        
        logging.info(f"[Stage: build_customer_ltv] input_count: {silver_df.count():,} rows")
        
        customer_ltv_df = (silver_df.groupBy("customer_id")
                            .agg(
                                total_spent=_sum("amount"),
                                total_txns=count("*"),
                                avg_txn_value=_sum("amount") / count("*"),
                                first_txn_date=min("txn_date"),
                                last_txn_date=max("txn_date")
                            ))
        
        payment_method_df = (silver_df.groupBy("customer_id", "payment_method")
                            .agg(count("*").alias("payment_count"))
                             .groupBy("customer_id")
                             .agg(max("payment_count").alias("max_payment_count")))
        
        mode_df = (payment_method_df.join(silver_df, on=["customer_id"], how="inner")
                   .where(col("payment_count") == col("max_payment_count"))
                  .drop("max_payment_count", "payment_count"))
        
        customer_ltv_df = customer_ltv_df.join(mode_df, on="customer_id", how="left")
        customer_ltv_df = customer_ltv_df.withColumn("preferred_payment_method", col("payment_method"))
        customer_ltv_df = customer_ltv_df.drop("payment_method")
        
        customer_ltv_df.write.mode("overwrite").parquet(output_path)
        
        logging.info(f"[Stage: build_customer_ltv] output_count: {customer_ltv_df.count():,} rows")
        logging.info("Completed build_customer_ltv stage")
        
    except Exception as e:
        logging.error(f"Error in build_customer_ltv: {e}")
        raise

def build_daily_summary(spark, silver_path, output_path, run_date):
    try:
        logging.info("Starting build_daily_summary stage")
        
        silver_df = (spark.read.format("delta").load(silver_path)
                     .where(col("txn_date") == run_date)  # Partition pruning
                    .cache())
        
        if silver_df.rdd.isEmpty():
            raise Exception("E003: silver.transactions is missing or empty")
        
        logging.info(f"[Stage: build_daily_summary] input_count: {silver_df.count():,} rows")
        
        daily_summary_df = (silver_df.groupBy("date")
                        .agg(
                            total_revenue=_sum(when(col("status") == "COMPLETED", col("amount")).otherwise(0)),
                            total_txns=count("*"),
                            unique_customers=countDistinct("customer_id"),
                            unique_merchants=countDistinct("merchant_id"),
                            failed_txns=count(when(col("status") == "FAILED", 1))
                        )
                       .withColumn("failure_rate_pct", (col("failed_txns") / col("total_txns")) * 100)
                      .drop("failed_txns"))
        
        partition_path = f"{output_path}/date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)  # Idempotency: delete partition before write
        
        daily_summary_df.write.partitionBy("date").mode("overwrite").parquet(output_path)
        
        logging.info(f"[Stage: build_daily_summary] output_count: {daily_summary_df.count():,} rows")
        logging.info("Completed build_daily_summary stage")
        
    except Exception as e:
        logging.error(f"Error in build_daily_summary: {e}")
        raise

def main():
    spark = SparkSession.builder.appName("TransactionsDQPipeline").getOrCreate()
    
    input_path = "s3://datalake/bronze/transactions/"
    bronze_path = "s3://datalake/bronze/transactions/"
    merchants_path = "s3://datalake/silver/merchants/"
    output_path = "s3://datalake/silver/transactions/"
    gold_output_dir = "s3://datalake/gold/transactions/"
    run_date = datetime.now().date().isoformat()
    run_id = "run_" + run_date.replace("-", "")
    
    started_at = datetime.utcnow().isoformat()
    
    try:
        ingest_bronze(spark, input_path, output_path, run_date, run_id)
        transform_silver(spark, bronze_path, merchants_path, output_path, run_date)
        run_gold(spark, output_path, gold_output_dir, run_date)
        
        completed_at = datetime.utcnow().isoformat()
        run_status = "SUCCESS"
        error_message = ""
        
    except Exception as e:
        completed_at = datetime.utcnow().isoformat()
        run_status = "FAILED"
        error_message = str(e)
        
    run_metadata = {
        "pipeline_name": "TransactionsDQPipeline",
        "run_date": run_date,
        "run_id": run_id,
        "run_status": run_status,
        "error_message": error_message,
        "started_at": started_at,
        "completed_at": completed_at
    }
    
    spark.sparkContext.parallelize([run_metadata]).write.json(f"{gold_output_dir}/run_metadata_{run_date}.json")

if __name__ == "__main__":
    main()