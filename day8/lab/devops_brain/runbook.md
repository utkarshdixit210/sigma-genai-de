# Pipeline Overview

This pipeline ingests transaction data, transforms it, and loads it into bronze, silver, and gold tables. It also computes merchant performance and daily summaries. If this pipeline stops, downstream analytics and reporting will be impacted.

## Pipeline Steps

1. Connect to DuckDB using `get_connection`.
2. Setup tables using `setup_tables`.
3. Load merchants using `load_merchants`.
4. Load transactions into bronze table using `load_bronze`.
5. Transform bronze to silver using `transform_bronze_to_silver`.
6. Load silver transactions using `load_silver`.
7. Compute merchant performance using `compute_merchant_performance`.
8. Compute daily summary using `compute_daily_summary`.
9. Load gold tables using `load_gold`.

## Schedule / Trigger

This pipeline runs every hour, triggered by a cron job.

## Failure Modes

1. **DuckDB Connection Failure**
   - **Root Cause:** Database is down or inaccessible.
   - **Symptom:** `get_connection` throws an error.
2. **Table Creation Failure**
   - **Root Cause:** Syntax error in SQL.
   - **Symptom:** `setup_tables` throws an error.
3. **Merchant Data Load Failure**
   - **Root Cause:** Corrupt merchant data.
   - **Symptom:** `load_merchants` throws an error.
4. **Bronze Table Load Failure**
   - **Root Cause:** Corrupt transaction data.
   - **Symptom:** `load_bronze` throws an error.
5. **Silver Table Transformation Failure**
   - **Root Cause:** Inconsistent data in bronze table.
   - **Symptom:** `transform_bronze_to_silver` throws an error.

## Recovery Actions

1. **DuckDB Connection Failure**
   - Check DuckDB service status.
   - Restart DuckDB if necessary.
   - Retry pipeline.
2. **Table Creation Failure**
   - Review SQL syntax in `setup_tables`.
   - Correct the syntax.
   - Retry pipeline.
3. **Merchant Data Load Failure**
   - Validate merchant data in `MERCHANTS`.
   - Correct any corrupt data.
   - Retry pipeline.
4. **Bronze Table Load Failure**
   - Validate transaction data in `TRANSACTIONS_CLEAN` and `TRANSACTIONS_DIRTY`.
   - Correct any corrupt data.
   - Retry pipeline.
5. **Silver Table Transformation Failure**
   - Investigate inconsistent data in bronze table.
   - Correct the data.
   - Retry pipeline.

## Known Bugs

- Hardcoded AWS credentials in the code.
- Lack of null handling in `transform_bronze_to_silver`.

## Escalation Contacts

1. **On-call DE:** Priya Nair (priya.nair@sigmadatatech.in, +91-98400-11111)
2. **Tech Lead:** Arjun Mehta (arjun.mehta@sigmadatatech.in)
3. **Platform Manager:** Kavya Reddy (kavya.reddy@sigmadatatech.in)

## Data Quality Checks

- Verify the number of records in `bronze_transactions`, `silver_transactions`, `gold_merchant_performance`, and `gold_daily_summary`.
- Ensure `quality_flag` is set correctly in `silver_transactions`.
- Check for any NULL values in critical fields.