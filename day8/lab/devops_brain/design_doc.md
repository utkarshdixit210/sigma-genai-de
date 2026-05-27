# Pipeline Design Document

## What This Pipeline Does

This pipeline ingests transaction data from both clean and dirty sources, processes it through bronze, silver, and gold layers, and finally produces merchant performance and daily summary reports.

## Data Flow Diagram

```plaintext
+--------------------+     +--------------------+     +--------------------+     +--------------------+
|  Source            |     |  Bronze Layer      |     |  Silver Layer      |     |  Gold Layer        |
|  (TRANSACTIONS_CLEAN, | --> |  bronze_transactions | --> |  silver_transactions | --> |  gold_merchant_performance |
|  TRANSACTIONS_DIRTY) |     |  (load_bronze)     |     |  (transform_bronze_to_silver) |  |  (compute_merchant_performance) |
+--------------------+     +--------------------+     +--------------------+     +--------------------+
                                                                                           |
                                                                                           |
                                                                                           |
                                                                                           |
+--------------------+     +--------------------+     +--------------------+     +--------------------+
|  Source            |     |  Bronze Layer      |     |  Silver Layer      |     |  Gold Layer        |
|  (MERCHANTS)       | --> |  bronze_transactions | --> |  silver_transactions | --> |  gold_daily_summary |
|                    |     |  (load_merchants)  |     |  (transform_bronze_to_silver) |  |  (compute_daily_summary) |
+--------------------+     +--------------------+     +--------------------+     +--------------------+
```

## Key Design Decisions

- **Layered Approach**: The pipeline is designed in a three-layer architecture (bronze, silver, gold) to ensure data is progressively cleaned and enriched.
- **Quality Flags**: Introduced quality flags in the silver layer to differentiate between clean and potentially problematic data.
- **Aggregations**: Aggregations are performed in the gold layer to produce summary metrics, ensuring the pipeline remains performant.
- **Date Handling**: The gold layer handles date-specific aggregations, ensuring reports are generated for the current date.

## Known Limitations

- **Single-threaded**: The pipeline runs sequentially, which may not be optimal for very large datasets.
- **No Error Handling**: The pipeline lacks comprehensive error handling, which could lead to data loss in case of failures.
- **Static Merchant Data**: Merchant data is loaded once and not updated dynamically, which could lead to stale information.
- **Limited Data Validation**: The pipeline performs minimal validation on incoming data, which could result in incorrect aggregations.

## Dependencies

- **DuckDB**: The pipeline relies on DuckDB for database operations.
- **MERCHANTS**: A list of merchant data used to enrich transaction data.
- **TRANSACTIONS_CLEAN and TRANSACTIONS_DIRTY**: Source data files containing transaction records.