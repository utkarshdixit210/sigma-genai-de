WITH  __dbt__cte__stg_transactions as (
WITH cleaned_transactions AS (
    SELECT
        LOWER(transaction_id) AS transaction_id,
        CAST(amount AS DECIMAL(10, 2)) AS amount,
        LOWER(status) AS status,
        LOWER(merchant_id) AS merchant_id,
        LOWER(customer_id) AS customer_id,
        CAST(transaction_date AS DATE) AS transaction_date,
        LOWER(payment_method) AS payment_method,
        CURRENT_TIMESTAMP AS loaded_at
    FROM 
        SIGMA_DE.PUBLIC.fact_transactions
    WHERE 
        merchant_id NOT LIKE 'TEST_%'
)

SELECT * FROM cleaned_transactions
), filtered_transactions AS (
    SELECT
        transaction_id,
        amount,
        status,
        merchant_id,
        customer_id,
        transaction_date,
        payment_method
    FROM __dbt__cte__stg_transactions
    WHERE status IN ('completed', 'failed')
),

merchant_details AS (
    SELECT
        merchant_id,
        merchant_name,
        category,
        city
    FROM SIGMA_DE.PUBLIC.dim_merchant
),

aggregated_metrics AS (
    SELECT
        ft.merchant_id,
        COUNT(ft.transaction_id) AS total_transactions,
        COUNT(DISTINCT ft.customer_id) AS unique_customers,
        SUM(CASE WHEN ft.status = 'completed' THEN ft.amount ELSE 0 END) AS total_revenue,
        COUNT(CASE WHEN ft.status = 'failed' THEN 1 END) AS failed_count,
        AVG(CASE WHEN ft.status = 'completed' THEN ft.amount ELSE NULL END) AS avg_transaction_value
    FROM filtered_transactions ft
    GROUP BY ft.merchant_id
),

final_kpis AS (
    SELECT
        md.merchant_id,
        md.merchant_name,
        md.category,
        md.city,
        am.total_transactions,
        am.unique_customers,
        am.total_revenue,
        am.failed_count,
        (am.failed_count::FLOAT / am.total_transactions) * 100 AS failure_rate_pct,
        am.avg_transaction_value
    FROM aggregated_metrics am
    JOIN merchant_details md ON am.merchant_id = md.merchant_id
)

SELECT * FROM final_kpis