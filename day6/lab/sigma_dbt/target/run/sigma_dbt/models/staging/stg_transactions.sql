
  create or replace   view SIGMA_DE.PUBLIC.stg_transactions
  
  
  
  
  as (
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
  );

