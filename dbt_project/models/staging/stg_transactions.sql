-- dbt_project/models/staging/stg_transactions.sql
WITH raw_data AS (
    SELECT * FROM {{ source('bronze', 'stg_transactions') }}
)

SELECT
    transaction_id,
    account_id,
    amount,
    currency,
    timestamp,
    merchant_category,
    CASE
        WHEN amount > 10000.0 THEN 'HIGH_VALUE'
        WHEN amount < 0.0 THEN 'OUTFLOW'
        ELSE 'STANDARD'
    END AS transaction_risk_profile
FROM raw_data
WHERE transaction_id IS NOT NULL
