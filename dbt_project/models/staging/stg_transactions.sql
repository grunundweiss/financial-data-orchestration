-- dbt_project/models/staging/stg_transactions.sql
WITH raw_data AS (
    SELECT * FROM {{ source('bronze', 'raw_transactions') }}
)

SELECT
    transaction_id,
    account_id,
    amount,
    currency,
    timestamp,
    merchant_category,
    batch_date,
    CASE
        WHEN amount > 10000.0 THEN 'HIGH_VALUE'
        WHEN amount < 0.0 THEN 'OUTFLOW'
        ELSE 'STANDARD'
    END AS transaction_risk_profile
FROM raw_data
-- Rows with no transaction_id can't be deduplicated or tracked downstream, so
-- they're excluded here - but not silently: stg_transactions_rejects.sql
-- captures exactly the same rows, and a singular test asserts the reject
-- count stays within an expected bound instead of the drop going unnoticed.
WHERE transaction_id IS NOT NULL
