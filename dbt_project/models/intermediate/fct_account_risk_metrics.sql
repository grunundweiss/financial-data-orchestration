-- dbt_project/models/intermediate/fct_account_risk_metrics.sql
WITH cleansed_transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
)

SELECT
    account_id,
    COUNT(transaction_id) AS total_transactions,
    ROUND(SUM(amount), 2) AS net_balance,
    COUNT(CASE WHEN transaction_risk_profile = 'HIGH_VALUE' THEN 1 END) AS high_value_count,
    ROUND(AVG(amount), 2) AS average_transaction_size
FROM cleansed_transactions
GROUP BY account_id
