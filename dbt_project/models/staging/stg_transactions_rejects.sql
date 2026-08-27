-- dbt_project/models/staging/stg_transactions_rejects.sql
-- Quarantines the rows stg_transactions drops (no transaction_id) so the loss
-- is counted instead of silent. See reject_rate_below_threshold.sql.
SELECT
    *,
    'missing transaction_id' AS reject_reason
FROM {{ source('bronze', 'raw_transactions') }}
WHERE transaction_id IS NULL
