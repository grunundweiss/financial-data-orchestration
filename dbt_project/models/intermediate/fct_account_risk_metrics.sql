-- dbt_project/models/intermediate/fct_account_risk_metrics.sql
-- Grain: one row per account per batch_date (a daily fact, not a lifetime
-- rollup). That's what makes this safely incremental - each run only
-- aggregates and upserts the day it was given, so re-running a date or
-- backfilling out of order never has to touch, or double-count, other days.
{{ config(
    materialized='incremental',
    unique_key=['account_id', 'batch_date'],
    incremental_strategy='delete+insert'
) }}

WITH cleansed_transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
    {% if is_incremental() %}
    {% set batch_date_var = var('batch_date', none) %}
    {% if batch_date_var %}
    -- Scoped to the day this run is for. Falls back to reprocessing every
    -- day (still correct, just not incremental) if the caller omits the var.
    WHERE batch_date = '{{ batch_date_var }}'
    {% endif %}
    {% endif %}
)

SELECT
    account_id,
    batch_date,
    COUNT(transaction_id) AS total_transactions,
    ROUND(SUM(amount), 2) AS net_balance,
    COUNT(CASE WHEN transaction_risk_profile = 'HIGH_VALUE' THEN 1 END) AS high_value_count,
    ROUND(AVG(amount), 2) AS average_transaction_size
FROM cleansed_transactions
GROUP BY account_id, batch_date
