-- dbt_project/tests/reject_rate_below_threshold.sql
-- Business rule: rows quarantined into stg_transactions_rejects (no
-- transaction_id) must stay under 5% of a batch's volume - past that, it's a
-- pipeline problem, not noise, and the quality gate should say so.
WITH accepted AS (
    SELECT batch_date, COUNT(*) AS accepted_count
    FROM {{ ref('stg_transactions') }}
    GROUP BY batch_date
),

rejected AS (
    SELECT batch_date, COUNT(*) AS reject_count
    FROM {{ ref('stg_transactions_rejects') }}
    GROUP BY batch_date
)

SELECT
    COALESCE(a.batch_date, r.batch_date) AS batch_date,
    COALESCE(a.accepted_count, 0) AS accepted_count,
    COALESCE(r.reject_count, 0) AS reject_count
FROM accepted a
FULL OUTER JOIN rejected r ON a.batch_date = r.batch_date
WHERE COALESCE(r.reject_count, 0) > 0.05 * (COALESCE(a.accepted_count, 0) + COALESCE(r.reject_count, 0))
