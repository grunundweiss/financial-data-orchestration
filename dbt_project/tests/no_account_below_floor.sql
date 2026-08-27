-- dbt_project/tests/no_account_below_floor.sql
-- Business rule: no account's daily net position may fall below the
-- regulatory floor. Singular test: fails (loudly, in the quality gate) if it
-- returns any rows.
SELECT account_id, batch_date, net_balance
FROM {{ ref('fct_account_risk_metrics') }}
WHERE net_balance < -100000
