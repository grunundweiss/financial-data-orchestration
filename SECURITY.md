# Security

This document opens with what was wrong with this repository, because that is
more useful than a list of tools.

Each finding below was real, found by auditing the code as it stood, and each
now has a test that fails if it comes back. The control register is
[security/controls.yaml](security/controls.yaml); the reasoning behind the
boundaries is [docs/threat-model.md](docs/threat-model.md).

**The data is synthetic.** Nothing here was ever at risk. What was missing was
any mechanism that would have behaved differently if it weren't — that absence
was the finding, and it is what the work below addresses.

## What was wrong, and what catches it now

| # | What was wrong | The test that now catches it |
|---|---|---|
| F-01 | The gold model built its incremental filter by string-interpolating a Jinja var: `WHERE batch_date = '{{ batch_date_var }}'`. Safe only by provenance — the moment `batch_date` came from `dag_run.conf`, a REST trigger, or an operator param, it was attacker-controlled SQL running with full warehouse privileges. | `tests/test_security_input.py` submits crafted values and asserts the run is rejected *and* gold is unchanged |
| F-02 | `docker-compose.yml` set `POSTGRES_PASSWORD: airflow` and repeated the pair inside the connection string. Both on the default branch. | `tests/test_security_secrets.py::test_compose_does_not_hardcode_database_credentials` |
| F-03 | Secrets resolved to published defaults: `${AIRFLOW__API_AUTH__JWT_SECRET:-airflow_jwt_secret}`, `${GF_SECURITY_ADMIN_PASSWORD:-admin}`. A missing `.env` didn't stop the stack — it booted with a signing key committed to this repo. Silent, which is what made it worse than no control. | `tests/test_security_secrets.py::test_compose_has_no_fail_open_defaults_for_secrets` |
| F-04 | The README advertised a CI badge and two jobs. There was no `.github/` directory. Every other claim in the README inherited that doubt. | `tests/test_security_ci.py` asserts the workflow exists and each gate is wired into it |
| F-05 | Ingestion is `DELETE`-then-`INSERT` and the gold model is `delete+insert` — correct idempotency, but any re-run silently replaced a day with no record that it happened, what the numbers were, or who asked. The quality gate ran and its evidence evaporated. | `tests/test_security_ledger.py` mutates a historical gold row and asserts `verify-chain` names it |
| F-06 | `account_id` was generated in Bronze, carried through Silver, and grouped on in Gold in clear text, into an unencrypted DuckDB file on a host bind mount. | `tests/test_security_classification.py` scans every text column in bronze/silver/gold for raw `ACC-NOR-` patterns |
| F-07 | Prometheus (9090) and statsd-exporter (9102) published on all interfaces with no authentication. Airflow's metrics stream leaks pipeline structure and volumes to anyone who can scrape it. | `tests/test_security_supply_chain.py::test_internal_services_are_not_published_to_the_lan` |
| F-08 | Three images floated on `latest`, dbt packages used version *ranges*, and `dbt deps` ran as a task inside the DAG — so a 03:00 run fetched from the dbt hub and could execute macro SQL from a version that never existed when the code was written. | `tests/test_security_supply_chain.py` — floating tags, ranges, and a runtime `deps` task all fail |
| F-09 | No `cap_drop`, no `no-new-privileges`, no read-only mounts anywhere. | `tests/test_security_supply_chain.py::test_observability_services_drop_capabilities` |
| F-10 | A root `requirements.txt` pinned different versions than `pyproject.toml` and `airflow/requirements.txt`, and nothing installed from it. `env.example` and `.env.example` were byte-identical duplicates. | Removed; `tests/test_security_supply_chain.py::test_python_dependencies_are_exact_pinned` |

## The demo

Three days of data, one row quietly edited afterwards, and the verifier naming
it. Ten seconds, and it shows a control doing something:

```bash
python security/verify_chain.py --db data/analytics_platform.db
```

```
verify-chain: OK - 9 ledger entries verified, gold matches every recorded snapshot
```

Then edit one historical row directly in DuckDB and run it again:

```
verify-chain: FAILED
  the ledger chain is intact across 9 entries, but the warehouse no longer matches what it recorded:
  - gold rows for 2026-01-16 no longer match the hash recorded when the run completed
    (recorded d8dcf3f49b1c..., now 29135f52421f...)
```

The two halves are separate on purpose. The hash chain proves the *ledger*
wasn't edited. The snapshot comparison proves the *warehouse* still holds what
the ledger committed to — the case where someone edits a gold row and leaves
the ledger alone.

## Controls

Nine, each named in [security/controls.yaml](security/controls.yaml) with the
test that proves it. `security/verify_controls.py` runs in CI and fails if any
control names a test that doesn't exist — a control with no test is a claim.

Two are marked `partial`, with notes saying what's missing. That is deliberate:
see [the threat model's "what is explicitly not defended"](docs/threat-model.md#what-is-explicitly-not-defended),
which also covers the deterministic-tokenization trade-off, the absence of
encryption at rest, and why containers still run in group 0.

## Running the checks

```bash
python security/preflight.py        # refuses placeholder or weak secrets
python security/verify_controls.py  # every control maps to a real test
python security/verify_chain.py     # ledger + warehouse integrity
pytest tests/                       # the controls themselves
```

## Reporting

This is a personal portfolio project with synthetic data. If you find something
wrong with it, open an issue — there is no embargo process and nothing
confidential to protect.
