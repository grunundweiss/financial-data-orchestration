#!/usr/bin/env python3
"""Verifies the pipeline's audit ledger and the gold rows it committed to.

    python security/verify_chain.py                    # uses $DUCKDB_PATH
    python security/verify_chain.py --db data/analytics_platform.db

Exit codes:
    0  chain intact and gold matches every recorded snapshot
    1  a chain break or a mismatched gold snapshot (details on stderr)
    2  no ledger found
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# audit_ledger lives beside the DAG. That is airflow/dags/ in the repo and
# /opt/airflow/dags/ inside the container, so look in both rather than
# assuming a layout.
for candidate in (REPO_ROOT / "airflow" / "dags", REPO_ROOT / "dags"):
    if (candidate / "audit_ledger.py").exists():
        sys.path.insert(0, str(candidate))
        break

from audit_ledger import ChainBreak, verify_chain, verify_gold_against_ledger

DEFAULT_DB = os.environ.get("DUCKDB_PATH", str(REPO_ROOT / "data" / "analytics_platform.db"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default=DEFAULT_DB, help="path to the DuckDB warehouse")
    args = parser.parse_args()

    if not Path(args.db).exists():
        print(f"verify-chain: no warehouse at {args.db}", file=sys.stderr)
        return 2

    try:
        verified = verify_chain(args.db)
    except ChainBreak as exc:
        print("verify-chain: FAILED", file=sys.stderr)
        print(f"  {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 - surfaced verbatim to the operator
        print(f"verify-chain: could not read the ledger: {exc}", file=sys.stderr)
        return 2

    if verified == 0:
        print(f"verify-chain: ledger at {args.db} is empty - nothing to verify")
        return 0

    mismatches = verify_gold_against_ledger(args.db)
    if mismatches:
        print("verify-chain: FAILED", file=sys.stderr)
        print(f"  the ledger chain is intact across {verified} entries, but the warehouse "
              f"no longer matches what it recorded:", file=sys.stderr)
        for mismatch in mismatches:
            print(f"  - {mismatch}", file=sys.stderr)
        return 1

    print(f"verify-chain: OK - {verified} ledger entries verified, "
          f"gold matches every recorded snapshot")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
