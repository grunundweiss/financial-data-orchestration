#!/usr/bin/env python3
"""Fails if any control in the register lacks the test it claims to have.

The register is only worth having if it can't quietly drift into a list of
aspirations. This parses security/controls.yaml and checks that every control
names a test file that exists and actually contains tests.

    python security/verify_controls.py

Exit codes: 0 all controls are backed by a test, 1 otherwise.
"""
from __future__ import annotations

import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
REGISTER = REPO_ROOT / "security" / "controls.yaml"

REQUIRED_FIELDS = ("id", "control", "owner", "proves", "test", "mapping", "status")
VALID_STATUSES = {"implemented", "partial", "planned"}


def main() -> int:
    register = yaml.safe_load(REGISTER.read_text())
    controls = register.get("controls", [])

    if not controls:
        print("verify-controls: register is empty", file=sys.stderr)
        return 1

    problems: list[str] = []
    seen_ids: set[str] = set()

    for control in controls:
        cid = control.get("id", "<no id>")

        for field in REQUIRED_FIELDS:
            if not control.get(field):
                problems.append(f"{cid}: missing required field '{field}'")

        if cid in seen_ids:
            problems.append(f"{cid}: duplicate control id")
        seen_ids.add(cid)

        status = control.get("status")
        if status and status not in VALID_STATUSES:
            problems.append(f"{cid}: status {status!r} is not one of {sorted(VALID_STATUSES)}")

        if status == "partial" and not control.get("notes"):
            problems.append(f"{cid}: status is 'partial' but no notes explain what is missing")

        test_path = control.get("test")
        if not test_path:
            continue

        resolved = REPO_ROOT / test_path
        if not resolved.exists():
            problems.append(f"{cid}: test file {test_path} does not exist")
        elif "def test_" not in resolved.read_text():
            problems.append(f"{cid}: test file {test_path} contains no tests")

    if problems:
        print(f"verify-controls: {len(problems)} problem(s) in {REGISTER.name}:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1

    implemented = sum(1 for c in controls if c.get("status") == "implemented")
    print(f"verify-controls: {len(controls)} controls, all backed by a test "
          f"({implemented} implemented, {len(controls) - implemented} partial/planned)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
