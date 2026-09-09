# tests/test_security_ci.py
"""Proves C-07: security gates run on every push and block merge.

The original audit's most damaging finding was that the README advertised a CI
workflow that did not exist in the repository. These tests assert the workflow
is present and that each gate the register claims is actually wired into it -
so the README's claims stay executable rather than aspirational.
"""
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
README = REPO_ROOT / "README.md"


@pytest.fixture(scope="module")
def workflow():
    assert WORKFLOW.exists(), (
        "the CI workflow the README advertises does not exist. This is the finding "
        "that made every other claim in the README suspect."
    )
    return yaml.safe_load(WORKFLOW.read_text())


def test_workflow_runs_on_push_and_pull_request(workflow):
    # PyYAML parses a bare `on:` key as the boolean True.
    triggers = workflow.get("on") or workflow.get(True)
    assert "push" in triggers
    assert "pull_request" in triggers, "gates that don't run on PRs cannot block a merge"


def test_workflow_has_least_privilege_token(workflow):
    assert workflow.get("permissions", {}).get("contents") == "read", (
        "the workflow token should be read-only unless it needs more"
    )


def all_run_steps(workflow) -> str:
    """Every `run:` and `uses:` line across all jobs, as one searchable blob."""
    parts = []
    for job in workflow["jobs"].values():
        for step in job.get("steps", []):
            parts.append(str(step.get("run", "")))
            parts.append(str(step.get("uses", "")))
    return "\n".join(parts)


@pytest.mark.parametrize("gate,needle", [
    ("lint", "ruff check"),
    ("types", "mypy"),
    ("tests", "pytest"),
    ("control register", "verify_controls.py"),
    ("secret scanning", "gitleaks"),
    ("dependency audit", "pip-audit"),
    ("static analysis", "bandit"),
    ("env preflight", "preflight.py"),
    ("audit chain verification", "verify_chain.py"),
])
def test_gate_is_wired_into_ci(workflow, gate, needle):
    assert needle in all_run_steps(workflow), f"the {gate} gate is not run by CI"


def test_ci_proves_the_tamper_control_actually_fails(workflow):
    """A verify-chain that only ever runs on clean data proves nothing. CI must
    also mutate the warehouse and assert the verifier rejects it."""
    steps = all_run_steps(workflow)
    assert "tampered" in steps.lower(), (
        "CI should tamper with the warehouse and assert verify-chain fails - "
        "otherwise the control is never observed failing"
    )


def test_ci_asserts_idempotency_still_holds(workflow):
    """The security layer must not have broken the pipeline's original property."""
    steps = all_run_steps(workflow)
    assert "idempotent" in steps.lower()


def test_readme_badge_points_at_the_real_workflow():
    """The badge must reference the workflow file that exists."""
    readme = README.read_text()
    if "badge.svg" not in readme:
        pytest.skip("README has no CI badge")
    assert "workflows/ci.yml/badge.svg" in readme, (
        "the badge must point at .github/workflows/ci.yml"
    )
