# tests/conftest.py
import os
from pathlib import Path

import pytest

DBT_PROJECT_DIR = str(Path(__file__).resolve().parent.parent / "dbt_project")

# Tokenization fails closed without a pepper, which is the point of the control
# (see tokenization.get_pepper). Tests need a deterministic one, set before any
# module imports pipeline_tasks. Never reuse this value outside tests.
os.environ.setdefault("ACCOUNT_TOKEN_PEPPER", "pytest-fixed-pepper-do-not-use-in-production")

from pipeline_tasks import run_dbt


@pytest.fixture(scope="session", autouse=True)
def dbt_packages():
    """Fetches dbt_utils/dbt_expectations once per test session, for every test module."""
    run_dbt("deps", DBT_PROJECT_DIR, DBT_PROJECT_DIR)
