# tests/test_security_secrets.py
"""Proves C-03: no secret ships in the repository or resolves to a default.

Two halves: the preflight rejects placeholder/weak/missing values, and the
compose file itself contains no `:-` fallback for anything secret.
"""
import re
from pathlib import Path

import pytest
import yaml

from security.preflight import check, parse_env_file

REPO_ROOT = Path(__file__).resolve().parent.parent
COMPOSE = REPO_ROOT / "docker-compose.yml"

GOOD_ENV = {
    "POSTGRES_USER": "airflow",
    "POSTGRES_DB": "airflow",
    "POSTGRES_PASSWORD": "S3rHhVJ2mkQpLd8xTn4w",
    "AIRFLOW__CORE__FERNET_KEY": "vZs3bvfhC_kNGe0WaigN0ZgCNtG9vQe_WCsbfVNJtZQ=",
    "AIRFLOW__API_AUTH__JWT_SECRET": "7Kd0pRq2XmVzYb5NwLtA9cFhJ3sEuG1i",
    "_AIRFLOW_WWW_USER_PASSWORD": "correct-horse-battery-staple",
    "GF_SECURITY_ADMIN_PASSWORD": "Zt6bQw9LmXr4PkNv2Hs8",
}


def test_a_fully_populated_env_passes():
    assert check(GOOD_ENV) == []


@pytest.mark.parametrize("key", sorted(GOOD_ENV))
def test_every_required_secret_is_enforced(key):
    """Dropping any one required secret must be caught."""
    env = {k: v for k, v in GOOD_ENV.items() if k != key}
    problems = check(env)
    assert any(key in p for p in problems), f"{key} missing was not reported"


@pytest.mark.parametrize("bad", ["change_me", "CHANGE_ME_TOO", "your_password_here",
                                 "replace_me", "placeholder", "xxxxxx"])
def test_placeholder_values_are_rejected(bad):
    env = {**GOOD_ENV, "GF_SECURITY_ADMIN_PASSWORD": bad}
    assert any("GF_SECURITY_ADMIN_PASSWORD" in p for p in check(env))


@pytest.mark.parametrize("weak", ["airflow", "admin", "password", "airflow_jwt_secret"])
def test_known_weak_defaults_are_rejected(weak):
    """These are exactly the values the compose file used to fall back to."""
    env = {**GOOD_ENV, "AIRFLOW__API_AUTH__JWT_SECRET": weak}
    assert any("AIRFLOW__API_AUTH__JWT_SECRET" in p for p in check(env))


def test_empty_value_is_rejected():
    env = {**GOOD_ENV, "AIRFLOW__CORE__FERNET_KEY": ""}
    assert any("AIRFLOW__CORE__FERNET_KEY" in p for p in check(env))


def test_short_secret_is_rejected():
    env = {**GOOD_ENV, "GF_SECURITY_ADMIN_PASSWORD": "short"}
    assert any("GF_SECURITY_ADMIN_PASSWORD" in p for p in check(env))


def test_password_equal_to_username_is_rejected():
    env = {**GOOD_ENV, "POSTGRES_PASSWORD": "airflow"}
    assert any("POSTGRES_PASSWORD" in p for p in check(env))


def test_shipped_env_example_would_be_rejected():
    """The example file must never be usable as-is - that is the whole point."""
    example = parse_env_file(REPO_ROOT / ".env.example")
    assert check(example), ".env.example must not pass preflight"


def test_compose_has_no_fail_open_defaults_for_secrets():
    """F-03: a `:-default` on a secret boots the stack with a published value."""
    text = COMPOSE.read_text()
    secret_markers = ("PASSWORD", "SECRET", "FERNET", "TOKEN", "PEPPER")

    offenders = []
    for match in re.finditer(r"\$\{([A-Za-z0-9_]+):-([^}]*)\}", text):
        name = match.group(1)
        if any(marker in name.upper() for marker in secret_markers):
            offenders.append(f"{name} falls back to {match.group(2)!r}")

    assert not offenders, "secrets must use ${VAR:?} not ${VAR:-}: " + "; ".join(offenders)


def test_compose_does_not_hardcode_database_credentials():
    """F-02: the connection string must be composed from variables, not literals."""
    compose = yaml.safe_load(COMPOSE.read_text())
    pg_env = compose["services"]["postgres"]["environment"]

    for key in ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"):
        value = str(pg_env[key])
        assert value.startswith("${"), f"{key} is hardcoded as {value!r}"

    conn = compose["x-airflow-common"]["environment"]["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
    assert "${POSTGRES_PASSWORD" in conn, "connection string must interpolate the password"
    assert "airflow:airflow@" not in conn, "connection string still carries literal credentials"
