#!/usr/bin/env python3
"""Refuses to let the stack start with missing, placeholder, or weak secrets.

Compose's `:?` expansions already fail closed on a *missing* variable. They
cannot catch a variable that is present but still set to the value shipped in
.env.example - which is the more likely mistake, and the one that boots a
stack with a known password while looking like it worked.

Run before `docker compose up`:

    python security/preflight.py

Exits 0 if the environment is safe to start, 1 otherwise.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Variables that must exist, be non-empty, and not look like a placeholder.
REQUIRED_SECRETS = (
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
    "AIRFLOW__CORE__FERNET_KEY",
    "AIRFLOW__API_AUTH__JWT_SECRET",
    "_AIRFLOW_WWW_USER_PASSWORD",
    "GF_SECURITY_ADMIN_PASSWORD",
)

# Substrings that mark a value as "still the example". Matched case-insensitively
# against the whole value.
PLACEHOLDER_MARKERS = (
    "change_me",
    "changeme",
    "your_",
    "replace_me",
    "example",
    "placeholder",
    "xxx",
)

# Values weak enough to be worth rejecting outright even though they are not
# literal placeholders - these are the defaults the compose file used to carry.
KNOWN_WEAK_VALUES = {
    "airflow",
    "admin",
    "password",
    "postgres",
    "secret",
    "airflow_jwt_secret",
    "test",
}

MIN_SECRET_LENGTH = 12

# Required, but identifiers rather than credentials: "airflow" is a fine
# database name and a fine username. Only the placeholder check applies to
# these - strength rules would reject a perfectly good value.
IDENTIFIERS = {"POSTGRES_USER", "POSTGRES_DB"}


def parse_env_file(path: Path) -> dict[str, str]:
    """Parses KEY=VALUE lines, ignoring comments, blanks, and `export` prefixes."""
    values: dict[str, str] = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        line = re.sub(r"^export\s+", "", line)
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip().strip('"').strip("'")
        values[key.strip()] = value
    return values


def check(env: dict[str, str]) -> list[str]:
    """Returns a list of problems; empty means the environment is safe to start."""
    problems: list[str] = []

    for key in REQUIRED_SECRETS:
        if key not in env:
            problems.append(f"{key} is missing")
            continue

        value = env[key]
        if not value:
            problems.append(f"{key} is empty")
            continue

        lowered = value.lower()
        if any(marker in lowered for marker in PLACEHOLDER_MARKERS):
            problems.append(f"{key} still holds a placeholder value")
            continue

        if key in IDENTIFIERS:
            continue

        if lowered in KNOWN_WEAK_VALUES:
            problems.append(f"{key} is set to a well-known default ({value!r})")
            continue

        if len(value) < MIN_SECRET_LENGTH:
            problems.append(
                f"{key} is shorter than {MIN_SECRET_LENGTH} characters"
            )

    if env.get("POSTGRES_PASSWORD") and env.get("POSTGRES_PASSWORD") == env.get("POSTGRES_USER"):
        problems.append("POSTGRES_PASSWORD is identical to POSTGRES_USER")

    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        type=Path,
        default=REPO_ROOT / ".env",
        help="path to the .env file to check (default: repo root .env)",
    )
    args = parser.parse_args()

    if not args.env_file.exists():
        print(f"preflight: {args.env_file} does not exist - copy .env.example and fill it in",
              file=sys.stderr)
        return 1

    problems = check(parse_env_file(args.env_file))

    if problems:
        print(f"preflight: refusing to start - {len(problems)} problem(s) in {args.env_file}:",
              file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1

    print(f"preflight: {args.env_file} looks safe to start")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
