# tests/test_security_supply_chain.py
"""Proves C-08 (dependencies and images are pinned) and C-09 (nothing is
fetched from the network at pipeline runtime).

Both controls are marked `partial` in security/controls.yaml: images are
tag-pinned rather than digest-pinned, and the egress-blocked smoke test that
would fully prove C-09 has not been written. These tests cover what is
actually true today, which is the point of the status field.
"""
import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
COMPOSE = REPO_ROOT / "docker-compose.yml"
DOCKERFILE = REPO_ROOT / "airflow" / "Dockerfile"
PACKAGES_YML = REPO_ROOT / "dbt_project" / "packages.yml"
DAG_FILE = REPO_ROOT / "airflow" / "dags" / "transaction_pipeline.py"
PYPROJECT = REPO_ROOT / "pyproject.toml"


def compose_images() -> list[str]:
    compose = yaml.safe_load(COMPOSE.read_text())
    images = [svc["image"] for svc in compose["services"].values() if "image" in svc]
    if "image" in compose.get("x-airflow-common", {}):
        images.append(compose["x-airflow-common"]["image"])
    return images


# --- C-08 -------------------------------------------------------------------

def test_no_image_uses_a_floating_tag():
    """`latest` means the same compose file resolves to different bytes over time."""
    floating = [img for img in compose_images() if img.endswith(":latest") or ":" not in img]
    assert not floating, f"images must be pinned to a version, not a floating tag: {floating}"


def test_every_image_has_an_explicit_version():
    for image in compose_images():
        _, _, tag = image.rpartition(":")
        assert tag and tag != "latest", f"{image} has no explicit version"


def test_dbt_packages_are_exact_pinned():
    """A range lets a scheduled run execute macro SQL that never existed when
    this code was written."""
    packages = yaml.safe_load(PACKAGES_YML.read_text())["packages"]
    assert packages, "packages.yml should declare packages"

    for package in packages:
        version = package["version"]
        assert isinstance(version, str), (
            f"{package['package']} uses a range ({version}) - pin it exactly"
        )
        assert re.fullmatch(r"\d+\.\d+\.\d+", version), (
            f"{package['package']} version {version!r} is not an exact x.y.z pin"
        )


def test_python_dependencies_are_exact_pinned():
    """dbt and duckdb decide what the models can do; they get exact pins."""
    text = PYPROJECT.read_text()
    block = text.split("dependencies = [", 1)[1].split("]", 1)[0]

    for line in block.splitlines():
        dep = line.strip().strip('",')
        if not dep:
            continue
        assert "==" in dep, f"{dep!r} is not exactly pinned"


# --- C-09 -------------------------------------------------------------------

def test_dag_does_not_fetch_dbt_packages_at_runtime():
    """The gate: `dbt deps` must not be a task in the pipeline."""
    dag_source = DAG_FILE.read_text()
    assert '"deps"' not in dag_source and "'deps'" not in dag_source, (
        "the DAG still runs `dbt deps` - a scheduled run would fetch from the dbt hub "
        "before the models execute"
    )


def test_image_installs_dbt_packages_at_build_time():
    dockerfile = DOCKERFILE.read_text()
    assert "dbt deps" in dockerfile, "packages must be resolved during the image build"
    assert "DBT_PACKAGES_PATH" in dockerfile, (
        "the baked packages must live outside the bind-mounted project dir, "
        "or the mount shadows them at runtime"
    )


def test_packages_install_path_is_configurable():
    """dbt must look where the image put them, without hardcoding a path that
    breaks local development."""
    project = (REPO_ROOT / "dbt_project" / "dbt_project.yml").read_text()
    assert "packages-install-path" in project
    assert "DBT_PACKAGES_PATH" in project


@pytest.mark.parametrize("service", ["prometheus", "statsd-exporter", "grafana"])
def test_observability_services_drop_capabilities(service):
    """F-09: no cap_drop anywhere was one of the original findings."""
    compose = yaml.safe_load(COMPOSE.read_text())
    svc = compose["services"][service]

    assert svc.get("cap_drop") == ["ALL"], f"{service} should drop all capabilities"
    assert "no-new-privileges:true" in svc.get("security_opt", []), (
        f"{service} should set no-new-privileges"
    )


@pytest.mark.parametrize("service,port", [("prometheus", "9090"), ("statsd-exporter", "9102")])
def test_internal_services_are_not_published_to_the_lan(service, port):
    """F-07: these publish pipeline structure and volumes to anyone who asks."""
    compose = yaml.safe_load(COMPOSE.read_text())
    for mapping in compose["services"][service].get("ports", []):
        if port in str(mapping):
            assert str(mapping).startswith("127.0.0.1:"), (
                f"{service} publishes {port} on all interfaces; bind it to loopback"
            )
