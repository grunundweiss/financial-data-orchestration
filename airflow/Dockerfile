# airflow/Dockerfile
# Extends the official Airflow image with the dbt tooling the DAG needs
# (see https://airflow.apache.org/docs/docker-stack/build.html).
#
# Build context is the repository root (see docker-compose.yml), not this
# directory, because the dbt packages are resolved here at build time and
# packages.yml lives in dbt_project/.
FROM apache/airflow:3.2.2

COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    # dbt-core pins mashumaro<3.15, but every version in that range fails to
    # import on newer Python interpreters. 3.17 fixes it and is compatible with
    # dbt-core 1.11.x; --no-deps is needed because pip's resolver won't install
    # it in one pass alongside dbt-core's own stale upper bound.
    && pip install --no-cache-dir --no-deps mashumaro==3.17

# dbt packages are resolved at build time, not by a task in the DAG. A `dbt
# deps` inside the pipeline means every scheduled run reaches out to the dbt
# hub before the models execute - a network dependency at 03:00, and a third
# party's ability to change what your SQL does between runs. packages.yml uses
# exact versions, so this is reproducible.
#
# They are installed outside /opt/airflow/dbt_project because that path is a
# bind mount at runtime and would shadow anything baked in here;
# DBT_PACKAGES_PATH points dbt at this copy (see dbt_project/dbt_project.yml).
ENV DBT_PACKAGES_PATH=/opt/airflow/dbt_packages
# --chown matters here: RUN steps in this image execute as the non-root
# airflow user, but COPY writes files as root regardless of that. Without
# --chown, `dbt deps` can't write package-lock.yml (or its own log file) into
# this directory and fails silently before printing anything.
COPY --chown=airflow:root dbt_project/packages.yml dbt_project/dbt_project.yml /home/airflow/dbt_build/
COPY --chown=airflow:root dbt_project/profiles.yml /home/airflow/dbt_build/profiles.yml
RUN dbt deps --project-dir /home/airflow/dbt_build --profiles-dir /home/airflow/dbt_build \
    && rm -rf /home/airflow/dbt_build
