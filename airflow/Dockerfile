# airflow/Dockerfile
# Extends the official Airflow image with the dbt tooling the DAG needs
# (see https://airflow.apache.org/docs/docker-stack/build.html).
FROM apache/airflow:3.2.2

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    # dbt-core pins mashumaro<3.15, but every version in that range fails to
    # import on newer Python interpreters. 3.17 fixes it and is compatible with
    # dbt-core 1.11.x; --no-deps is needed because pip's resolver won't install
    # it in one pass alongside dbt-core's own stale upper bound.
    && pip install --no-cache-dir --no-deps mashumaro==3.17
