# dockerfile-airflow.dockerfile
FROM apache/airflow:2.9.3

USER airflow

# Install dbt for Postgres
RUN pip install --no-cache-dir dbt-core dbt-postgres
