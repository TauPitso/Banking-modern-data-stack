import os
from datetime import UTC, datetime, timedelta

import boto3
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# -------- MinIO Config --------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET = os.getenv("MINIO_BUCKET", "raw")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

# -------- Postgres Config --------
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
PG_DB = os.getenv("POSTGRES_DB", "banking")
PG_SCHEMA = os.getenv("POSTGRES_SCHEMA", "raw")

TABLES = ["customers", "accounts", "transactions"]

# Only pick up files modified within the last N minutes — keeps each run lean
LOOKBACK_MINUTES = 10


# -------- Python Callables --------
def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    cutoff = datetime.now(UTC) - timedelta(minutes=LOOKBACK_MINUTES)
    local_files = {}

    for table in TABLES:
        prefix = f"{table}/"
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        objects = resp.get("Contents", [])
        local_files[table] = []

        for obj in objects:
            # Skip files older than the lookback window
            if obj["LastModified"] < cutoff:
                continue

            key = obj["Key"]
            local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
            s3.download_file(BUCKET, key, local_file)
            print(f"Downloaded {key} -> {local_file}")
            local_files[table].append(local_file)

        print(f"[{table}] {len(local_files[table])} new file(s) found.")

    return local_files


def load_to_postgres(**kwargs):
    local_files = kwargs["ti"].xcom_pull(task_ids="download_minio")
    if not local_files:
        print("No files found in MinIO.")
        return

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB,
    )
    cur = conn.cursor()

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA};")
    conn.commit()

    for table, files in local_files.items():
        if not files:
            print(f"No new files for {table}, skipping.")
            continue

        # Create table from first file's columns if it doesn't exist yet
        first_df = pd.read_parquet(files[0])
        columns = ", ".join([f'"{col}" TEXT' for col in first_df.columns])
        cur.execute(f"CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.{table} ({columns});")
        conn.commit()

        # Insert all new rows — no truncate, since we're only loading new files
        for f in files:
            df = pd.read_parquet(f)
            for _, row in df.iterrows():
                placeholders = ", ".join(["%s"] * len(row))
                insert_sql = f'INSERT INTO {PG_SCHEMA}.{table} VALUES ({placeholders})'
                cur.execute(insert_sql, tuple(row))
            conn.commit()
            print(f"Loaded {f} -> {PG_SCHEMA}.{table}")

    cur.close()
    conn.close()


# -------- Airflow DAG --------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="minio_to_postgres_banking",
    default_args=default_args,
    description="Load MinIO parquet files into Postgres raw schema",
    schedule_interval="*/5 * * * *",  # every 5 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2 = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        provide_context=True,
    )

    task1 >> task2