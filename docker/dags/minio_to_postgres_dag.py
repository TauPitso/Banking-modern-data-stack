import os
import boto3
import psycopg2
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# -------- MinIO Config --------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

# -------- Postgres Config --------
PG_HOST = os.getenv("POSTGRES_HOST")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PG_DB = os.getenv("POSTGRES_DB")
PG_SCHEMA = os.getenv("POSTGRES_SCHEMA", "raw")

TABLES = ["customers", "accounts", "transactions"]

# -------- Python Callables --------
def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    local_files = {}
    for table in TABLES:
        prefix = f"{table}/"
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        objects = resp.get("Contents", [])
        local_files[table] = []
        for obj in objects:
            key = obj["Key"]
            local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
            s3.download_file(BUCKET, key, local_file)
            print(f"Downloaded {key} -> {local_file}")
            local_files[table].append(local_file)
    return local_files


def load_to_postgres(**kwargs):
    # Pull the file dictionary from the previous task via XCom
    local_files = kwargs["ti"].xcom_pull(task_ids="download_minio")
    if not local_files:
        print("No files found in MinIO.")
        return

    # Connect to Postgres
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB
    )
    cur = conn.cursor()

    # Make sure the raw schema exists
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA};")
    conn.commit()

    for table, files in local_files.items():
        if not files:
            print(f"No files for {table}, skipping.")
            continue

        # Read the first file just to get the column structure for CREATE TABLE
        first_df = pd.read_parquet(files[0])
        columns = ", ".join([f'"{col}" TEXT' for col in first_df.columns])
 
        # Create the table if it doesn't exist yet
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.{table} ({columns});
        """)
        conn.commit()
 
        # Truncate ONCE per table before loading any files — prevents duplicates
        # across runs while still loading all files in this run
        cur.execute(f"TRUNCATE TABLE {PG_SCHEMA}.{table};")
        conn.commit()
        print(f"Truncated {PG_SCHEMA}.{table} before fresh load")

        # Now loop through all files and insert all rows
        for f in files:
            # Read the parquet file into a dataframe
            df = pd.read_parquet(f)

            # Build a CREATE TABLE statement from the dataframe columns if it doesn't exist
            columns = ", ".join([f'"{col}" TEXT' for col in df.columns])
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.{table} ({columns});
            """)
            conn.commit()


            # Insert rows one by one using parameterised queries (safe from SQL injection)
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
    schedule_interval="@hourly",
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
