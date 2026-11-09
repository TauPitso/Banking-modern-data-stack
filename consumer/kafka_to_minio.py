import boto3
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import os
import time
import signal
import sys
from dotenv import load_dotenv

# -----------------------------
# Load secrets from .env
# -----------------------------
load_dotenv()

import socket

# Wait for Kafka to be reachable before connecting
kafka_host = "kafka"
kafka_port = 9092
MAX_RETRIES = 30
WAIT_SECONDS = 10

print("⏳ Waiting for Kafka to be reachable...")
for attempt in range(1, MAX_RETRIES + 1):
    try:
        sock = socket.create_connection((kafka_host, kafka_port), timeout=5)
        sock.close()
        print(f"✅ Kafka is reachable (attempt {attempt}).")
        break
    except OSError:
        print(f"⏳ Kafka not ready yet... ({attempt}/{MAX_RETRIES})")
        time.sleep(WAIT_SECONDS)
else:
    print("❌ Kafka never became reachable. Exiting.")
    sys.exit(1)

# Kafka consumer settings
consumer = KafkaConsumer(
    'banking_server.public.customers',
    'banking_server.public.accounts',
    'banking_server.public.transactions',
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=os.getenv("KAFKA_GROUP"),
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=5000  # stop blocking after 5s of no new messages, so we can check the flush timer
)

# MinIO client
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

bucket = os.getenv("MINIO_BUCKET")

# Create bucket if not exists
if bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=bucket)

# Consume and write function
def write_to_minio(table_name, records):
    if not records:
        return
    df = pd.DataFrame(records)
    date_str = datetime.now().strftime('%Y-%m-%d')
    file_path = f'{table_name}_{date_str}.parquet'
    df.to_parquet(file_path, engine='fastparquet', index=False)
    s3_key = f'{table_name}/date={date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
    s3.upload_file(file_path, bucket, s3_key)
    os.remove(file_path)
    print(f'✅ Uploaded {len(records)} records to s3://{bucket}/{s3_key}')

# Batch consume
batch_size = 50
flush_interval_seconds = 10  # flush whatever is buffered at least this often

buffer = {
    'banking_server.public.customers': [],
    'banking_server.public.accounts': [],
    'banking_server.public.transactions': []
}

last_flush_time = time.time()


def flush_all(reason="periodic"):
    """Flush any non-empty buffers, regardless of size."""
    for topic, records in buffer.items():
        if records:
            print(f"⏳ Flushing {len(records)} buffered records for {topic} ({reason})")
            write_to_minio(topic.split('.')[-1], records)
            buffer[topic] = []


def handle_shutdown(signum, frame):
    """Catch Ctrl+C / termination and flush before exiting."""
    print("\n🛑 Shutdown signal received. Flushing remaining buffers before exit...")
    flush_all(reason="shutdown")
    consumer.close()
    sys.exit(0)


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

print("✅ Connected to Kafka. Listening for messages...")

while True:
    try:
        for message in consumer:
            topic = message.topic
            event = message.value
            payload = event.get("payload", {})
            record = payload.get("after")  # Only take the actual row

            if record:
                buffer[topic].append(record)
                print(f"[{topic}] -> {record}")  # Debugging

            if len(buffer[topic]) >= batch_size:
                write_to_minio(topic.split('.')[-1], buffer[topic])
                buffer[topic] = []
                last_flush_time = time.time()

            # Time-based flush check even while messages are actively arriving
            if time.time() - last_flush_time >= flush_interval_seconds:
                flush_all(reason="time interval reached")
                last_flush_time = time.time()

        # consumer_timeout_ms triggers StopIteration after 5s of silence
        # this is where we land if no new messages arrived — check the timer here too
        if time.time() - last_flush_time >= flush_interval_seconds:
            flush_all(reason="idle timeout reached")
            last_flush_time = time.time()

    except KeyboardInterrupt:
        handle_shutdown(None, None)