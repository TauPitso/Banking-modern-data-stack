import os
import json
import time
import requests
from dotenv import load_dotenv

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

# -----------------------------
# Debug: confirm env vars actually loaded
# -----------------------------
print("DEBUG - POSTGRES_HOST:", repr(os.getenv("POSTGRES_HOST")))
print("DEBUG - POSTGRES_PORT:", repr(os.getenv("POSTGRES_PORT")))
print("DEBUG - POSTGRES_USER:", repr(os.getenv("POSTGRES_USER")))
print("DEBUG - POSTGRES_DB:", repr(os.getenv("POSTGRES_DB")))


# -----------------------------
# Build connector JSON in memory
# -----------------------------
connector_config = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.getenv("POSTGRES_HOST", "postgres"),
        "database.port": os.getenv("POSTGRES_PORT", "5432"),
        "database.user": os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": os.getenv("POSTGRES_DB"),
        "topic.prefix": "banking_server",
        "table.include.list": "public.customers,public.accounts,public.transactions",
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot",
        "snapshot.mode": "never",
        "publication.autocreate.mode": "filtered",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
    },
}

# -----------------------------
# Connect Kafka Connect service (Docker service name, not localhost)
# -----------------------------
url = "http://connect:8083/connectors"
headers = {"Content-Type": "application/json"}

# -----------------------------
# Wait for Kafka Connect to be ready before posting
# -----------------------------
MAX_RETRIES = 60
WAIT_SECONDS = 10

ready = False
for attempt in range(1, MAX_RETRIES + 1):
    try:
        health = requests.get("http://connect:8083/connectors", timeout=5)
        if health.status_code == 200:
            print(f"✅ Kafka Connect is ready (attempt {attempt}).")
            ready = True
            break
    except requests.exceptions.RequestException:
        pass
    print(f"⏳ Waiting for Kafka Connect... (attempt {attempt}/{MAX_RETRIES})")
    time.sleep(WAIT_SECONDS)

if not ready:
    print("❌ Kafka Connect never became ready. Exiting without registering connector.")
    raise SystemExit(1)

# -----------------------------
# Send request to Debezium Connect
# -----------------------------
response = requests.post(url, headers=headers, data=json.dumps(connector_config))

# -----------------------------
# Debug/Output
# -----------------------------
if response.status_code == 201:
    print("✅ Connector created successfully!")
elif response.status_code == 409:
    print("⚠️ Connector already exists.")
else:
    print(f"❌ Failed to create connector ({response.status_code}): {response.text}")