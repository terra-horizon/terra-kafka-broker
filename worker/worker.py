from __future__ import annotations

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

from dotenv import load_dotenv

env_file = os.environ.get("TERRA_ENV_FILE")
if env_file:
    load_dotenv(env_file)
else:
    load_dotenv()

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pymongo import MongoClient
from pymongo.errors import PyMongoError


# ---------------------------------------------------------------------------
# Storage Settings (Auto-parses remote MONGO_URI)
# ---------------------------------------------------------------------------

class StorageSettings:
    def __init__(self):
        self.enabled = os.getenv("TERRA_STORAGE_ENABLED", "true").lower() in ("true", "1", "yes")
        self.mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo:27017/terra")
        
        # Extract database name directly from the URI path: /dbname -> dbname
        parsed = urlparse(self.mongo_uri)
        path_db = parsed.path.lstrip("/").split("?")[0]
        self.mongo_database = path_db if path_db else os.getenv("MONGO_DATABASE", "usv_raw_data")
        
        # Hide credentials for clean console logging (only prints host:port)
        if "@" in parsed.netloc:
            self.mongo_target = parsed.netloc.split("@")[-1]
        else:
            self.mongo_target = parsed.netloc or "remote-mongo"

    @classmethod
    def from_env(cls):
        return cls()


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TOPICS = [
    "environmental.data",
    "gps_data",
    "usv.telemetry",
]

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "terra-mongodb-worker")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "usv_raw_data")

running = True


def shutdown_handler(signum, frame):
    global running
    print(f"[Worker] Received signal {signum}. Shutting down...", flush=True)
    running = False


signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)


def json_deserializer(data: bytes):
    return json.loads(data.decode("utf-8"))


def ensure_collection(db):
    """
    Attempts to create a time-series collection. If it already exists or if
    the user lacks admin permissions, logs a notice and continues safely.
    MongoDB will create a standard collection automatically on first write.
    """
    try:
        if MONGO_COLLECTION not in db.list_collection_names():
            print(f"[MongoDB] Creating time-series collection: {MONGO_COLLECTION}", flush=True)
            db.create_collection(
                MONGO_COLLECTION,
                timeseries={"timeField": "timestamp"},
            )
            print(f"[MongoDB] Time-series collection created: {MONGO_COLLECTION}", flush=True)
    except Exception as exc:
        print(f"[MongoDB] Notice: Skipping explicit collection creation ({exc}). Will auto-create on write.", flush=True)


def connect_mongo(settings):
    print(f"[MongoDB] Connecting to {settings.mongo_target} (Database: {settings.mongo_database})...", flush=True)
    while running:
        try:
            client = MongoClient(
                settings.mongo_uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
            )
            # Test authentication and connectivity
            client.admin.command("ping")
            print("[MongoDB] Connected successfully.", flush=True)
            return client
        except PyMongoError as exc:
            print(f"[MongoDB] Waiting for remote MongoDB ({exc})...", flush=True)
            time.sleep(3)
    return None


def connect_kafka():
    print(f"[Kafka] Connecting to {KAFKA_BROKER}...", flush=True)
    while running:
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                group_id=KAFKA_GROUP_ID,
                value_deserializer=json_deserializer,
                max_poll_interval_ms=300000,
                session_timeout_ms=30000,
                heartbeat_interval_ms=3000,
            )
            print("[Kafka] Connected successfully.", flush=True)
            return consumer
        except KafkaError as exc:
            print(f"[Kafka] Waiting for Kafka broker ({exc})...", flush=True)
            time.sleep(3)
    return None


def main() -> int:
    settings = StorageSettings.from_env()
    if not settings.enabled:
        print("[MongoDB] Storage is disabled.", file=sys.stderr, flush=True)
        return 1

    mongo_client = connect_mongo(settings)
    if not mongo_client:
        return 1

    db = mongo_client[settings.mongo_database]
    ensure_collection(db)
    collection = db[MONGO_COLLECTION]

    consumer = connect_kafka()
    if not consumer:
        if mongo_client:
            mongo_client.close()
        return 1

    print(f"[Worker] Waiting for Kafka messages to write into {settings.mongo_database}.{MONGO_COLLECTION}...", flush=True)

    try:
        while running:
            records = consumer.poll(timeout_ms=1000)
            if not records:
                continue

            for topic_partition, messages in records.items():
                for message in messages:
                    if not running:
                        break
                    try:
                        if message.value is None:
                            continue

                        document = {
                            "timestamp": datetime.now(timezone.utc),
                            "topic": message.topic,
                            "data": message.value,
                        }
                        result = collection.insert_one(document)
                        print(f"[MongoDB] Inserted topic={message.topic} id={result.inserted_id}", flush=True)
                        consumer.commit()
                    except Exception as exc:
                        print(f"[ERROR] Failed processing record: {exc}", file=sys.stderr, flush=True)
                        break
    finally:
        if consumer:
            consumer.close()
        if mongo_client:
            mongo_client.close()
        print("[Worker] Clean shutdown complete.", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
