## Terra Kafka Broker & Telemetry Ingestion Worker


### Overview

- **Kafka Broker:** Receives messages from USV sensor streams.
- **Worker Service:** A Python-based consumer service running on Python 3.11 that deserializes incoming JSON payloads and writes them directly into MongoDB.
- **Zero-Secret Containers:** The container image contains only application logic. All MongoDB credentials, database names, and broker addresses are injected at runtime via environment variables or `.env` files.

---

### Monitored Kafka Topics

The worker automatically subscribes to:
- `usv.telemetry`: USV sensor data, speed, heading, and battery status.
- `gps_data`: Real-time coordinate streams.
- `environmental.data`: External sensor measurements.

---

### Repository Structure

```text
├── Dockerfile              # Builds the standalone Python worker image
├── .dockerignore           # Prevents secrets (.env) from leaking into Docker images
├── docker-compose.yaml     # Portainer / Docker Compose orchestration
├── worker.py               # Kafka consumer and MongoDB ingestion script
└── .env.example            # Sample configuration file
```

### 1. Publish a test message to Kafka 
    echo '{"id": "2026-07-23T16:15:31.376913+00:00", "gps": {"latitude": 38.8630954, "longitude": 22.5033712, "altitude": 1.88}, "environment": {"temperature": 30.9465942383, "ph": 13.09000015258789, "tds": 939.3574062, "ec": 30.3572656, "turbidity": 1972.3758828125}}' | \
      docker exec -i terra-kafka-broker /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server <server-name>:9094 \
    --topic environmental.data

### 2. Check the worker container logs to verify message ingestion
    docker logs terra-kafka-worker --tail 10

### 3. Query MongoDB to confirm persistence
    docker exec -i terra-mongo mongosh "$MONGO_URI" --eval 'db.telemetry.find().sort({_id: -1}).limit(1).pretty()'
