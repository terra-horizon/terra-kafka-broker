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
```

### Publish a test message to Kafka 
    curl -k -X POST \
    -H "Content-Type: application/vnd.kafka.json.v2+json" \
    -H "Accept: application/vnd.kafka.v2+json" \
    --data '{"records":[{"value":{"latitude":38.2466,"longitude":21.7346}}]}' \
    https://server.iprism.eu/terra/kafka-broker/topics/gps_data
  
