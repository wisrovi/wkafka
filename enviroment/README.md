# WKafka Test & Benchmark Environments

This directory provides containerized Docker Compose environments for local development, integration testing, and empirical latency benchmarking of the WKafka library.

---

## 🛠️ Key Technologies and Libraries

- **Apache Kafka (`apache/kafka:latest`)**: High-throughput distributed event streaming platform operating in native KRaft mode (ZooKeeper-less architecture).
- **Redis (`redis:7.2-alpine`)**: Ultra-fast in-memory key-value data structure store used for transient Claim-Check payload offloading.
- **Docker & Docker Compose v2**: Isolated container orchestration for deterministic local and CI verification.

---

## 🚀 Available Environments

### 1. Unified Benchmark Suite (`docker-compose.yml`)

Spins up both Apache Kafka on port `9092` and Redis on port `6379` configured for high-throughput video frame benchmarking:

```bash
# From wkafka/enviroment directory
docker compose up -d
```

To stop the services:
```bash
docker compose down
```

### 2. Standalone Redis Cache (`redis/docker-compose.yml`)

Spins up a lightweight Redis instance with AOF disabled and volatile-TTL optimization for in-memory payload offloading:

```bash
# From wkafka/enviroment/redis directory
docker compose up -d
```

### 3. Legacy Multi-Broker Kafka (`basic/`)

Contains legacy ZooKeeper and multi-broker topologies for partition scaling and distributed consumer group verification.
