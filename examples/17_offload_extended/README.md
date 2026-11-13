# Example 17: Extended Payload Offloading (YAML & Production Options)

This example illustrates advanced **Payload Offloading** configuration patterns, including YAML files, custom network parameters, and expiration policies.

## Key Technologies and Libraries

- **Apache Kafka / `wkafka`**: Scalable event streaming platform with automated payload offloading.
- **`wredis`**: High-availability in-memory key-value database.
- **PyYAML (`yaml`)**: Declarative configuration file loader.
- **OpenCV (`cv2`) & `numpy`**: Computer vision and numeric matrix processing.

---

## Configuration Modes

### 1. External YAML File (`config.yaml`)
Externalizing configuration allows Kubernetes ConfigMaps or Docker Compose volumes to adjust connection parameters and TTL without changing Python code:

```yaml
kafka:
  bootstrap_servers: "192.168.1.68:9092"
  client_id: "vision-pipeline-extended"
  offload:
    backend: "redis"
    host: "192.168.1.68"
    port: 15963
    password: null
    db: 0
    ttl: 5
    on_expired: "drop"
    formats:
      - "image"
      - "file"
    threshold_kb: 50
    key_prefix: "wkafka:claim:camera_1"
```

In Python:
```python
from wkafka import WKafka

# Loads both Kafka and offload settings from the YAML file
kafka = WKafka(config="config.yaml")
```

---

### 2. In-Memory Extended Dictionary
You can also supply the same configuration directly via dictionary:

```python
kafka = WKafka(
    bootstrap_servers="localhost:9092",
    offload={
        "backend": "redis",
        "host": "192.168.1.68",
        "port": 15963,
        "ttl": 5,
        "on_expired": "drop",
        "threshold_kb": 50,
        "key_prefix": "wkafka:claim:camera_1",
    }
)
```

---

## Running the Example

1. Start the extended consumer:
   ```bash
   poetry run python examples/17_offload_extended/consumer.py
   ```

2. Start the extended producer:
   ```bash
   poetry run python examples/17_offload_extended/producer.py
   ```
