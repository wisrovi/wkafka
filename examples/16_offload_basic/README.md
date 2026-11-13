# Example 16: Basic Payload Offloading (Claim-Check Pattern)

This example demonstrates how to use WKafka's native **Payload Offloading** feature using the **Claim-Check pattern** with `WRedis`.

## Key Technologies and Libraries

- **Apache Kafka / `wkafka`**: High-performance distributed messaging system and Python orchestrator.
- **`wredis`**: High-speed in-memory database and caching layer for ephemeral binary storage.
- **OpenCV (`cv2`) & `numpy`**: Real-time image generation, matrix compression, and decoding.
- **Python 3.9+**: Modern asynchronous and synchronous execution runtime.

---

## Overview

When streaming high-frequency video frames (e.g., 30 FPS at 1080p), passing full image matrices directly through Kafka brokers can saturate broker memory, disks, and network interfaces.

With **Payload Offloading**:
1. The **Producer** compresses the image into JPEG bytes and stores it in Redis RAM with a short Time-To-Live (TTL = 5s).
2. Kafka brokers transfer only a tiny Claim-Check JSON pointer (~120 bytes) with headers.
3. The **Consumer** intercepts the pointer, fetches the binary frame directly from Redis RAM, decodes it into a `numpy.ndarray`, and supplies it to your handler.
4. If a consumer falls behind by more than 5 seconds, stale frames automatically evaporate from RAM, preventing downstream latency buildup.

---

## Code Example

### Producer (`producer.py`)
```python
from wkafka import WKafka

kafka = WKafka(
    bootstrap_servers="localhost:9092",
    offload={"backend": "redis", "ttl": 5}
)

# Offloads heavy format='image' to Redis RAM automatically:
kafka.send("camera-stream", value=frame, format="image")
```

### Consumer (`consumer.py`)
```python
from wkafka import WKafka

kafka = WKafka(
    bootstrap_servers="localhost:9092",
    offload={"backend": "redis", "ttl": 5}
)

@kafka.consumer("camera-stream", format="image")
def on_frame(msg):
    frame = msg.value  # Seamlessly receives the decoded numpy.ndarray
```

---

## Running the Example

1. Ensure Kafka and Redis are running locally:
   ```bash
   # Kafka on localhost:9092
   # Redis on localhost:6379
   ```

2. Start the consumer in one terminal:
   ```bash
   poetry run python examples/16_offload_basic/consumer.py
   ```

3. Start the producer in a second terminal:
   ```bash
   poetry run python examples/16_offload_basic/producer.py
   ```
