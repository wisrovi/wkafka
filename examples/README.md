# WKafka Examples Suite 📚

This suite provides hands-on, runnable examples for mastering **WKafka**, ranging from fundamental messaging patterns to advanced multimedia streaming and high-throughput payload offloading.

---

## 🗂️ Complete Examples Directory

| Directory | Level | Concept |
| :--- | :--- | :--- |
| `01_basic` | 🟢 Beginner | Simple JSON messaging (Producer/Consumer). |
| `02_advanced` | 🟡 Intermediate | Message metadata (Headers) and routing partition keys. |
| `03_multimedia` | 🟠 Specialized | Continuous image streaming with NumPy & OpenCV. |
| `04_security_sasl` | 🔴 Pro | Secure authentication using SASL (PLAIN / SCRAM). |
| `05_patterns` | 🔥 Architecture | Microservices communication patterns (Worker / Client). |
| `06_images` | 🖼️ Multimedia | Image transmission using Pillow PIL and OpenCV. |
| `07_files` | 📁 Files | Binary document & file transfer (PDF, ZIP, TXT) via `format="file"`. |
| `08_manual_commit` | ⚙️ Resilience | Manual offset management (`auto_commit=False` & `msg.commit()`) for At-Least-Once delivery. |
| `09_retries_and_dlq` | 🛡️ Resilience | Automatic retry logic (`max_retries`) and Dead Letter Queue (`dlq_topic`). |
| `10_multi_topic_regex` | 🎯 Routing | Multi-topic subscription (`topic=[...]`) and regex matching (`pattern="..."`). |
| `11_async_handlers` | ⚡ Async | Asynchronous consumer callbacks powered by `asyncio` and `async / await`. |
| `12_pydantic_validation` | 🛡️ Validation | Strongly-typed payload validation with `format="pydantic"`. |
| `13_partition_scale` | 🚀 Scalability | Dynamic topic partition auto-scaling (`partition_scale=True`) for true parallelism. |
| `14_interactive_producer` | 🎮 CLI Tools | Interactive CLI prompt to type and produce messages in real time. |
| `15_long_running_background_tasks` | ⚡ Heavy Compute | Decoupled background workers (`background=True`) and immediate commits (`commit_on_receive=True`). |
| `16_offload_basic` | 🚀 RAM Offloading | High-throughput Claim-Check pattern: heavy images stored in Redis RAM with 5s TTL, Kafka carries ~120 byte references. |
| `17_offload_extended` | 🌐 Enterprise Offload | Production Claim-Check: external YAML config, custom network host/port (`192.168.1.68:15963`), and drop-on-expired policies. |

---

## 🚀 Execution Guide

To run any example from the repository root:

1. **Start the Consumer (Terminal 1):**
   ```bash
   poetry run python examples/XX_folder/consumer.py
   ```

2. **Start the Producer (Terminal 2):**
   ```bash
   poetry run python examples/XX_folder/producer.py
   ```

---

## 🛠️ Key Technologies and Libraries

- **WKafka**: Professional Python orchestrator for Apache Kafka featuring transparent payload offloading, decorators, and multimedia serializers.
- **Apache Kafka**: Distributed event streaming platform.
- **WRedis (`wredis`)**: High-speed in-memory database and caching layer for ephemeral multimedia offloading.
- **PyYAML (`yaml`)**: Declarative configuration loading from YAML files.
- **OpenCV & Pillow (PIL)**: Computer vision matrix serialization, compression, and rendering.
- **Pydantic**: Type-safe payload validation.
- **NumPy**: Multidimensional array processing for video matrices.
- **Python asyncio**: Non-blocking asynchronous event loop for concurrent consumer handlers.
- **Loguru**: Structured logging and diagnostics.
