<img width="1040" height="582" alt="image" src="https://github.com/user-attachments/assets/e03505ea-5bb0-4e99-b80d-c0d5c261a322" />

<p align="center">
  <a href="https://pypi.org/project/wkafka/"><img src="https://img.shields.io/pypi/v/wkafka?style=for-the-badge&logo=pypi&color=3b82f6" alt="PyPI version" /></a>
  <a href="https://pypi.org/project/wkafka/"><img src="https://img.shields.io/pypi/dm/wkafka?style=for-the-badge&color=10b981" alt="PyPI Downloads" /></a>
  <a href="https://linkedin.com/in/wisrovi-rodriguez"><img src="https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white" alt="LinkedIn" /></a>
  <a href="https://wisrovi.dev"><img src="https://img.shields.io/badge/Author-wisrovi.dev-111827?style=for-the-badge&logo=google-chrome&logoColor=white" alt="Portal" /></a>
  <a href="https://orcid.org/0009-0005-0710-1861"><img src="https://img.shields.io/badge/ORCID-0009--0005--0710--1861-A6CE39?style=for-the-badge&logo=orcid&logoColor=white" alt="ORCID" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge" alt="License" /></a>
</p>

# WKafka 🚀

**Professional, Decorator-based Kafka Wrapper for Python.**

WKafka simplifies Apache Kafka integration by providing a high-level, intuitive API focused on developer productivity. It includes built-in support for complex data types like JSON, YAML, Images, Files, and Pydantic models, making it ideal for modern microservices, IoT, and Computer Vision pipelines.

---

## 🌟 Features

- **Decorator-driven API**: Minimalistic and clean message handling.
- **Modern Python**: Fully typed, PEP 8 compliant, supporting Python 3.9 through 3.14.
- **Enterprise Security**: Built-in support for SASL (PLAIN, SCRAM) and KRaft mode.
- **Multimedia & File Native**: Seamlessly send and receive images (OpenCV/NumPy/PIL) and arbitrary files (PDF, ZIP, TXT) via `format="file"`.
- **Type-safe Pydantic Validation**: Automatic schema validation with `format="pydantic"`.
- **Manual Offset Commit**: Control At-Least-Once delivery semantics with `auto_commit=False` and `msg.commit()`.
- **Immediate Commit on Receive**: Built-in support for At-Most-Once / early acknowledgment with `commit_on_receive=True`.
- **Decoupled Background Execution**: Non-blocking background worker dispatch with `background=True` and `background_workers=N` to prevent Kafka heartbeat timeouts during long-running tasks.
- **Partition Auto-scaling & Retries**: Automatic topic partition scaling with `describe_topics` inspection and exponential backoff retry resilience against transient `NodeNotReadyError`.
- **Async/Await Support**: Define non-blocking `async def` consumer handlers.
- **Professional Ops**: Structured logging via `loguru` and multi-version testing with `tox`.

---

## 📦 Installation

```bash
# Via pip
pip install wkafka

# Via poetry
poetry add wkafka
```

*Optional snappy compression:*
```bash
pip install wkafka[snappy]
```

---

## 🚀 Quick Start

### Basic Producer & Consumer
```python
from wkafka import WKafka

# Configures automatically via KAFKA_SERVER or defaults to localhost:9092
kafka = WKafka(bootstrap_servers="localhost:9092")

@kafka.consumer(topic="orders", format="json")
def handle_order(msg):
    print(f"New order received: {msg.value}")

# Start consumers in a background thread pool
kafka.run_consumers(block=True)

# Produce with context manager safety
with kafka.producer() as p:
    p.send("orders", value={"id": 123, "item": "Coffee"}, format="json")
```

### Manual Offset Commit
```python
@kafka.consumer(topic="transactions", format="json", auto_commit=False)
def handle_tx(msg):
    # Process business logic
    save_to_db(msg.value)
    # Explicitly commit offset only after success
    msg.commit()
```

### Retries & DLQ Routing
```python
@kafka.consumer(
    topic="unstable_events",
    format="json",
    max_retries=3,
    retry_delay=1.0,
    dlq_topic="unstable_events.DLQ"
)
def handle_event(msg):
    process_payload(msg.value)
```

### Immediate Commit on Receive (Early Acknowledgment)
```python
@kafka.consumer(topic="events", format="json", commit_on_receive=True)
def handle_early_ack(msg):
    # Offset is committed immediately upon receipt before entering this function
    process_quick_event(msg.value)
```

### Long-Running Tasks & Decoupled Background Workers (The ML & Video Dilemma)

In Apache Kafka, if a consumer handler takes longer than `max_poll_interval_ms` (default: 5 minutes) to return:
1. The Kafka broker assumes the consumer has crashed or deadlocked.
2. The broker evicts the consumer from the consumer group (**Consumer Group Rebalance**).
3. The message offset is never committed, and another consumer (or the restarted consumer) picks up the same message in an **infinite duplicate loop**.

**WKafka provides two native solutions that can be used independently or combined:**

* **`commit_on_receive=True`**: Forces an immediate offset commit to the Kafka broker as soon as the message is dequeued, before entering your handler function. This ensures that the message is acknowledged and will not be re-delivered if processing is interrupted.
* **`background=True`**: Dispatches the execution of your callback to an internal `ThreadPoolExecutor`. The main Kafka polling thread returns immediately to its `poll()` loop, continually answering broker heartbeats. Your heavy processing function can run for minutes or hours without ever triggering a timeout or rebalance!
* **`background_workers=N`**: Controls the maximum number of concurrent tasks executed in parallel.

```python
from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092")

# Perfect for ML inference, video processing, or PDF generation that takes hours
@kafka.consumer(
    topic="reports.generate",
    format="json",
    commit_on_receive=True,  # 1. Commit offset immediately upon receipt
    background=True,         # 2. Offload execution to a background worker pool
    background_workers=4,    # 3. Allow up to 4 parallel heavy tasks
)
def handle_long_task(msg):
    # Runs in a separate thread.
    # The Kafka consumer loop never blocks and never triggers max_poll_interval_ms rebalances!
    generate_heavy_report_and_pdf(msg.value)
```

#### Architecture Flow:
```
Kafka Topic ────────► Consumer Thread (poll)
                            │
                            ├─► commit_on_receive=True ──► [Commit Offset to Broker Immediately]
                            │
                            └─► background=True ─────────► [ThreadPoolExecutor Worker 1] ──► Long Task (Hours)
                                                         ├─► [ThreadPoolExecutor Worker 2] ──► Long Task (Hours)
                                                         └─► Consumer Thread Continues Polling (Heartbeats Active!)
```

---

## 📋 Consumer Configuration Reference (`@kafka.consumer`)

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `topic` | `str \| List[str]` | `None` | Topic or list of topics to subscribe to. |
| `pattern` | `str` | `None` | Regex pattern for dynamic multi-topic matching. |
| `group_id` | `str` | Auto-generated | Consumer group identifier. |
| `format` | `str` | `"json"` | Deserializer format (`"json"`, `"yaml"`, `"image"`, `"file"`, `"pydantic"`). |
| `auto_commit` | `bool` | `True` | Enables periodic automatic background offset commits by Kafka. |
| `commit_on_receive` | `bool` | `False` | Immediately commits the offset upon receipt before executing the handler. |
| `background` | `bool` | `False` | Executes handler in a background worker thread to keep the consumer poll loop active. |
| `background_workers` | `int` | `1` | Maximum parallel worker threads when `background=True`. |
| `max_retries` | `int` | `0` | Number of retry attempts on exception before routing to DLQ. |
| `retry_delay` | `float` | `1.0` | Initial exponential backoff delay in seconds between retries. |
| `dlq_topic` | `str` | `None` | Dead Letter Queue topic to route permanently failed messages. |
| `model` | `BaseModel` | `None` | Pydantic model class for automatic typed validation when `format="pydantic"`. |
| `key_filter` | `str` | `None` | Only processes messages matching this specific partition key. |
| `partition_scale` | `bool` | `None` | Enables dynamic partition scaling via KafkaAdminClient for true parallel workers. |

---

## 📂 Project Structure

- `wkafka.core`: Orchestration and base logic (`WKafka`, `Message`).
- `wkafka.serializers`: Extensible serialization system (`JSONSerializer`, `YAMLSerializer`, `ImageSerializer`, `PydanticSerializer`, `FileSerializer`).
- `wkafka.controller`: Backward compatibility layer for legacy code.
- `examples/`: 15 complete, production-ready example modules (`01_basic` through `15_long_running_background_tasks`).
- `environment/`: Production-ready Docker setups (KRaft, SASL).

---

## 🛠️ Tecnologías y Librerías Relevantes

- **Python (3.9 - 3.14)**: Lenguaje principal de desarrollo y ejecución.
- **kafka-python-ng / kafka-python**: Client library for low-level communication with Apache Kafka.
- **OpenCV (`opencv-python`) & Pillow**: Procesamiento, renderizado, serialización y deserialización de imágenes.
- **Pydantic**: Validación de esquemas y modelos de datos tipados (`format="pydantic"`).
- **NumPy**: Manejo de estructuras de datos matriciales multidimensionales para imágenes.
- **PyYAML**: Serialización y deserialización nativa de estructuras YAML.
- **Loguru**: Sistema avanzado de logging estructurado.

---

## 🧪 Unit Testing & Coverage

To run the unit test suite, calculate coverage, or execute tests inside a sandboxed Docker container:

### Run Locally (pytest)
```bash
poetry run pytest tests/
```

### Coverage Report (80% Minimum Guaranteed)
```bash
./run_coverage.sh
```
El script genera un informe de cobertura HTML (`htmlcov/index.html`) garantizando una cobertura mínima global de **80%**.

### Sandboxed Testing with Docker
```bash
./run_tests_docker.sh
```

---

## 👤 Author & Affiliation

* **William Steve Rodriguez Villamizar (Wisrovi)**
* **Cargo:** Principal AI Engineer & Applied AI Solutions Architect | Scientific Researcher
* 🌐 **Official Website:** [wisrovi.dev](https://wisrovi.dev)
* 💼 **LinkedIn:** [wisrovi-rodriguez](https://www.linkedin.com/in/wisrovi-rodriguez/)
* 🆔 **ORCID:** [0009-0005-0710-1861](https://orcid.org/0009-0005-0710-1861)
* 📦 **PyPI:** [pypi.org/user/wisrovi/](https://pypi.org/user/wisrovi/)
* 🐙 **GitHub:** [@wisrovi](https://github.com/wisrovi)

---

## 📜 License
Distributed under the **MIT License**. Open for industrial and research use.

