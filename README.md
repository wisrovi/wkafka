<img width="1040" height="582" alt="image" src="https://github.com/user-attachments/assets/e03505ea-5bb0-4e99-b80d-c0d5c261a322" />


# WKafka v1.0.0 LTS 🚀

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
- **Retries & Dead Letter Queue**: Automatic exponential backoff retries and DLQ routing (`max_retries`, `dlq_topic`).
- **Multi-Topic & Regex Subscription**: Subscribe to topic lists (`topic=["a", "b"]`) or patterns (`pattern="sensor_.*"`).
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

---

## 📂 Project Structure

- `wkafka.core`: Orchestration and base logic (`WKafka`, `Message`).
- `wkafka.serializers`: Extensible serialization system (`JSONSerializer`, `YAMLSerializer`, `ImageSerializer`, `PydanticSerializer`, `FileSerializer`).
- `wkafka.controller`: Backward compatibility layer for legacy code.
- `examples/`: 12 complete, production-ready example modules (`01_basic` through `12_pydantic_validation`).
- `enviroment/`: Production-ready Docker setups (KRaft, SASL).

---

## 🛠️ Tecnologías y Librerías Relevantes

- **Python (3.9 - 3.14)**: Lenguaje principal de desarrollo y ejecución.
- **kafka-python-ng / kafka-python**: Cliente subyacente para comunicación de bajo nivel con Apache Kafka.
- **OpenCV (`opencv-python`) & Pillow**: Procesamiento, renderizado, serialización y deserialización de imágenes.
- **Pydantic**: Validación de esquemas y modelos de datos tipados (`format="pydantic"`).
- **NumPy**: Manejo de estructuras de datos matriciales multidimensionales para imágenes.
- **PyYAML**: Serialización y deserialización nativa de estructuras YAML.
- **Loguru**: Sistema avanzado de logging estructurado.

---

## 📜 License
MIT License. Created by [wisrovi](https://github.com/wisrovi).
