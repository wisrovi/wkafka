<img width="1040" height="582" alt="image" src="https://github.com/user-attachments/assets/e03505ea-5bb0-4e99-b80d-c0d5c261a322" />


# WKafka v1.0.0 LTS 🚀

**Professional, Decorator-based Kafka Wrapper for Python.**

WKafka simplifies Apache Kafka integration by providing a high-level, intuitive API focused on developer productivity. It includes built-in support for complex data types like JSON, YAML, and Images/Video, making it ideal for modern microservices, IoT, and Computer Vision pipelines.

---

## 🌟 Features

- **Decorator-driven API**: Minimalistic and clean message handling.
- **Modern Python**: Fully typed, PEP 8 compliant, supporting Python 3.9 through 3.14.
- **Enterprise Security**: Built-in support for SASL (PLAIN, SCRAM) and KRaft mode.
- **Multimedia Native**: Seamlessly send and receive images (OpenCV/NumPy/PIL).
- **Professional Ops**: Structured logging via `loguru` and multi-version testing with `tox`.
- **Easy Deployment**: Modern Docker environments provided for development.

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

### SASL Authentication
```python
kafka = WKafka(
    bootstrap_servers="my-secure-broker:30092",
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username="admin",
    sasl_plain_password="password"
)
```

## 📂 Project Structure

- `wkafka.core`: Orchestration and base logic.
- `wkafka.serializers`: Extensible serialization system.
- `wkafka.controller`: Backward compatibility layer for legacy code.
- `enviroment/`: Production-ready Docker setups (KRaft, SASL).

## 🧪 Compatibility

WKafka is tested against:
- **Python**: 3.9 | 3.10 | 3.11 | 3.12 | 3.13 | 3.14 (pre-release)
- **Kafka**: 2.x | 3.x (KRaft and Zookeeper)

## 🗺️ Roadmap (Próximas Funcionalidades)

WKafka busca simplificar Kafka sin perder la potencia de la librería base. Las siguientes funcionalidades están planeadas para futuras versiones:

1.  **Transacciones (Exactly-once Semantics)**: Soporte para productores transaccionales y envíos atómicos ("todo o nada").
2.  **Gestión Manual de Offsets**: Permitir el `.commit()` manual dentro de las funciones decoradas para un control total del procesamiento.
3.  **Suscripción por Regex**: Soporte para patrones de tópicos (ej. `@kafka.consumer(topic="logs.*")`).
4.  **Interceptores Globales**: Capacidad de añadir hooks de pre y post-procesamiento para todos los mensajes entrantes/salientes.
5.  **Particionado Personalizado**: Control granular sobre la distribución de mensajes en las particiones del broker.
6.  **Schema Registry Integration**: Soporte nativo para validación de esquemas Avro, Protobuf y JSON Schema.
7.  **Métricas en Tiempo Real**: Integración directa con Prometheus/Grafana para monitorear el lag y el throughput.
8.  **Headers Dinámicos**: Mejoras en la propagación de contextos y trazabilidad (OpenTelemetry).

## 🛠️ Tecnologías y Librerías Relevantes

- **Python (3.9 - 3.14)**: Lenguaje principal de desarrollo y ejecución.
- **kafka-python-ng / kafka-python**: Cliente subyacente para comunicación de bajo nivel con Apache Kafka.
- **OpenCV (`opencv-python`) & Pillow**: Procesamiento, renderizado, serialización y deserealización eficiente de imágenes.
- **NumPy**: Manejo de estructuras de datos matriciales multidimensionales para imágenes.
- **PyYAML**: Serialización y deserialización nativa de estructuras YAML.
- **Loguru**: Sistema avanzado de logging estructurado.
- **Pydantic**: Validación y definición de modelos de datos internos.

## 📜 License
MIT License. Created by [wisrovi](https://github.com/wisrovi).

