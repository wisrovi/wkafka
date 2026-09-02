# WKafka Examples Suite 📚

Esta suite está diseñada para que domines **WKafka v1.0.0 LTS** paso a paso, desde los conceptos básicos hasta las características avanzadas de arquitectura de microservicios y procesamiento multimedia.

---

## 🗂️ Estructura Completa de Ejemplos

| Carpeta | Nivel | Concepto |
| :--- | :--- | :--- |
| `01_basic` | 🟢 Fácil | Mensajería JSON simple (Producer/Consumer). |
| `02_advanced` | 🟡 Intermedio | Metadatos adicionales (Headers) y claves de routing (Keys). |
| `03_multimedia` | 🟠 Especial | Streaming continuo de imágenes (NumPy/OpenCV). |
| `04_security_sasl` | 🔴 Pro | Autenticación de seguridad SASL (PLAIN / SCRAM). |
| `05_patterns` | 🔥 Arquitecto | Patrón de Microservicios (Worker / Client). |
| `06_images` | 🖼️ Multimedia | Envío y recepción de imágenes (Pillow PIL & OpenCV). |
| `07_files` | 📁 Archivos | Transmisión de archivos binarios/documentos (PDF, ZIP, TXT) via `format="file"`. |
| `08_manual_commit` | ⚙️ Resiliencia | Gestión manual de offsets (`auto_commit=False` & `msg.commit()`) At-Least-Once. |
| `09_retries_and_dlq` | 🛡️ Resiliencia | Reintentos automáticos (`max_retries`) y Dead Letter Queue (`dlq_topic`). |
| `10_multi_topic_regex` | 🎯 Routing | Suscripción a múltiples tópicos (`topic=[...]`) y patrones Regex (`pattern="..."`). |
| `11_async_handlers` | ⚡ Asíncrono | Handlers consumidores asíncronos con `async / await` e `asyncio`. |
| `12_pydantic_validation` | 🛡️ Validación | Validación tipada de modelos Pydantic con `format="pydantic"`. |
| `13_partition_scale` | 🚀 Escalabilidad | Auto-escalado dinámico de particiones por tópico (`partition_scale=True`) para paralelismo real. |
| `14_interactive_producer` | 🎮 Herramientas | Consola interactiva CLI para escribir y publicar mensajes en tiempo real. |

---

## 🚀 Guía de Uso General

Para cualquier módulo de ejemplo `XX_folder`:

1. **Inicia el Consumidor (Terminal 1):**
   ```bash
   poetry run python examples/XX_folder/consumer.py
   ```
2. **Inicia el Productor (Terminal 2):**
   ```bash
   poetry run python examples/XX_folder/producer.py
   ```

---

## 🛠️ Tecnologías y Librerías Relevantes

- **WKafka**: Orquestador Python para Apache Kafka con decoradores, gestión de offsets, reintentos y serializadores automáticos.
- **Apache Kafka**: Plataforma distribuida de procesamiento e ingesta de eventos en tiempo real.
- **OpenCV & Pillow (PIL)**: Serialización, procesamiento y renderizado de imágenes/video.
- **Pydantic**: Validación y tipado estricto de datos JSON de entrada y salida.
- **NumPy**: Arreglos numéricos multidimensionales para representación matricial de datos.
- **Python asyncio**: Bucle de eventos no bloqueante para funciones consumidoras asíncronas.
- **Loguru**: Sistema avanzado de trazabilidad y logging de eventos.
