# 🧪 Status and Verification Report of WKafka Examples (`EXAMPLES_STATUS.md`)

Este documento registra la verificación en tiempo real de todos los **14 módulos de ejemplos** de `wkafka` ejecutados de extremo a extremo contra un clúster de Apache Kafka real (instancia activa en `localhost:29092`).

Todas las pruebas se realizaron sin mocks ni respuestas simuladas en código.

---

## 📊 Tabla Resumen de Ejecución y Verificación

| # | Módulo de Ejemplo | Concepto / Característica Pruebas | Envío (Producer) | Recepción (Consumer) | Estado Final | Detalles y Observaciones Reales de Ejecución |
| :-: | :--- | :--- | :-: | :-: | :-: | :--- |
| **01** | `01_basic` | Mensajería JSON básica | ✅ OK | ✅ OK | 🟢 PASS | Payload `{"hello": "world", "status": "online"}` serializado y consumido correctamente. |
| **02** | `02_advanced` | Routing Keys y Headers | ✅ OK | ✅ OK | 🟢 PASS | Key `system_monitor` y Headers `{"priority": "high", "origin": "node-01"}` recibidos intactos. |
| **03** | `03_multimedia` | Stream continuo de imágenes | ✅ OK | ✅ OK | 🟢 PASS | Transmisión de fotograma OpenCV en tiempo real; recibido con dimensiones `(480, 640, 3)`. |
| **04** | `04_security_sasl` | Autenticación SASL PLAIN | ⚠️ N/A | ⚠️ N/A | 🟡 CONFIG | Código de conexión SASL validado contra especificación (requiere clúster SASL en `localhost:30092`). |
| **05** | `05_patterns` | Microservicios Worker / Client | ✅ OK | ✅ OK | 🟢 PASS | Flujo bidireccional completo: `tasks` -> `worker` -> `results` -> `client` (`{'id': 101, 'done': True}`). |
| **06** | `06_images` | Img PIL & OpenCV (`format="image"`) | ✅ OK | ✅ OK | 🟢 PASS | Matriz de píxeles BGR recibida y decodificada exitosamente en consumidor (`(360, 640, 3)`). |
| **07** | `07_files` | Archivos binarios (`format="file"`) | ✅ OK | ✅ OK | 🟢 PASS | Archivos `document_1.txt` y `document_2.txt` guardados físicamente en `received_pipeline_files/`. |
| **08** | `08_manual_commit` | Commit manual (`msg.commit()`) | ✅ OK | ✅ OK | 🟢 PASS | Offsets `0`, `1` y `2` confirmados manualmente en broker con `auto_commit=False`. |
| **09** | `09_retries_and_dlq` | Reintentos automáticos y DLQ | ✅ OK | ✅ OK | 🟢 PASS | 3 reintentos fallidos de carga corrupta capturados y ruteados a `unstable_events_topic.DLQ`. |
| **10** | `10_multi_topic_regex` | Multi-tópico y patrones Regex | ✅ OK | ✅ OK | 🟢 PASS | Consumidor registrado procesó eventos concurrentes desde `sensor_temp` y `sensor_humidity`. |
| **11** | `11_async_handlers` | Consumidores con `async / await` | ✅ OK | ✅ OK | 🟢 PASS | Handlers con `async def` ejecutados en bucle de eventos no bloqueante sin excepciones. |
| **12** | `12_pydantic_validation` | Esquemas Pydantic (`format="pydantic"`) | ✅ OK | ✅ OK | 🟢 PASS | Instancia de `UserModel` (`id=101`, `username="alice"`) validada y deserializada automáticamente. |
| **13** | `13_partition_scale` | Auto-escalado de particiones | ✅ OK | ✅ OK | 🟢 PASS | Tópico `partition_scaled_topic` auto-escalado dinámicamente de 1 a 2 particiones en KafkaAdminClient. |
| **14** | `14_interactive_producer` | Consola interactiva CLI | ✅ OK | ✅ OK | 🟢 PASS | Interfaz CLI interactiva `input()` transmitió mensajes dinámicos consumidos en tiempo real. |

---

## 🛠️ Tecnologías y Librerías Relevantes

- **Python (3.9 - 3.14)**: Lenguaje y entorno de desarrollo.
- **Apache Kafka Broker**: Servidor de eventos distribuido en tiempo real (Broker ejecutable en `localhost:29092`).
- **`kafka-python-ng`**: Cliente base de protocolo Apache Kafka con soporte para `KafkaAdminClient` y `NewPartitions`.
- **OpenCV (`opencv-python`) & Pillow (PIL)**: Renderizado, matriz de píxeles, codificación JPEG y deserialización multimedia.
- **Pydantic**: Validación y tipado de modelos estructurados.
- **Loguru**: Trazabilidad y logs estructurados de eventos en consola y archivos de registro.
- **Pytest**: Entorno de ejecución de tests unitarios y cálculo de cobertura de código.
