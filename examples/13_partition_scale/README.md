# 🚀 Partition Auto-Scaling Example (`13_partition_scale`)

Este módulo de ejemplo demuestra cómo lograr **paralelismo real** en Apache Kafka aumentando dinámicamente el número de particiones del tópico mediante `kafka.run_consumers(partition_scale=True)`.

---

### 📖 Descripción

En Apache Kafka, la cantidad de consumidores paralelos en un mismo grupo de consumo (`group_id`) está limitada por el número de **particiones** del tópico. Si un tópico solo tiene 1 partición y ejecutas 3 consumidores, únicamente 1 consumidor podrá procesar mensajes mientras los otros 2 permanecen inactivos.

Al activar `partition_scale=True`:
1. `WKafka` cuenta los trabajadores/consumidores registrados para el tópico.
2. Utiliza `KafkaAdminClient` para verificar cuántas particiones existen en el clúster.
3. Si la cantidad de particiones es inferior al número de consumidores, **escala las particiones del tópico automáticamente** en tiempo de ejecución.

---

### 🏃 Cómo Ejecutarlo

1. **Iniciar el Consumidor con Auto-Escalado**:
   ```bash
   python consumer.py
   ```

2. **Enviar Mensajes desde el Productor**:
   ```bash
   python producer.py
   ```

---

## 🛠️ Tecnologías y Librerías Relevantes

- **Python (3.9+)**: Entorno de ejecución principal.
- **kafka-python-ng (`kafka.admin.KafkaAdminClient`)**: Cliente de administración Kafka para modificación de metadatos y particiones (`NewPartitions`).
- **WKafka**: Orquestador con soporte de auto-escalado dinámico por tópico.
- **Apache Kafka Broker**: Servidor de mensajería distribuido.
