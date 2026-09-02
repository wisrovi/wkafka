# Reintentos Automáticos y Dead Letter Queue (DLQ) en WKafka 🛡️

Este ejemplo demuestra el sistema integrado de **reintentos automáticos** (`max_retries`, `retry_delay`) y enrutamiento hacia una **Dead Letter Queue (DLQ)** en **WKafka**.

---

## 🚀 Cómo Ejecutar

1. **Consumidor y Monitor DLQ (Terminal 1):**
   ```bash
   poetry run python examples/09_retries_and_dlq/consumer.py
   ```
2. **Productor (Terminal 2):**
   ```bash
   poetry run python examples/09_retries_and_dlq/producer.py
   ```

---

## 🛠️ Tecnologías y Librerías Relevantes

- **WKafka**: Reintentos automáticos con backoff exponencial y derivación a tópicos DLQ.
- **Apache Kafka**: Broker de transmisión para aislamiento de mensajes corruptos.
- **Loguru**: Formateo de mensajes de reintento y log de excepciones.
