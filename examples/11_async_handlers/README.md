# Handlers Asíncronos (async/await) en WKafka ⚡

Este ejemplo demuestra el uso de funciones consumidoras **asíncronas** (`async def`) en **WKafka**, permitiendo ejecutar operaciones I/O no bloqueantes (`asyncio`) dentro de las funciones decoradas.

---

## 🚀 Cómo Ejecutar

1. **Consumidor (Terminal 1):**
   ```bash
   poetry run python examples/11_async_handlers/consumer.py
   ```
2. **Productor (Terminal 2):**
   ```bash
   poetry run python examples/11_async_handlers/producer.py
   ```

---

## 🛠️ Tecnologías y Librerías Relevantes

- **WKafka**: Ejecución nativa de handlers asíncronos (`async def`).
- **Python asyncio**: Bucle de eventos asíncronos para I/O no bloqueante.
- **Apache Kafka**: Transporte de mensajes distribuido.
