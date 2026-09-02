# Gestión Manual de Offsets (Manual Commit) en WKafka ⚙️

Este ejemplo demuestra cómo lograr garantías de procesamiento **At-Least-Once** en **WKafka** desactivando el commit automático (`auto_commit=False`) y llamando explícitamente a `msg.commit()` tras completar la lógica de negocio.

---

## 🚀 Cómo Ejecutar

1. **Consumidor (Terminal 1):**
   ```bash
   poetry run python examples/08_manual_commit/consumer.py
   ```
2. **Productor (Terminal 2):**
   ```bash
   poetry run python examples/08_manual_commit/producer.py
   ```

---

## 🛠️ Tecnologías y Librerías Relevantes

- **WKafka**: Framework Python con control explícito de commits de offset (`msg.commit()`).
- **Apache Kafka**: Gestión distribuida de offsets por partición y grupo de consumo.
- **Loguru**: Logging de eventos de commit.
