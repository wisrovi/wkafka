# Suscripción a Múltiples Tópicos y Regex en WKafka 🎯

Este ejemplo muestra cómo suscribir una misma función consumidora a múltiples tópicos (`topic=["topic_a", "topic_b"]`) o expresiones regulares (`pattern="sensor_.*"`) usando **WKafka**.

---

## 🚀 Cómo Ejecutar

1. **Consumidor (Terminal 1):**
   ```bash
   poetry run python examples/10_multi_topic_regex/consumer.py
   ```
2. **Productor (Terminal 2):**
   ```bash
   poetry run python examples/10_multi_topic_regex/producer.py
   ```

---

## 🛠️ Tecnologías y Librerías Relevantes

- **WKafka**: Soporte para suscripción múltiple y coincidencia por patrones Regex.
- **Apache Kafka**: Gestión distribuida de suscripciones a grupos de tópicos.
