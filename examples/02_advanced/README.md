# Headers y Claves Personalizadas en WKafka 🔑

Este ejemplo muestra cómo enviar y recibir metadatos adicionales (**Headers**) y claves de particionamiento (**Keys**) en mensajes Kafka usando **WKafka**.

---

## 🚀 Cómo Ejecutar

1. **Terminal 1 (Consumidor):**
   ```bash
   poetry run python examples/02_advanced/consumer.py
   ```
2. **Terminal 2 (Productor):**
   ```bash
   poetry run python examples/02_advanced/producer.py
   ```

---

## 🛠️ Tecnologías y Librerías Relevantes

- **WKafka**: Orquestador Python para comunicación distribuida sobre Kafka.
- **Apache Kafka**: Sistema de mensajería con soporte nativo para headers de metadatos y claves.
- **Loguru**: Formateo estructurado de logs y trazabilidad.
