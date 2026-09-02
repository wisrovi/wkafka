# Validación de Esquemas con Pydantic en WKafka 🛡️

Este ejemplo muestra cómo utilizar el serializador nativo `format="pydantic"` de **WKafka** para enviar modelos de Pydantic y deserializarlos con validación estricta de esquemas (`model=UserProfile`) en el consumidor.

---

## 🚀 Cómo Ejecutar

1. **Consumidor (Terminal 1):**
   ```bash
   poetry run python examples/12_pydantic_validation/consumer.py
   ```
2. **Productor (Terminal 2):**
   ```bash
   poetry run python examples/12_pydantic_validation/producer.py
   ```

---

## 🛠️ Tecnologías y Librerías Relevantes

- **WKafka**: Serializador nativo `format="pydantic"` y paso del parámetro `model` al consumidor.
- **Pydantic**: Validación y tipado estricto de estructuras de datos en Python.
- **Apache Kafka**: Transporte de mensajes distribuido.
