# Envío y Recepción de Archivos con WKafka 📁

Este ejemplo demuestra cómo **enviar (producir)** y **recibir (consumir)** archivos arbitrarios (documentos PDF, imágenes, archivos comprimidos zip o archivos de texto) utilizando el serializador nativo `format="file"` de **WKafka v1.0.0 LTS**.

---

## 📁 Archivos Incluidos

- `producer.py`: Lee archivos locales y transmite sus bytes utilizando `format="file"` junto con metadatos en headers (nombre del archivo, tipo de contenido).
- `consumer.py`: Recibe los mensajes con `format="file"` y guarda los bytes recibidos en disco conservando el nombre original.
- `file_streaming.py`: Ejemplo completo y autónomo ejecutable que transfiere archivos entre productor y consumidor en paralelo.

---

## 🚀 Cómo Ejecutar los Ejemplos

### Opción 1: Consumidor y Productor Separados

1. **Inicia el Consumidor (Terminal 1):**
   ```bash
   poetry run python examples/07_files/consumer.py
   ```
2. **Inicia el Productor (Terminal 2):**
   ```bash
   poetry run python examples/07_files/producer.py
   ```

### Opción 2: Pipeline Integrado

```bash
poetry run python examples/07_files/file_streaming.py
```

---

## 🛠️ Tecnologías y Librerías Relevantes

- **WKafka**: Framework Python para Apache Kafka con soporte para transmisión nativa de bytes de archivos.
- **Apache Kafka**: Broker distribuido para transporte eficiente de streams binarios de datos.
- **Python I/O (os/open)**: Manejo nativo de archivos y flujos de lectura/escritura en sistema de archivos local.
