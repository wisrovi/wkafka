# Envío y Recepción de Imágenes con WKafka 🖼️

Este ejemplo demuestra cómo realizar el **envío (producir)** y la **recepción (consumir)** de imágenes a través de **Apache Kafka** utilizando **WKafka v1.0.0 LTS**.

WKafka simplifica el proceso de transporte de multimedia mediante su serializador nativo `format="image"`, convirtiendo imágenes (tanto arreglos de NumPy de OpenCV como objetos PIL Image) en bytes optimizados (JPEG) antes de ser transmitidos al broker.

---

## 📁 Archivos Incluidos

- `producer.py`: Demuestra cómo enviar/producir imágenes desde código Python a un tópico Kafka usando imágenes de OpenCV (`np.ndarray`) y PIL (`PIL.Image`).
- `consumer.py`: Demuestra cómo recibir/consumir imágenes desde Kafka usando el decorador `@kafka.consumer(topic=..., format="image")`, decodificándolas automáticamente en imágenes manipulables.
- `image_streaming.py`: Un script completo ejecutable que inicia el consumidor en segundo plano y produce una ráfaga de imágenes en tiempo real.

---

## 🚀 Cómo Ejecutar los Ejemplos

### Opción 1: Consumidor y Productor Separados

1. **Inicia el Consumidor (Terminal 1):**
   ```bash
   poetry run python wkafka/examples/06_images/consumer.py
   ```
   *El consumidor quedará a la espera de mensajes de imagen entrantes.*

2. **Inicia el Productor (Terminal 2):**
   ```bash
   poetry run python wkafka/examples/06_images/producer.py
   ```
   *El productor enviará las imágenes al tópico y el consumidor las recibirá y guardará en disco.*

### Opción 2: Pipeline Integrado en un Solo Script

Para probar el flujo completo de envío y recepción de forma autónoma:

```bash
poetry run python wkafka/examples/06_images/image_streaming.py
```

---

## 🛠️ Tecnologías y Librerías Relevantes

- **WKafka**: Framework Python para Apache Kafka que proporciona una API basada en decoradores y serializadores de datos nativos.
- **OpenCV (`opencv-python`)**: Librería de visión por computador utilizada para decodificar, codificar (compresión JPEG) y manipular arreglos de píxeles (`numpy.ndarray`).
- **Pillow (PIL)**: Librería de procesamiento de imágenes en Python usada para crear y transformar objetos de imagen.
- **NumPy**: Biblioteca para computación numérica que maneja la representación matricial multidimensional de los fotogramas e imágenes.
- **Apache Kafka**: Plataforma distribuida de procesamiento y transmisión de eventos usada como broker para el transporte de mensajes.
