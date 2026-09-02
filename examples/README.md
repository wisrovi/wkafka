# WKafka Examples Suite 📚

Esta suite está diseñada para que domines **WKafka v1.0.0 LTS** paso a paso.

## 🗂️ Estructura de la Carpeta

| Carpeta | Nivel | Concepto |
| :--- | :--- | :--- |
| `01_basic` | 🟢 Fácil | JSON simple (Producer/Consumer separados). |
| `02_advanced` | 🟡 Intermedio | Headers y claves personalizadas. |
| `03_multimedia` | 🟠 Especial | Streaming de Imágenes (NumPy/OpenCV). |
| `04_security_sasl` | 🔴 Pro | Autenticación SASL segura. |
| `05_patterns` | 🔥 Arquitecto | Patrón Microservicios (Worker/Client). |
| `06_images` | 🖼️ Multimedia | Envío y Recepción de Imágenes (PIL / OpenCV / NumPy). |

## 🚀 Guía de Uso

Para cada nivel:
1. Terminal 1 (**Consumer**): `poetry run python examples/XX_folder/consumer.py`
2. Terminal 2 (**Producer**): `poetry run python examples/XX_folder/producer.py`

---

## 🛠️ Tecnologías y Librerías Relevantes

- **WKafka**: Framework Python para Apache Kafka con decoradores y serializadores automáticos.
- **Apache Kafka**: Sistema de transmisión distribuida de mensajes.
- **OpenCV & Pillow (PIL)**: Librerías para procesamiento, codificación y decodificación de imágenes.
- **NumPy**: Manipulación de arreglos numéricos n-dimensionales para datos de imagen.
