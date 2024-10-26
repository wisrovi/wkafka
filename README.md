# Project Title

Wkafka: Simplificando la Integración de Kafka con Decoradores en Python

## Introduction

Wkafka es una clase que facilita la gestión de productores y consumidores de Kafka usando un enfoque basado en decoradores para procesar mensajes de manera eficiente. Está diseñada para simplificar la integración con Apache Kafka, proporcionando funciones clave para consumir, producir y procesar mensajes en un entorno distribuido.

## Características

- Enfoque basado en decoradores: Define consumidores de Kafka de manera sencilla utilizando decoradores en funciones Python.
- Soporte para diferentes tipos de datos: Los consumidores y productores admiten JSON, archivos e imágenes.
- Manejo de mensajes en paralelo: Los consumidores se ejecutan en hilos separados, lo que permite procesar múltiples mensajes en paralelo.
- Funciona como contexto de productor: Permite usar el productor de Kafka dentro de un bloque with, asegurando que las conexiones se cierren correctamente.
- Fácil deserialización de mensajes: Admite la conversión automática de los datos recibidos a JSON o imágenes.
- Compatibilidad con Kafka: Configuración simplificada para Kafka Consumers y Producers.

## Requerimientos

- Python 3.7 o superior

  - Dependencias:

    - kafka-python
    - opencv-python
    - numpy

## Instalación

1. con pip

```bash
  pip install wkafka
```

2. desde la fuente clona el repositorio:

```bash
  git clone https://github.com/wisrovi/wkafka.git
  cd wkafka
  pip install -e .
```

## Cómo iniciar

Configuración básica

Inicia un servidor Kafka local o remoto y configura la clase Wkafka con la dirección del servidor.

```python
from wkafka.controller import Wkafka

kafka_instance = Wkafka(server="localhost:9092")
```

Puedes usar localhost:9092 para Kafka local o una dirección remota para un servidor externo.

sino tienes un servidor de kafka instalado, puedes usar el docker compose en la carpeta wkafka/enviroment/docker-compose.yaml para instalar un servidor de kafka, simplemente ejecutando:

```bash
  cd enviroment
  make start
  docker-compose up -d --build
```

## Uso del Consumer

Para consumir mensajes de Kafka, usa el decorador @kafka_instance.consumer sobre la función que deseas ejecutar cuando se recibe un mensaje.

```python
@kafka_instance.consumer(topic="mi_tema", value_type="json")
def process_message(data):
    print(f"Mensaje recibido: {data.value}, con clave: {data.key}")
```

Luego, inicia los consumidores:

```python
kafka_instance.run_consumers()
```

El consumidor se ejecuta en un hilo separado y deserializa automáticamente los mensajes de Kafka.

## Producer use

Para enviar mensajes a un topic de Kafka, usa el productor de la clase Wkafka en un contexto with o fuera de él.

```python
with Wkafka(server="localhost:9092").producer() as producer:
    producer.send(
        topic="mi_tema",
        value={"mensaje": "Hola Kafka!"},
        key="clave1",
        value_type="json"
    )
```

## Configuraciones Adicionales

Configuración de Consumer: Puedes configurar el deserializador, el grupo de consumidores, los filtros de clave, otros parametros, entre otros.

Ejemplo con configuración adicional:

```python
@kafka_instance.consumer(
    topic="mi_tema",
    group_id="grupo_especifico",
    value_type="image",
    other_config=dict(
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    ),
)
def process_image(data):
    # Procesar imagen recibida
    print(f"Imagen procesada: {data.key}")

```

Configuración del Producer: Puedes enviar diferentes tipos de datos como JSON, archivos o imágenes.

## Manejo de Errores

Si un consumidor o productor tiene problemas, Wkafka manejará las excepciones de manera automática, imprimiendo el error en la consola y continuando la ejecución.

```python
try:
    kafka_instance.producer().send(
        topic="mi_tema",
        value={"data": "Mensaje importante"},
        value_type="json"
    )
except Exception as e:
    print(f"Error al enviar mensaje: {e}")
```

## Consumidores en Paralelo

Todos los consumidores creados con @kafka_instance.consumer se ejecutan en hilos separados, permitiendo la ejecución paralela de múltiples consumidores.

```python
# Se ejecutan en paralelo
kafka_instance.run_consumers()
```

## Examples

Casos de uso detallados con código de ejemplo.

### 1. Procesamiento de Mensajes JSON

```python
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="basic")

@kafka_client.consumer(topic="json_topic", value_type="json")
def process_json(data):
    print("Processing JSON data:", data.value)

kafka_client.run_consumers()
```

Este ejemplo muestra cómo consumir mensajes en formato JSON y procesarlos. Es útil para manejar datos estructurados en aplicaciones de análisis.

### 2. Filtrado de Mensajes por Clave

```python
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="key")

@kafka_client.consumer(
    topic="filtered_topic",
    key="important_key",
    value_type="json",)
def process_filtered(data):
    print(f"Received filtered message with key {data.key}: {data.value}")

kafka_client.run_consumers()

```

Se filtran los mensajes por una clave específica (key). Solo los mensajes que coincidan serán procesados.

### 3. Recepción y Visualización de Imágenes

```python
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="image_show")

@kafka_client.consumer(topic="image_topic", value_type="image")
def display_image(data):
    cv2.imshow("Received Image", data.value)
    cv2.waitKey(0)
    cv2.destroyAllWindows()

kafka_client.run_consumers()
```

Consume imágenes desde Kafka y las muestra usando OpenCV, ideal para aplicaciones de visión por computadora.

### 4. Almacenamiento de Mensajes en un Archivo

```python
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="save_msgs_in_file")

@kafka_client.consumer(topic="log_topic", value_type="json")
def save_to_file(data):
    with open("logs.txt", "a") as file:
        file.write(f"{data.value}\n")

kafka_client.run_consumers()
```

Guarda mensajes recibidos en un archivo de texto, útil para mantener registros o auditorías.

### 5. Streaming de Video

1. Receptor Video

```python
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="video_show")

@kafka_instance.consumer(topic="video_topic", value_type="image")
def stream_video(data):
    DREAM_WIDTH = 600

    im0 = data.value
    header = data.header

    frame_width = header.get("frame_width")
    frame_height = header.get("frame_height")

    new_size = DREAM_WIDTH / frame_width
    im0 = cv2.resize(im0, (int(frame_width * new_size), int(frame_height * new_size)))

    # Process each video frame as an image
    cv2.imshow("Video Received", im0)
    if cv2.waitKey(1) & 0xFF == ord("q"):
        return

    if header.get("frame_id") == (header.get("total_frames") - 1):
        cv2.destroyAllWindows()

kafka_client.run_consumers()
```

Recibe y muestra un flujo de video en tiempo real, tratando cada mensaje como un fotograma de video.

2. Transmisor video

```python
import cv2
from tqdm import tqdm
from wkafka.controller import Wkafka


kafka_instance = Wkafka(server="localhost:9092")


with kafka_instance.producer() as producer:

    video_capture = cv2.VideoCapture(f"../../data/videos/statistics/0007.mp4")
    assert video_capture.isOpened(), "Error reading video file"

    # read metadata of video
    frame_width, frame_height, total_frames, fps = [
        int(video_capture.get(value))
        for value in [
            cv2.CAP_PROP_FRAME_WIDTH,
            cv2.CAP_PROP_FRAME_HEIGHT,
            cv2.CAP_PROP_FRAME_COUNT,
            cv2.CAP_PROP_FPS,
        ]
    ]

    for frame_id in tqdm(
        range(total_frames), desc="Processing video frames", unit="frames"
    ):
        ret, im0 = video_capture.read()
        if not ret:
            break

        cv2.putText(im0, f"frame: {frame_id}", (100, 100), cv2.FONT_HERSHEY_SIMPLEX, 2, (255, 0, 255), 2,
        )

        producer.async_send(
            topic="video_topic",
            value=im0,
            key="image",
            value_type="image",
            header={
                "frame_width": frame_width,
                "frame_height": frame_height,
                "total_frames": total_frames,
                "fps": fps,
                "frame_id": frame_id,
            },
        )

        DREAM_WIDTH = 600
        new_size = DREAM_WIDTH / frame_width
        im0 = cv2.resize(
            im0, (int(frame_width * new_size), int(frame_height * new_size))
        )

        cv2.imshow("Video Send", im0)
        if cv2.waitKey(1) & 0xFF == ord("q"):
            break
    cv2.destroyAllWindows()
```

### 6. Envío de Alertas

```python
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="alerts_received")

@kafka_client.consumer(topic="sensor_topic", value_type="json")
def alert_on_high_temp(data):
    if data.value.get("temperature") > 30:
        print("High temperature alert:", data.value)

kafka_client.run_consumers()
```

Procesa mensajes de sensores y genera alertas si la temperatura excede un umbral, útil en monitoreo de sistemas.

### 7. Integración con Sistemas de Monitoreo

```python
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="basic_data_received")

@kafka_client.consumer(topic="metrics_topic", value_type="json")
def process_metrics(data):
    # Forward metrics to a monitoring system
    print("Metrics:", data.value)

kafka_client.run_consumers()
```

Consume métricas y las envía a sistemas de monitoreo para la visualización en dashboards.

### 8. Procesamiento de Logs

```python
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="varios")

@kafka_client.consumer(topic="service_logs", value_type="json")
def process_logs(data):
    print("Service Log:", data.value)

kafka_client.run_consumers()
```

Recibe y procesa logs de servicios, ideal para auditoría y depuración.

- Nota: otros usos pueden ser:
  - Ejecución de Microservicios Asíncronos
  - Procesamiento de Datos Financieros
  - Recolección de Métricas de Aplicaciones
  - Gestión de Transacciones Bancarias
  - Sincronización de Configuraciones
  - Control de Dispositivos Remotos
  - Envío de Datos a Sistemas de Machine Learning
  - Implementación de Workflows Distribuidos
  - Recolección de Métricas de Aplicaciones

### 9. Carga de Datos en Tiempo Real a Bases de Datos

```python
import sqlite3
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="insert_in_DB")

conn = sqlite3.connect('database.db')
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS metrics (id INTEGER PRIMARY KEY, data TEXT)''')

@kafka_client.consumer(topic="db_topic", value_type="json")
def save_to_db(data):
    cursor.execute("INSERT INTO metrics (data) VALUES (?)", (str(data.value),))
    conn.commit()

kafka_client.run_consumers()
```

Inserta los datos recibidos en una base de datos en tiempo real para almacenamiento y análisis.

### 10. Simulación de Sensores IoT

```python
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="send_IoT_data")

with kafka_client.producer() as producer:
    for i in range(10):
        producer.send(
            topic="sensor_topic",
            value={"sensor_id": i, "temperature": random.randint(20, 40)},
            value_type="json",
            verbose=True
        )
```

Simula datos de sensores IoT y los envía a Kafka, útil para pruebas antes del despliegue real.

### 11. Envío de Archivos a Través de Kafka

```python
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="file_send")

with kafka_client.producer() as producer:
    producer.send(
        topic="file_topic",
        value="path/to/file.txt",  # Ruta del archivo
        value_type="file",
        verbose=True
    )
```

Envía un archivo a través de Kafka para ser procesado por otro sistema, como análisis de logs.

### 12. Procesamiento de Datos Financieros

```python
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="financial_predict")

@kafka_client.consumer(topic="finance_topic", value_type="json")
def analyze_financial_data(data):
    print("Processing financial data:", data.value)

kafka_client.run_consumers()
```

Consume datos financieros para su análisis en tiempo real, como la detección de anomalías.

### 13. Distribución de Modelos de IA

```python
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="update_models_in_production")

with kafka_client.producer() as producer:
    with open("model.pt", "rb") as model_file:
        producer.send(
            topic="model_distribution",
            value=model_file.read(),
            value_type="file",
            verbose=True
        )
```

Envía un modelo de IA entrenado para su despliegue en nodos de inferencia distribuidos.

### 14. Preprocesamiento de Datos en Tiempo Real para Modelos ML

```python
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="preprocess")

@kafka_client.consumer(topic="raw_data", value_type="json")
def preprocess_data(data):
    # Preprocess data (e.g., normalization, feature extraction)
    processed_data = {
        "feature1": data.value["sensor_value"] / 100,
        "feature2": len(data.value["text"])
    }
    print("Preprocessed Data:", processed_data)

kafka_client.run_consumers()
```

Este ejemplo muestra cómo preprocesar datos en tiempo real antes de enviarlos a un modelo de ML, realizando tareas como normalización y extracción de características.

### 15. Inferencia en Tiempo Real con Modelos de IA

```python
from sklearn.externals import joblib
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="features_inference")

# Cargar el modelo previamente entrenado
model = joblib.load("model.pkl")

@kafka_client.consumer(topic="inference_data", value_type="json")
def run_inference(data):
    # Realizar la predicción con el modelo cargado
    prediction = model.predict([data.value["features"]])
    print(f"Prediction: {prediction}")

kafka_client.run_consumers()
```

Consume datos de inferencia y utiliza un modelo de IA previamente entrenado para predecir resultados en tiempo real, útil en sistemas de recomendaciones o detección de anomalías.

### 16. Inferencia en Tiempo Real con Modelos de IA Usando Imágenes

```sh
pip install ultralytics
```

```python
from ultralytics import YOLO
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="image_inference")

# Cargar el modelo YOLOv5 preentrenado
model = YOLO('yolov8s.pt')

@kafka_client.consumer(topic="image_topic", value_type="image")
def image_inference(data):
    image = data.value

    results = model(image)

    predictions = results.pandas().xyxy[0]  # DataFrame con las predicciones

    annotated_frame = results[0].plot()

    # Muestra la imagen con las detecciones
    cv2.imshow("Image Inference", annotated_frame)
    cv2.waitKey(1)  # Refresca la ventana de visualización

# Inicia el consumidor de Kafka
kafka_client.run_consumers()
```

Este ejemplo muestra cómo consumir mensajes en formato JSON y procesarlos. Es útil para manejar datos estructurados en aplicaciones de análisis.

### 17. Monitoreo de Rendimiento de Modelos en Producción

```python
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="metrics_monitor")

@kafka_client.consumer(topic="model_metrics", value_type="json")
def monitor_model_performance(data):
    # Monitoriza las métricas de rendimiento del modelo
    print(f"Model accuracy: {data.value['accuracy']} at {data.value['timestamp']}")

kafka_client.run_consumers()
```

Permite monitorear el rendimiento del modelo en producción, recibiendo métricas como precisión o tiempos de inferencia, lo cual es crucial para mantener la calidad del modelo.

### 18. Entrenamiento de Modelos de IA Distribuidos

```sh
pip install ultralytics
```

```python
from ultralytics import YOLO
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="model_train")

model = YOLO('yolov8s.pt')  # old_model

@kafka_client.consumer(topic="training_data", value_type="json")
def distributed_training(data):

    data_yaml_path = data.value['data_yaml_path']  # path (string)
    dataset_hiperparemeters = data.value['dataset_hiperparemeters'] # json

    results = model.train(data_yaml_path, **dataset_hiperparemeters)

    # Agregar datos al set de entrenamiento
    print("Training with data:", data.value)

kafka_client.run_consumers()
```

Facilita el entrenamiento de modelos de manera distribuida recibiendo datos de diferentes fuentes para actualizar el modelo continuamente.

### 19. Generación de Características en Tiempo Real

```python
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="RT_features")

@kafka_client.consumer(
    topic="feature_engineering", 
    value_type="json",
)
def generate_features(data):
    # Generar nuevas características a partir de los datos entrantes
    features = {
        "feature1": data.value["value1"] ** 2,
        "feature2": data.value["value2"] * 0.5
    }
    print("Generated features:", features)

kafka_client.run_consumers()
```

Este ejemplo genera nuevas características de los datos entrantes en tiempo real, útil para pipelines de datos en modelos de ML.

### 20. Análisis de Sentimientos en Tiempo Real

```python
from textblob import TextBlob
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="TextBlob")

@kafka_client.consumer(topic="tweets", value_type="json")
def sentiment_analysis(data):
    # Realizar análisis de sentimiento sobre texto recibido
    sentiment = TextBlob(data.value["text"]).sentiment
    print(f"Sentiment Analysis: {sentiment.polarity}, {sentiment.subjectivity}")

kafka_client.run_consumers()
```

Realiza un análisis de sentimientos sobre textos recibidos en tiempo real, útil para monitorear redes sociales o feedback de usuarios.

### 21. Despliegue de Modelos de NLP (Procesamiento de Lenguaje Natural)

```python
from transformers import pipeline
from wkafka.controller import Wkafka

kafka_client = Wkafka(server="localhost:9092", name="TextBlob")

# Cargar un modelo de procesamiento de texto
nlp_model = pipeline("text-classification")

@kafka_client.consumer(topic="nlp_topic", value_type="json")
def nlp_processing(data):
    # Procesar texto con el modelo NLP
    result = nlp_model(data.value["text"])
    print(f"NLP Result: {result}")

kafka_client.run_consumers()
```

Este ejemplo despliega un modelo de NLP para procesar texto en tiempo real, útil en clasificación de correos, chatbots o análisis de contenido.

### 22. Orquestación de Modelos en Inferencias Complejas

```python
from wkafka.controller import Wkafka



model_1 = ...
model_2 = ...
model_3 = ...


kafka_client = Wkafka(server="localhost:9092", name="TextBlob")

@kafka_client.consumer(
    topic="complex_inference", 
    value_type="json",
)
def orchestrate_models(data):
    # Orquestar múltiples modelos para una inferencia compleja
    step1_result = model_1.predict([data.value["step1_input"]])
    step2_result = model_2.predict([step1_result])
    final_result = model_3.predict([step2_result])
    print(f"Final Inference Result: {final_result}")


kafka_client.run_consumers()
```

Orquesta una secuencia de modelos en un pipeline de inferencia complejo, combinando resultados de diferentes modelos para obtener una respuesta final.

### 23. Captura datos, Reentrenamiento e Inferencia del Modelo

```python
from sklearn.ensemble import RandomForestClassifier
import pickle
import json
from wkafka.controller import Wkafka



class ModelUpdater(Wkafka):
    def __init__(self, server: str, name: str, model_path: str):
        super().__init__(server, name)
        self.model_path = model_path
        self.new_data = []

    @Wkafka.consumer(topic="data_topic", value_type="json")
    def capture_data(self, data: dict):
        self.new_data.append(data)
        if len(self.new_data) >= 100:  # Ajusta el tamaño del lote si es necesario
            self.retrain_model()

    def retrain_model(self):
        # Prepara datos para el entrenamiento
        X = [d['features'] for d in self.new_data]
        y = [d['label'] for d in self.new_data]

        # Entrena el modelo
        model = RandomForestClassifier()
        model.fit(X, y)

        # Guarda el modelo
        with open(self.model_path, 'wb') as f:
            pickle.dump(model, f)

        self.new_data = []


class ModelInferencer(Wkafka):
    def __init__(self, server: str, name: str, model_path: str):
        super().__init__(server, name)
        self.model_path = model_path
        with open(model_path, 'rb') as f:
            self.model = pickle.load(f)
        self.producer = KafkaProducer(
            bootstrap_servers=server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    @Wkafka.consumer(topic="data_topic", value_type="json")
    def predict_and_send(self, data: dict):
        features = [data['features']]
        prediction = self.model.predict(features)[0]
        result = {'id': data['id'], 'prediction': prediction}

        with self.producer() as producer:
            producer.send(topic='prediction_topic', value=result, value_type="json")


model_update = ModelUpdater(server="localhost:9092", name="TextBlob", model_path="path/to/model.ptl")
model_update.run_consumers()

model_predict = ModelUpdater(server="localhost:9092", name="TextBlob", model_path="path/to/model.ptl")
model_predict.run_consumers()
```

Explicación

- Captura y Reentrenamiento (ModelUpdater):
  Usa un consumidor Kafka para recibir datos desde data_topic.
  Los datos se acumulan y, cuando hay suficientes (100 en este caso), se reentrena el modelo y se guarda.

- Inferencia (ModelInferencer):
  Usa un consumidor Kafka para recibir datos y cargar el modelo actualizado.
  Realiza predicciones con el modelo y envía los resultados al tópico prediction_topic.

- Ejecución:
  Se crean instancias de ModelUpdater y ModelInferencer y se ejecutan para manejar datos y realizar inferencias.

Este enfoque simplificado permite la actualización y el uso de modelos en producción de manera eficiente y con un mínimo de código.

## Buenas Prácticas

- Definir temas y grupos de consumidores claramente para evitar duplicación de procesamiento.
- Usar el bloque with para productores cuando sea posible, para asegurar que los recursos se liberen apropiadamente.
- Implementar manejo de errores y validación de datos en las funciones que consumen mensajes.

## Solución de Problemas

- Problema: No se reciben mensajes en el consumidor.

  - Solución: Asegúrate de que el topic existe y de que el group_id del consumidor es correcto.

- Problema: El productor no puede enviar mensajes.

  - Solución: Verifica que Kafka está corriendo y que has especificado correctamente el servidor Kafka.

## Contribuyendo

1. Haz un fork del proyecto.
2. Crea una rama con tu nueva funcionalidad: git checkout -b mi-nueva-funcionalidad.
3. Haz commit de tus cambios: git commit -m 'Agregar nueva funcionalidad'.
4. Haz push de la rama: git push origin mi-nueva-funcionalidad.
5. Crea un Pull Request.

## License

Este proyecto está bajo la licencia MIT, lo que significa que puedes usarlo, modificarlo y distribuirlo libremente, siempre que mantengas la atribución al autor original. Ver el archivo LICENSE para más detalles.

[MIT](https://choosealicense.com/licenses/mit/)
