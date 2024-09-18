import cv2
from wkafka.controller import Wkafka


kafka_instance = Wkafka(server="192.168.1.60:9092")


with kafka_instance.producer() as producer:
    producer.send(
        topic="mi_tema",
        value={"mensaje": "Hola Kafka!"},
        key="clave1",
        value_type="json"
    )


