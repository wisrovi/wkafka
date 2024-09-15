import cv2
from wkafka.controller.wkafka import Wkafka

# Instancia de CustomKafkaProducer
kf = Wkafka(server="localhost:9092")

# Ejemplo de uso con 'with' y el método producer()
with kf.producer() as kf_producer:
    # Enviar un mensaje
    # kf_producer.send(topic="sms", value={"name": "John"}, key="csv", value_type="json")
    # kf_producer.send(topic="sms", value={"name": "Juan"}, key="csv", value_type="json")

    # kf_producer.send(topic="sms", value="dog.jpg", key="image", value_type="file")

    image = cv2.imread("dog.jpg")
    kf_producer.send(
        topic="sms",
        value=image,
        key="image",
        value_type="image",
        headers={
            "status": True,
            "value": 12345,
            "correlation_id": "12345",
            "source": "service_A",
            "destination": "service_B",
            "content_type": "image/jpeg",
        },
    )
