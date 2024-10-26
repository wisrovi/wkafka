import cv2
from wkafka.controller import Wkafka


kafka_instance = Wkafka(server="192.168.1.60:9092")


with kafka_instance.producer() as kf_producer:
    # Enviar un mensaje
    # kf_producer.send(topic="sms", value={"name": "John"}, key="csv", value_type="json")
    # kf_producer.send(topic="sms", value={"name": "Juan"}, key="csv", value_type="json")

    # kf_producer.send(topic="sms", value="dog.jpg", key="image", value_type="file")

    image = cv2.imread("dog.jpg")

    frame_height, frame_width, _ = image.shape


    kf_producer.send(
        topic="image",
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
            "frame_width": frame_width,
            "frame_height": frame_height
        },
    )
