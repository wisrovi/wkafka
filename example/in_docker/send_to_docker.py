import cv2
from wkafka import Wkafka


kafka_instance = Wkafka(server="192.168.1.137:9092")


with kafka_instance.producer() as producer:
    producer.send(
        topic="read_respond",
        value={"mensaje": "Hola Kafka!"},
        key="clave1",
        value_type="json",
        header={"response_to": "send_to_docker", "id_db": "abcd_1234"},
    )


# image = cv2.imread("dog.jpg")
# frame_height, frame_width, _ = image.shape

# with kafka_instance.producer() as producer:
#     producer.send(
#         topic="read_respond",
#         value=image,
#         key="clave1",
#         value_type="image",
#         header={"response_to": "send_to_docker", "id_db": "abcd_1234"},
#     )
