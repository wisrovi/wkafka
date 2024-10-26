import io
import random
from PIL import Image
import cv2
from wkafka.controller.wkafka import Wkafka

# Ejemplo de uso
kf = Wkafka(server="localhost:9092")
kf = Wkafka(server="192.168.1.137:9092")


# @kf.consumer(
#     topic="sms",
#     # group_id=f"A{random.randint(1, 100)}",
#     # key="csv",
#     value_type="json",
#     other_config=dict(
#         auto_offset_reset="earliest",
#         enable_auto_commit=False,
#     ),
# )
# def process_message(message):
#     print(f"[sms] Message received: {message.value}, key: {message.key}")


@kf.consumer(
    topic="send_to_docker",
    # group_id=f"A{random.randint(1, 100)}",
    # key="csv",
    value_type="json",
    other_config=dict(
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    ),
)
def process_message(data):

    header = data.header
    value = data.value
    key = data.key

    print(f"[send_to_docker] Message received: {value}, key: {key}, header: {header}")


# @kf.consumer(
#     topic="sms",
#     group_id=f"B{random.randint(1, 100)}",
#     key="csv",
# )
# def process_message2(message):
#     print(f"Message received 2: {message.value}, key: {message.key}")


# @kf.consumer(
#     topic="sms",
#     group_id=f"C{random.randint(1, 100)}",
#     key="image",
#     value_type="image",
#     other_config=dict(
#         auto_offset_reset="earliest",
#         enable_auto_commit=False,
#     ),
# )
# def process_message3(message):

#     cv2.imwrite("demo.jpg", message.value)

#     print(
#         f"Message received 3: {message.value.shape}, key: {message.key}, headers: {message.header}"
#     )


# @kf.consumer(
#     topic="sms",
#     group_id=f"D{random.randint(1, 100)}",
#     key="image",
#     value_type="file",
# )
# def process_message3(message):
#     print(f"Message received 4: {message.value}, key: {message.key}")

#     with open("demo.jpg", "wb") as file:
#         file.write(message.value)


# Iniciar el consumo
kf.run_consumers()
