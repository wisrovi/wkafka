
from wkafka import Wkafka, Consumer_data


kafka_instance = Wkafka(server="192.168.1.137:9092", name="read_respond")


@kafka_instance.consumer(
    topic="read_respond",
    value_type="json",
    key="clave1",
)
def process_message(data: Consumer_data):
    header = data.header
    value = data.value
    print(f"Mensaje recibido: {value}")


# @kafka_instance.consumer(
#     topic="read_respond",
#     value_type="image",
#     key="clave1",
# )
# def stream_video(data: Consumer_data):
#     header = data.header
#     value = data.value
#     print(f"Mensaje recibido: {value}")


if __name__ == "__main__":
    kafka_instance.run_consumers()
