from wkafka.controller import Wkafka


kafka_instance = Wkafka(server="192.168.1.137:9092")


with kafka_instance.producer() as producer:
    producer.send(
        topic="sms",
        value={"mensaje": "Hola Kafka!"},
        key="clave1",
        value_type="json",
        header={"response_to": "send_to_docker", "id_db": "abcd_1234"},
    )
