from wkafka.controller import Wkafka
from loguru import logger

brokers = ["192.168.3.89:30092"]
kafka_security_config = {
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "SCRAM-SHA-512",
    "sasl_plain_username": "external-user",
    "sasl_plain_password": "mdL0Q9gKAANuglBV8KaGvPYS6NihQP5u",
}

kf = Wkafka(server=brokers, name="consumer_service")


@kf.consumer(
    topic="sms",
    value_type="json",
    group_id="sms", 
    other_config={
        **kafka_security_config,
        # "group_id": None,
    },
)
def process_message(data):
    logger.info(f"Key: {data.key} | Offset: {data.offset}")
    logger.info(f"Payload: {data.value}")


kf.run_consumers()
