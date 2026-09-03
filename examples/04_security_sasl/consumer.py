from wkafka import WKafka

sasl_config = {
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "sasl_plain_username": "external-user",
    "sasl_plain_password": "mdL0Q9gKAANuglBV8KaGvPYS6NihQP5u",
}
kafka = WKafka(
    bootstrap_servers="localhost:30092", dynamic_group_id=True, **sasl_config
)


@kafka.consumer(topic="secure_topic", format="json")
def on_secure_msg(msg):
    print(f"🔒 Recibido en canal seguro: {msg.value}")


if __name__ == "__main__":
    kafka.run_consumers(block=True)
