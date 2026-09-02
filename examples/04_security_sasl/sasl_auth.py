from wkafka import WKafka

security = {
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "sasl_plain_username": "external-user",
    "sasl_plain_password": "mdL0Q9gKAANuglBV8KaGvPYS6NihQP5u",
}

kafka = WKafka(bootstrap_servers="localhost:30092", dynamic_group_id=True, **security)


@kafka.consumer(topic="secure_topic", format="json")
def on_secure_msg(msg):
    print(f"🔒 Mensaje Seguro: {msg.value}")


print(
    "Consumer SASL configurado. Ejecuta con kafka.run_consumers() teniendo el entorno SASL levantado."
)
