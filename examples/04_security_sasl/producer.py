from wkafka import WKafka

sasl_config = {
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "sasl_plain_username": "external-user",
    "sasl_plain_password": "mdL0Q9gKAANuglBV8KaGvPYS6NihQP5u",
}
kafka = WKafka(bootstrap_servers="localhost:30092", **sasl_config)
if __name__ == "__main__":
    with kafka.producer() as p:
        p.send("secure_topic", value={"auth": "success"}, format="json")
        print("🚀 Mensaje seguro enviado.")
