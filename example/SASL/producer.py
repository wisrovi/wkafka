from wkafka.controller import Wkafka

brokers = ["192.168.3.89:30092"]
kafka_security_config = {
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "SCRAM-SHA-512",
    "sasl_plain_username": "external-user",
    "sasl_plain_password": "mdL0Q9gKAANuglBV8KaGvPYS6NihQP5u",
}

kf = Wkafka(server=brokers, other_config={**kafka_security_config})

with kf.producer() as kf_producer:
    kf_producer.send(
        topic="sms",
        value={"name": "Juan", "auth": "SCRAM-SHA-512"},
        value_type="json",
        header={"source": "Wkafka-Client"},
    )
