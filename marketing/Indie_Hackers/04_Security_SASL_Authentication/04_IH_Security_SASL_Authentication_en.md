#### KAFKA_SASL scaffolding, killed with a typed one-time declaration.

Open listener, zero auth, staging → prod copy-paste. The day somebody outside the org can reach :9092, "internal network" stops being an argument. Here's WKafka treating SASL as a typed contract, not a copy-paste ritual (Day 04 — open-source series, MIT, reproducible).

```python
from wkafka import WKafka

kafka = WKafka(
    bootstrap_servers="localhost:30092",
    security_protocol="SASL_PLAINTEXT",      # or SASL_SSL
    sasl_mechanism="SCRAM-SHA-256",          # PLAIN / SCRAM-SHA-512 also
    sasl_plain_username="external-user",
    sasl_plain_password=os.getenv("KAFKA_PASSWORD"),   # never in code
    dynamic_group_id=True,
)
```

Consumer route stays 100% Day-01 — decorator, key-order, clean shutdown. Auth is transport, not handler logic.

Verified boundaries: WKafka wires SASL_PLAINTEXT + SASL_SSL, PLAIN and SCRAM-SHA-256/512, SASL mechanisms on the consumer/producer decorators. It does NOT claim to manage your PKI certs (that stays with the platform/TLS layer) and there's no fabricated "enterprise security" metric — only the runnable `04_security_sasl` example to prove the manual auth cycle.

- PyPI: https://pypi.org/project/wkafka
- GitHub + `04_security_sasl` SASL full round-trip: https://github.com/wisrovi/wkafka
- Docs: https://wkafka.readthedocs.io

*Ever shipped a Kafka password in a committed yaml? We've all been there. Confession corner below.*

#Kafka #SASL #Security #SCRAM #Python #DataEngineering #OpenSource #Wisrovi