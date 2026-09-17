[P] WKafka Day 04 — the SASL auth you stopped copy-pasting wrong. Open source, MIT, reproducible.

Today's hook: Kafka's first security question is "who are you?" and the wrong answer format costs a whole service. SASL over SASL_PLAINTEXT (or SASL_SSL) with PLAIN or SCRAM-SHA-256/512 is a typed one-time declaration in WKafka — not a snippet tattooed into five services.

```python
kafka = WKafka(
    bootstrap_servers="localhost:30092",
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username="external-user",
    sasl_plain_password=os.getenv("KAFKA_PASSWORD"),
)
```

Because auth is transport, the consumer decorator stays exactly the Day-01 contract (loop, key routing, clean shutdown). No passwords in committed code — env, typed once, validated at construction.

Verified, nothing invented:
- SASL_PLAINTEXT and SASL_SSL; PLAIN and SCRAM-SHA-256/512 mechanisms
- Python 3.9 → 3.14, mypy strict, loguru, tox multi-version, MIT
- `04_security_sasl` runs a complete manual SASL auth cycle — reproducible
- Links: PyPI https://pypi.org/project/wkafka · GitHub https://github.com/wisrovi/wkafka · Docs https://wkafka.readthedocs.io

*What's the auth-related Kafka config you've seen hardcoded that made you flinch?*