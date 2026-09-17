# The most expensive Kafka outage isn't broker failure — it's nobody noticing the listener has no auth until demo day.

An open `:9092` with no SASL isn't "an internal detail", it's a wire with no handshake sitting between you and anyone who can reach the port. Managed brokers hand you auth config and quietly hope you don't leak tokens around. Today: WKafka turns auth into a single typed declaration, and shows a full SASL_PLAINTEXT round-trip in a runnable example.

> Day 04, WKafka open-source series — decorator-based, MIT, reproducible.

## The config most people paste wrong once

The connoisseur version, both faces — producer *and* consumer behind the same contract:

```python
kafka = WKafka(
    bootstrap_servers="localhost:30092",
    security_protocol="SASL_PLAINTEXT",   # or SASL_SSL when TLS
    sasl_mechanism="PLAIN",               # or SCRAM-SHA-256 / SCRAM-SHA-512
    sasl_plain_username="external-user",
    sasl_plain_password=os.getenv("KAFKA_PASSWORD"),
)
```

Three wins vs scattered config:
1. **One object, resolved and validated once** — typo in the mechanism fails at construction, not at 2am.
2. **Password never in code.** It comes from env, so rotation is a redeploy, not a code change.
3. **The decorator API doesn't change.** Auth is transport; your `@kafka.consumer(...)` handlers stay exactly the same (Day 01 contract intact).

## Honest boundary

WKafka wires SASL PLAIN and SCRAM (SHA-256/512), SASL_PLAINTEXT and SASL_SSL. What it does **not** do is make you type-aware of PKI: TLS cert management remains the platform's job — the wrapper respects it, doesn't reinvent it.

## Try it

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub, `04_security_sasl` runs a full auth cycle over SASL_PLAINTEXT:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*Which Kafka secret did you once leave in a committed file? We've all been there — confess below, it's a safe space.*

#Kafka #Security #SASL #SCRAM #Python #DataEngineering #OpenSource #Wisrovi