# Your Kafka is a network service. Sometimes that's the scariest thing you'll read today.

An open listener on `:9092` with no auth is not "an internal detail" — it's a door with no key between you and anyone who can reach the port. Teams on managed clusters hand you a SASL config and quietly hope you wire it right. Here's WKafka treating auth as a *declared contract*, not a mystery.

> Day 04 of the WKafka open-source series — decorator-based, MIT, reproducible.

## The config that used to be a gnarly blob

Every broker hands you some flavor of this. On the consumer side it's even heavier — you carry it everywhere:

```python
from wkafka import WKafka

kafka = WKafka(
    security_protocol="SASL_PLAINTEXT",   # or SASL_SSL
    sasl_mechanism="PLAIN",               # or SCRAM-SHA-256/512
    sasl_plain_username="external-user",
    sasl_plain_password=os.environ["KAFKA_PASSWORD"],
)
```

A cluster contract in one object, resolved once, typed, validated at construction — not scattered tokens floating through service code. Rotate the password in env, redeploy, done.

## Watch your ears: SASL module

WKafka's `04_security_sasl` example runs a full auth cycle over **SASL_PLAINTEXT** end to end. Point the producer at a secured broker

```python
kafka = WKafka(
    bootstrap_servers="localhost:30092",
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username="external-user",
    sasl_plain_password="...",
)
```

Then produce and consume as usual — the decorator API doesn't change. Auth is transport, not interface.

## The rule we fake nothing about

> 🔐 **Authentication is not a feature you add later.** It's the first thing you configure when the broker is not on your laptop. WKafka makes that first thing a typed, one-time declaration — and keeps SCRAM-SHA-256/512 within reach for the "passwords in transit" crowd.

## Try it yourself

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub, `04_security_sasl` runnable on SASL_PLAINTEXT:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io
- **MIT, typed (mypy), Python 3.9–3.14, loguru, tox**

*What's the angriest response you've ever gotten from a broker that was "just testing in dev"? Story time in the comments.*

#Kafka #Security #SASL #Python #DataEngineering #OpenSource #Wisrovi