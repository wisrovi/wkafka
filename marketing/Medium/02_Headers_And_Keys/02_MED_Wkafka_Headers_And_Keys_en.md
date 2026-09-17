# Kafka only guarantees order if you earn it — and your key is the currency.

"Idempotent, ordered, exactly-once" — says the magic marketing slide. The closer-to-reality version: Kafka guarantees order **per partition**, and only if you route deterministically. WKafka hands you the two levers that make that promise true: the **key** and the **headers**.

> Day 02 of the WKafka open-source series — open access, MIT, reproducible.

## The problem with "it's ordered!"

Most people assume Kafka is one big ordered queue. It isn't. It's an ordered queue per partition, in parallel. If two messages for the same logical entity (a user, an order, a device) land in different partitions, "order" is a coin toss.

## How WKafka earns it

Keys are not decoration — they're the routing strategy. The producer example shows it literally:

```python
with kafka.producer() as p:
    p.send(
        topic="advanced_topic",
        value={"event_id": i, "status": "active"},
        key=f"user_{i}",                 # <-- same key → same partition → order preserved
        headers={"source": "api_gateway",
                 "correlation_id": f"corr_{i}"},
        format="json",
    )
```

Every event for `user_7` lands in the same partition, in the same order it was sent. The consumer reads them in that order — no extra work.

## Headers: metadata that travels for free

The consumer side reads both sides of the message:

```python
print(f"Key: {msg.key}")
print(f"Headers: {msg.headers}")
```

Trace IDs, source system, priority — contextual metadata that rides along without touching the payload. Perfect for correlation across services (think distributed tracing) without schema churn on the value itself.

## The takeaway

> 🎯 Routing by key gives you **per-entity ordering**; headers give you **searchable context** without bloating the payload schema. Both are one line in WKafka.

## Try it yourself

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + 14 runnable examples, `02_advanced`:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*What's the most surprising thing about how Kafka actually orders messages? Share it below.*

#Kafka #Python #Streaming #DataEngineering #OpenSource #Wisrovi
