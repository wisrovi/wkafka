# Kafka only guarantees order if you earn it — keys are the currency

"Exactly-once, ordered, idempotent." Great marketing slide. Here's the version you can ship on: Kafka preserves order **per topic-partition**, and only when you route deterministically. This post shows the two WKafka primitives that turn that promise into behavior: **keys** and **headers**.

> Day 02 of the WKafka open-research series — open source, MIT, reproducible.

## The "ordered" trap

People treat Kafka like one big FIFO queue. It isn't. It's one ordered queue *per partition*, running in parallel. Two messages for the same business entity (a user, an order, a device) that land in different partitions are in a dice-roll order — even though each partition looks perfectly sequential on its own.

## Keys: deterministic routing you can see

Keys are not decoration; they're the routing strategy. Same key → same partition → same order. WKafka makes it explicit:

```python
with kafka.producer() as p:
    p.send(
        topic="advanced_topic",
        value={"event_id": i, "status": "active"},
        key=f"user_{i}",          # same key → same partition → preserved order
        headers={"source": "api_gateway",
                 "correlation_id": f"corr_{i}"},
        format="json",
    )
```

Every event for `user_7` lands in the same partition, in the exact order it was sent. The consumer reads them sequentially — zero extra work.

## Headers: metadata that rides for free

On the consumer side you read both sides of the message:

```python
print(f"Key: {msg.key}")
print(f"Headers: {msg.headers}")
```

Trace IDs, source system, priority — contextual metadata traveling with the message without touching the payload. Ideal for cross-service correlation (think distributed traces) without schema breaks on the value.

## The takeaway

> 🎯 Keys give you **per-entity ordering**; headers give you **searchable context** without bloating the schema. Both are one line in WKafka.

## Try it yourself

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + 14 runnable examples, `02_advanced`:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*What's the most surprising thing about how Kafka actually orders messages? Drop it in the comments.*

#Kafka #Python #Streaming #DataEngineering #OpenSource #Wisrovi
