"Kafka is ordered" — until you read the fine print. The fine print is: ordered **per partition**, only if you route deterministically. Here's how two WKafka primitives turn that fine print into behavior you can ship. Keys for routing order, headers for carry-along metadata.

> Day 02, WKafka open-source series — open access, MIT, reproducible.

## The "ordered" trap

Same-entity events (one user, one order, one device) landing in different partitions arrive in dice-throw order — even though each partition looks perfectly sequential. That's not a Kafka bug; it's the contract.

## Keys: the routing strategy (visible, not vibes)

Same key → same partition → preserved order. In WKafka it's one argument:

```python
with kafka.producer() as p:
    p.send(
        topic="advanced_topic",
        value={"event_id": i, "status": "active"},
        key=f"user_{i}",                     # deterministic route
        headers={"source": "api_gateway",
                 "correlation_id": f"corr_{i}"},
        format="json",
    )
```

Every event for `user_7` falls in the same partition, in send order. The consumer reads it sequentially — zero sorting code.

## Headers: context rides for free

Consumer side reads both faces of the message:

```python
print(f"Key: {msg.key}")
print(f"Headers: {msg.headers}")
```

Trace IDs, origin system, priority — searchable correlation metadata without touching the payload schema awake. Ideal for distributed tracing.

## The takeaway

> 🎯 Keys = per-entity ordering. Headers = searchable context, zero schema churn. Both are one line in WKafka.

## Try it

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + 14 runnable examples (`02_advanced`):** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*What did Kafka's *real* ordering rules surprise you the most? Drop it below.*

#Kafka #Python #Streaming #DataEngineering #OpenSource #Wisrovi