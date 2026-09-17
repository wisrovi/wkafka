[P] Two discovery posts in a row hit the same truth: Kafka does NOT give you ordering for free — you route with keysolar, and headers ride along. WKafka makes both one line.

First, the honest gap between "ordered" marketing and per-partition reality. Second, how two WKafka primitives close it — keys for per-entity ordering, headers for searchable context that never touches the payload schema.

Story: KAFKA ORDERING IS A CONTRACT YOU MUST EARN

You can brand "ordered, exactly-once" on a slide; you can imply it in a summary; but you cannot ship it without routing deterministically. Kafka preserves order **per partition** — parallel queues, not one FIFO. Two events for the same entity that hit different partitions arrive in coin-flip order. The fix is not a framework feature; it's a routing strategy you choose, one line at a time:

```python
with kafka.producer() as p:
    p.send(
        topic="advanced_topic",
        value={"event_id": i, "status": "active"},
        key=f"user_{i}",           # same key → same partition → order preserved
        headers={"correlation_id": f"corr_{i}"},
        format="json",
    )
```

**Key = the routing strategy.** Same key → same partition → deterministic order, per entity.
**Headers = free metadata.** Consumer reads both sides:

```python
print(f"Key: {msg.key}")
print(f"Headers: {msg.headers}")
```

Trace IDs, source system, priority — context that travels with the message without bloating the payload. Perfect for cross-service correlation (distributed tracing) without touching the value schema.

**What your team gets**
- Per-entity ordering & searchable headers in one line
- Python 3.9 → 3.14, mypy-strict, loguru, tox multi-version, MIT
- 14+ reproducible examples (`02_advanced` included), stable LTS API

**Try it**
- PyPI: https://pypi.org/project/wkafka
- GitHub + 14 runnable examples: https://github.com/wisrovi/wkafka
- Docs: https://wkafka.readthedocs.io

*What's the Kafka ordering assumption that your codebase currently depends on, unexamined? That's the one to check this week.*