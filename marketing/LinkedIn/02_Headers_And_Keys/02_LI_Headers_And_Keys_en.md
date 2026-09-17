### "Kafka is ordered" — reading the fine print: ordered per partition, only when you route deterministically.

Most tutorials show Kafka as one big FIFO queue. The operations-reality: it's an ordered queue **per partition**, running in parallel. Same-entity events routed to different partitions arrive in dice-throw order. WKafka hands you the two levers that fix it: **keys** (deterministic routing → per-entity order) and **headers** (carry-along metadata → searchable context without schema churn).

```python
with kafka.producer() as p:
    p.send(topic="advanced_topic",
           value={"event_id": i, "status": "active"},
           key=f"user_{i}",
           headers={"source": "api_gateway",
                    "correlation_id": f"corr_{i}"},
           format="json")
```

Consumer reads both faces:

```python
print(f"Key: {msg.key}")
print(f"Headers: {msg.headers}")
```

▶ 14 runnable examples, tipado estricto, Python 3.9→3.14, MIT.
Repos: https://github.com/wisrovi/wkafka · PyPI: https://pypi.org/project/wkafka · Docs: https://wkafka.readthedocs.io

*What Kafka ordering assumption cost your team the most debugging time?*

#Kafka #Python #Streaming #DataEngineering #OpenSource #Wisrovi