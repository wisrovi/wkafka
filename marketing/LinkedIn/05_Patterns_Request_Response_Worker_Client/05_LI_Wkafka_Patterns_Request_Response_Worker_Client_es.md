### Open-source tooling day: WKafka request-response over Kafka — the honest RPC flagship.

The "event-driven bus" demo always shows fire-and-forget. The half nobody films: an actual **answer**. WKafka runs the worker ↔ client ↔ microservice trio on the same Day-01 decorator, both directions — keys for per-requester order, `correlation_id` headers for the round-trip, zero thread juggling.

```python
@kafka.consumer(topic="srpc_requests", format="json")
def on_request(req):
    kafka.producer_send(topic="srpc_responses", value=compute(req.value),
                        key=req.key,
                        headers={"correlation_id": req.headers["correlation_id"]},
                        format="json")
```

Open source, MIT, 14+ runnable examples, Python 3.9→3.14.
**PyPI:** https://pypi.org/project/wkafka · **GitHub `05_patterns`:** https://github.com/wisrovi/wkafka · **Docs:** https://wkafka.readthedocs.io

*What would you rather call over Kafka than HTTP? I'll read the list — and so will the comments.*

#Kafka #RPC #Microservices #Python #OpenSource #Wisrovi