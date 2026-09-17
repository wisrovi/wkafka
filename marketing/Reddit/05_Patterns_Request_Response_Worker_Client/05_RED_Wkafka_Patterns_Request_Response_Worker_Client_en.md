[P] WKafka Day 05 — Kafka as a typed request-response bus, not just fire-and-forget events. Worker ↔ client ↔ microservice trio, runnable.

Almost every "Kafka as your integration bus" tutorial stops at events. The honest other half is request-response: a worker *answers* a client, and correlating the reply by hand is where teams reinvent correlation tables, timeouts and DLQs all at once. WKafka Day-05 (`05_patterns`) runs the full trio — worker, client, microservice — with the exact Day-01 decorator contract, in both directions.

```python
# worker.py — consume the request, answer through the same bus
@kafka.consumer(topic="srpc_requests", format="json")
def on_request(req):
    result = compute(req.value)
    kafka.producer_send(
        topic="srpc_responses",
        value=result,
        key=req.key,                    # same key → same requester → coherent per-entity flow
        headers={"correlation_id": req.headers["correlation_id"]},
        format="json",
    )

# client.py — one call, key keeps requester order, header round-trips the trace
with kafka.producer() as p:
    p.send(topic="srpc_requests",
           value={"op": "compute", "payload": req},
           key=f"client_{i}",
           headers={"correlation_id": f"corr_{i}"},
           format="json")
```

## Why request-response-over-Kafka (and not HTTP glue)

- **Typed contract on both faces.** Worker and client are the same Day-01 typed contract, not "POST and hope for a 200".
- **Keys preserve requester order** (Day 02); the `correlation_id` header (Day 02/04 headers + Day 04 auth are transport) round-trips without schema churn.
- **One decorator, no thread juggling** — the worker replies from inside the handler; the client stays a plain decorator consumer.

## Verified, reproducible

- **`05_patterns` = worker + client + microservice, runnable:** https://github.com/wisrovi/wkafka
- **PyPI:** https://pypi.org/project/wkafka
- **Docs:** https://wkafka.readthedocs.io
- Python 3.9 → 3.14, mypy, loguru, tox, MIT, 14+ examples

*What service do you wish would answer over Kafka instead of an HTTP call? Honest list in the comments.*

#Kafka #RPC #Microservices #Python #Streaming #OpenSource #Wisrovi