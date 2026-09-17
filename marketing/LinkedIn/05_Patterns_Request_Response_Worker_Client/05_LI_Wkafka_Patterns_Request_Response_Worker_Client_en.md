### "Event-driven" is a great slide. Until a service needs an answer — and Kafka just stares.

Most "Kafka as a bus" demos stop at fire-and-forget events. The unfunny half nobody shows: **request-response** — a worker that answers, a client that correlates. Do it by hand and you reinvent correlation tables, timeouts and DLQs all at once. WKafka Day 05 exposes it as a typed, MIT, reproducible contract.

> Day 05 of the WKafka open-source series — decorator-based, MIT, reproducible, 14+ examples.

## A worker that answers (not just consumes)

Both faces use the Day-01 decorator: loop, serialization, key ordering, clean shutdown. The reply is a one-time producer call from inside the handler:

```python
@kafka.consumer(topic="srpc_requests", format="json")
def on_request(req):
    result = compute(req.value)
    kafka.producer_send(
        topic="srpc_responses",
        value=result,
        key=req.key,                     # key → same requester → coherent per-entity flow
        headers={"correlation_id": req.headers["correlation_id"]},
        format="json",
    )
```

## The client: one call, correlation that rides along

```python
with kafka.producer() as p:
    p.send(topic="srpc_requests",
           value={"op": "compute", "payload": req},
           key=f"client_{i}",
           headers={"correlation_id": f"corr_{i}"},
           format="json")
```

## Why RPC-over-Kafka, not HTTP glue

- **Typed two-way contract**: worker and client share the same Day-01 contract — not "POST and cross your fingers for a 200".
- **Keys preserve per-requester order** (Day 02); the `correlation_id` header (Days 02/04) round-trips without schema churn.
- **Zero extra threads**: the reply leaves from inside the handler; the client stays a plain decorator consumer.

## Verified, reproducible

- **`05_patterns` = runnable worker + client + microservice:** https://github.com/wisrovi/wkafka
- **PyPI:** https://pypi.org/project/wkafka
- **Docs:** https://wkafka.readthedocs.io
- Python 3.9 → 3.14, mypy, loguru, tox, MIT, 14+ examples

*What service do you wish would answer over Kafka instead of an HTTP call? Honest list below.*

#Kafka #RPC #Microservices #Python #Streaming #OpenSource #Wisrovi