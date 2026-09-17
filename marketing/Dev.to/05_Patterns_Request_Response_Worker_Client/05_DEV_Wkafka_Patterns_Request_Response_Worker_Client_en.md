# Kafka as a microservice bus means answering requests. WKafka types that contract.

Events are only half the bus. The other half is **request-response** — a worker that answers, a client that correlates. Nail it naively and you reinvencorrelation, timeout and DLQ at onceholme. WKafka's Day 05 demonstrates the typed worker/client/microservice trio.

> Day 05 of the WKafka open-source series — open access, MIT, reproducible.

## The contract without hand-wired correlation

The worker consumes requests and answers on a response topic. Keys keep requester order; headers carry the round-trip:

```python
# worker.py
@kafka.consumer(topic="srpc_requests", format="json")
def on_request(req):
    result = compute(req.value)                                   # your logic, nothing else
    kafka.producer_send(
        topic="srpc_responses",
        value=result,
        key=req.key,                                             # back to the asking entity
        headers={"correlation_id": req.headers["correlation_id"]},
        format="json",
    )

# client.py
p.send(topic="srpc_requests", value=pkt, key=f"user_{i}",
       headers={"correlation_id": f"corr_{i}"}, format="json")
```

The consumer decorator is 100% Day-01: loop, serialization, key order and clean shutdown unchanged. The reply is just a producer line from inside the handler — no thread lending, no correlation table.

## Why RPC-over-Kafka wins here

- **Server-per-service is a deployment, not a framework choice.** WKafka doesn't build a Kafka-embedded HTTP server; it keeps the bus honest: worker handles request topic, reply goes to response topic, both typed.
- **Order per requester** (key → partition) = responses return in request order.
- **`correlation_id` header** round-trips without schema churn (Day 02) — and auth stays transport (Day 04).

## The result

> 🎯 `05_patterns` = runnable `worker.py` + `client.py` + `microservice.py`. Same decorator, bidirectional, reproducible. RPC-over-Kafka, typed.

## Try it

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `05_patterns` runnable trio:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*What call would you rather make over Kafka than HTTP? The comments are the honest RPC registry.*

#Kafka #Microservices #Python #RPC #Streaming #OpenSource #Wisrovi