# Kafka as a microservice bus means you'll answer requests. Better type that contract.

Most Kafka-as-a-bus tutorials show fire-and-forget events. The other half of the bus is *request-response* — and if you bolt it onto the event API naively, you re-invent correlation, timeout and DLQ all at onceholme. WKafka exposes it as the Day 05 reproducible pattern: a typed client ↔ worker contract.

> Day 05 of the WKafka open-source series — decorator-based, MIT, reproducible.

## The request-response shape, without hand-wired correlation

A worker answers a request topic; the origin (client) tracks it with keys + correlation_id headers (Day 02 + Day 04, both still standing):

```python
# microservice.py — both faces in one typed module
@kafka.consumer(topic="srpc_requests", format="json")
def on_request(req):
    # same key → same partition → ordered per requester
    result = process_payload(req.value)
    producer.send(topic="srpc_responses",
                  value=result,
                  key=req.key,                  # route back to the asking entity
                  headers={"correlation_id": req.headers["correlation_id"]})

# client.py — send + correlate, one call
with kafka.producer() as p:
    p.send(topic="srpc_requests",
           value={"op": "compute", "payload": ...},
           key=f"user_{i}",
           headers={"correlation_id": f"corr_{i}"})
```

The consumer decorator stays 100% Day-01: loop, serialization, key order, clean shutdown. The response is just a producer write *from inside the handler* — no extra thread, no manual correlation table.

## Why this earns its own day

- **Mirrors the RPC mental model**: calling service A from service B stays a call, not "fire an event and hope".
- **Ordering by requester** (key → partition) means each caller's responses come back in request order — no reordering in the client.
- **correlation_id in headers** carries the round-trip without schema churn — trace it across services (Day 02 + Day 04 already wired).

## The result

> 🎯 05_patterns is a runnable worker/client/microservice trio. Same decorator, same typed contract, bi-directional, reproducible — the RPC-over-Kafka you actually wanted.

## Try it yourself

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `05_patterns` (worker/client/microservice trio):** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*What's a request you'd rather answer over Kafka than an HTTP call? I'm curious about your version of "events are the wrong shape here".*

#Kafka #Microservices #RPC #Streaming #Python #OpenSource #Wisrovi