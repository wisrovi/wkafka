### Stop treating Kafka as one-way events. The other half is request-response — and WKafka types the round-trip.

Most "Kafka as a bus" demos stop at fire-and-forget. The awkward half nobody demos: a worker that *answers*. So the Day-05 WKafka example is the honest trio: worker ↔ client ↔ microservice, request-response over Kafka, same Day-01 decorator on both faces.

```python
# worker.py — consume request, answer through the same bus
@kafka.consumer(topic="srpc_requests", format="json")
def on_request(req):
    result = compute(req.value)                       # your logic
    producer.send(topic="srpc_responses",
                  value=result,
                  key=req.key,                        # same key → same requester → same partition
                  headers={"correlation_id": req.headers["correlation_id"]},
                  format="json")
```

```python
# client.py — send and let the key/header do the correlation
with kafka.producer() as p:
    p.send(topic="srpc_requests",
           value={"op": "compute", "payload": req},
           key=f"client_{i}",
           headers={"correlation_id": f"corr_{i}"},
           format="json")
```

**Why RPC-over-Kafka instead of HTTP glue:**
- Worker and client are **typed contracts**, not "call POST and hope for a 200". The decorator is the same one from Day 01 — loop, serialization, key-order, clean shutdown.
- **Keys** keep requester ordering (Day 02); **headers correlation_id** round-trip the trace (Day 01 headers, Day 04 auth) without schema churn.
- Reproducible: 14+ examples, `05_patterns` runs the worker+client+microservice trio end-to-end.

**Run it:**
- PyPI: https://pypi.org/project/wkafka
- GitHub + `05_patterns`: https://github.com/wisrovi/wkafka
- Docs: https://wkafka.readthedocs.io

*Which service do you wish actually answered over Kafka instead of a REST call? Drop the list.*

#Kafka #RPC #Microservices #Python #Streaming #OpenSource #Wisrovi
