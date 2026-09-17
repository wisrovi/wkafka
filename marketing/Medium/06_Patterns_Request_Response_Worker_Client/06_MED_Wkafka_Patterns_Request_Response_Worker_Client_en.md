# Kafka can do request-response. Most demos just... don't show it. Let's be honest about that.

Fire-and-forget events are the demo half. The other half — a *worker* that actually responds — is where "event-driven" meets "someone is waiting". WKafka Day 06 makes the honest pattern explicit: **request-response over Kafka, same Day-01 decorator contract, both faces**.

> Day 06 of the WKafka open-source series — decorator-based, MIT, reproducible.

## The problem nobody demos

Everyone shows the producer. Nobody shows the **answer**. RPC-over-Kafka has an ugly reputation because correlation, timeouts and partitions are hand-rolled — the "worker microservice" becomes a week of thread-lending and DLQ archaeology.

## The WKafka pattern: worker ↔ client, one decorator

`05_patterns` is a runnable trio — worker, client, microservice — where the **same Day-01 decorator** that consumes the request also responds. Keys for order (Day 02), headers for correlation (Day 02/03), typed condition serialization is Day-01 all the way down:

```python
# worker.py — consume request, respond typed, same decorator
@kafka.consumer(topic="request_topic", format="json")
def on_request(msg):
    print(f"⚙️ Procesando ID {msg.value['id']}...")
    with kafka.producer() as p:
        p.send(topic="response_topic",
               value={"id": msg.value["id"], "status": "OK"},
               format="json")
```

```python
# client.py — dynamic group, one shot, waits
kafka = WKafka(dynamic_group_id=True)

@kafka.consumer(topic="response_topic", format="json")
def on_response(msg):
    print(f"🏁 Cliente recibió: {msg.value}")
```

## Why this beats "HTTP + cross your fingers"

- **Typed contract on both faces** — the response is a Day-01 typed message, not a `resp.json()` you hope followed a convention.
- **Order per requester** (Day-02 keys) carries over for free.
- **No DLQ-forgotten**: the worker replies from a plain handler — no correlation table, no thread juggling.

## The honest takeaway

> 🎯 Kafka request-response is not a Weekend Project. It's a decorator on both faces. WKafka's `05_patterns` runs the whole worker ↔ client ↔ microservice trio, reproducible.

## Try it yourself

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `05_patterns` (worker + client runnable):** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*What "call over Kafka" would you never stop telling your team about? The good ones live in the comments.*

#Kafka #Python #RPC #Microservices #Streaming #OpenSource #Wisrovi