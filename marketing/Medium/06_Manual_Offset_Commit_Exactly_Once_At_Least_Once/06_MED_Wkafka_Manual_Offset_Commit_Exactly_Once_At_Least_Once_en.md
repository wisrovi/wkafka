# Auto-commit is the quiet lie that ate your "processed" flag.

> Day 06 of the WKafka open-source series — decorator-based, MIT, reproducible.

There's a message that fails *after* the fetch and *before* your handler finishes. Auto-commit marks it "done" anyway. Your consumer moves on, the message stays dead in the topic — "Where did event 37 go?" is Kafka's most expensive sentence.

## WKafka Day 06: you own the commit

The decorator already gives you the loop, serializationring, clean shutdown and the contract. What it didn't silently decide is *when "processed" is true*. `auto_commit=False` hands you the honest moment:

```python
from wkafka import WKafka

kafka = WKafka(dynamic_group_id=True)

@kafka.consumer(
    topic="financial_ops",
    format="json",
    auto_commit=False,          # now "processed" is YOUR decision
)
def on_op(msg):
    ok = apply_operation(msg.value)     # e.g. idempotent, retried (Day 09)
    if ok:
        msg.commit()                    # exactly here: done = durable
```

## At-least-once, chosen (not a surprise)

`commit()` after success gives **at-least-once**: a crash between apply and commit → the same key re-delivers (Day 02). That's a *contract*, not a bug: your handler re-runs idempotently)Skip exact dedup to layering.

Alternative, also typed: commit before downstream side-effect for at-most-once, or wire Pydantic validation (Day 12) + DLQ (Day 09) to make at-least-once boring instead of scary.

## Why it deletes a whole class of incidents

- **No "lost message" tickets.** Auto-commit's hole (fail between fetch and handler result) is plain impossible when you pick the commit point.
- **One method, one responsibility** — `msg.commit()` keeps handler as Day-01: function of the message, no manual topic/partition/offset math.
- **Reproducible:** `08_manual_commit` example does the full financial-op cycle and prints the honest "received → processed → committed" sequence.

## Try it

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `08_manual_commit` (at-least-once, runnable):** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*Have you ever lost a Kafka message to a race between fetch and commit? I'd honestly like to count them — and they're exactly what Day 06 kills.*

#Kafka #Python #ExactlyOnce #AtLeastOnce #Streaming #DataEngineering #OpenSource #Wisrovi