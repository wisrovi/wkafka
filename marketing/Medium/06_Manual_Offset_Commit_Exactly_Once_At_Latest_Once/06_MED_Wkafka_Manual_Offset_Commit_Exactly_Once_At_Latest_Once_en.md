# WKafka Day 06: "auto_commit" is the polite fiction that made you lose a message. Manual commit is the honest say-so.

Most Kafka OPS peoples came running from the same scar: framework knows it *fetched*, you think it *processed*. Those are two different truths. Kafka's default auto-commit collapses them into one — the offset advances on **fetch**, not on your handler's real work. WKafka makes the collapse your choice.

> Day 06 of 20, WKafka open-source series — decorator-based, MIT, reproducible, Python 3.9→3.14.

## The lie in "auto"

With the default, the client marks a message "done" the moment it's **delivered to your handler** — not when your handler finished with it. For financial ops, inventory, or anything with a durable side effect, that distinction is exactly where orders get lost at 2 AM.

## The remedy: commit when YOUR code says "done"

WKafka keeps Day-01's decorator identical and hands you the offset decision explicitly:

```python
from wkafka import WKafka

kafka = WKafka(dynamic_group_id=True)

@kafka.consumer(topic="financial_ops", format="json", auto_commit=False)
def on_op(msg):
    apply_operation(msg.value)     # your business effect, FIRST
    msg.commit()                   # only here is "done" a real thing
```

- At-least-once, chosen not accidental: offset moves only when your handler returns success and you say `commit()`.
- Reprocessing after a crash is honest, second-chance logic — your handler stays idempotent (Day 02 keys → same partition → per-entity order preserved).
- Clean shutdown, same decorator, Day-01 contract untouched; `examples/08_manual_commit` is runnable and reproducible.

## The honest trade-off table

| Mode | Offset advances | "Lost" window | Your guarantee |
|------|-----------------|---------------|----------------|
| auto_commit (default) | on fetch | after fetch, before handler | at-most-once, silently |
| manual commit | on `msg.commit()` | only if you never commit | at-least-once, by contract |

## Try it yourself

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `08_manual_commit` (runnable, reproducible):** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io
- **PyPI dyn:** https://pypi.org/project/wkafka

*Have you ever lost work to an auto-committed offset? The honest stories are the only metadata Kafka can't duplicate.*