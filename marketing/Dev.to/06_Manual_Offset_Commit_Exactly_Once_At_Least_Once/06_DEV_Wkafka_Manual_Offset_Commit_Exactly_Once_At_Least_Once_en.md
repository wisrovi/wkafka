### Kafka's default offset commit is a polite fiction: "read" ≠ "processed". Day 06 manual commit makes the difference explicit.

Your auto-commit says "done" the moment the fetch lands — before your handler, your side effects, your everything. For financial ops, inventory or orders, that gap is where "at-least-once" becomes "lost exactly that one". WKafka Day 06 hands the decision back: `auto_commit=False` + `msg.commit()` grants **typed at-least-once**, enjoyed consciously.

> Day 06, WKafka open-source series — decorator-based, MIT, reproducible.

## The one change

```python
kafka = WKafka(auto_commit=False)

@kafka.consumer(topic="financial_ops", format="json")
def on_op(msg):
    apply_operation(msg.value)      # side effect happens FIRST
    msg.commit()                    # "done" is declared AFTER
```

**Guarantee:** if the process dies between `apply_operation` and `commit()`, the message is redelivered on restart. The offset advances only when your code says "processed".

## Why this is the honest at-least-once

- Financial ops handler re-executes and commits only at the end — "a charge happens once" becomes defensible.
- Key ordering (Day 02) intact; the commit travels after the handler, not ahead of it.
- Clean shutdown (Day 01) untouched: same decorator, same `block=True` — only now the guarantee belongs to your code.

## Verified & reproducible

- **`08_manual_commit` runnable:** https://github.com/wisrovi/wkafka
- **PyPI:** https://pypi.org/project/wkafka
- **Docs:** https://wkafka.readthedocs.io
- MIT · Python 3.9→3.14 · mypy · loguru · tox multi-version · 14+ examples

*How many times has auto-commit lied to you? The offset-thief count in comments...*

#Kafka #Python #AtLeastOnce #ExactlyOnce #OffsetCommit #DataEngineering #Streaming #Wisrovi
