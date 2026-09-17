[P] WKafka Day 06 — "auto_commit" made me lose a payment once. Here's the honest fix.

Default Kafka: the offset advances when the message is *delivered* to your handler — not when your handler claims success. For anything financial, that gap is a real expense. WKafka flips auto_commit=False and puts `msg.commit()` where your business effect finishes:

```python
@kafka.consumer(topic="financial_ops", format="json", auto_commit=False)
def on_op(msg):
    apply_operation(msg.value)   # side effect first
    msg.commit()                 # done = done, explicitly
```

At-least-once by contract. Reproducible (`08_manual_commit`), MIT, Python 3.9→3.14, mypy, loguru.

- PyPI: https://pypi.org/project/wkafka
- GitHub: https://github.com/wisrovi/wkafka
- Docs: https://wkafka.readthedocs.io

*The "auto" in auto_commit is Kafka being polite about something it can't verify. Today we stop being nice.*