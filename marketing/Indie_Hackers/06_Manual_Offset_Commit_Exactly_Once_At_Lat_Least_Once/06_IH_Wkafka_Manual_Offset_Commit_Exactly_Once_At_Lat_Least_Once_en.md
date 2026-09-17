Indie_Hackers — WKafka Day 06: auto_commit is Kafka's "trust me on the offset". The honest version is when the handler says "done", not the fetch loop.

The two lines that turn "at-most-once, silently" into "at-least-once, by contract":

```python
# producer → topics/financial_ops, format=json, key → per-entity order (Day 02)
kafka = WKafka(auto_commit=False)

@kafka.consumer(topic="financial_ops", format="json")
def on_op(msg):
    apply_operation(msg.value)      # side effect, FIRST
    msg.commit()                    # offset moves only when YOUR code is done
```

MIT · reproducible · `examples/08_manual_commit` · Python 3.9→3.14 · mypy · loguru · tox

- PyPI: https://pypi.org/project/wkafka
- GitHub: https://github.com/wisrovi/wkafka
- Docs: https://wkafka.readthedocs.io

*We released the stupidest auto-commit story in the comments. What's yours?*