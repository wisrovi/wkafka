## Reinventing the wheel every Kafka project? Just declare it once.

Tired of copy-pasting the consumer loop, the deserializer switch and the try/except into every new service? After 20 days of the WKafka open-source series, here's the pattern that removed all of it for us.

> We ship WKafka: a decorator-based Kafka wrapper for Python (MIT, open source). Code > opinions.

### What we did
One decorator replaces the whole consumer skeleton:

```python
from wkafka import WKafka

kafka = WKafka(dynamic_group_id=True)

@kafka.consumer(topic="basic_json", format="json")
def on_message(msg):
    print(f"Received: {msg.value}")
```

Config resolves from `KAFKA_SERVER` (or `localhost:9092`), serialization is handled by `format`, shutdown is clean. No `while True`, no manual commits to start.

### Numbers we can defend
- **Python 3.9 → 3.14** supported (multi-version CI with tox).
- **14 reproducible examples** in the repo, from images/PDFs (OpenCV, NumPy, PIL) to SASL auth and manual offset control.
- **MIT license**, typed (mypy), `loguru` logging, **LTS public API**.

### Justifications (why Kafka != raw ASCII)
Native serializers for JSON, YAML, images, files and Pydantic validation — so your messages are typed business objects, not byte soup.

### References
- Code: https://github.com/wisrovi/wkafka
- PyPI: https://pypi.org/project/wkafka
- Docs: https://wkafka.readthedocs.io

*What's the one Kafka feature you wish was a one-liner? Tell us below.*
