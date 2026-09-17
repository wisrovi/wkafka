# Stop writing raw Kafka boilerplate in Python. A decorator is enough.

Most Kafka clients force you to wire up the consumer loop, the deserializers, the error handling and the shutdown hook by hand — every single time. For a library, that boilerplate is the real tax on velocity.

> Day 01 of the WKafka open-research series — Open Source, MIT, reproducible.

## Why it matters

After learning to read pipelines through raw telemetry (WPipe), the same lesson applies to messaging: the hours you spend writing the consumer skeleton are hours you don't spend on the message. A decorator-based wrapper changes the equation — you declare behavior, not plumbing.

## What we built

`WKafka` is a professional decorator-based Kafka wrapper for Python. You configure cluster, serialization and validation declaratively; the handler is a plain function:

```python
from wkafka import WKafka

kafka = WKafka(dynamic_group_id=True)

@kafka.consumer(topic="orders", format="json")
def on_order(msg):
    print(f"New order: {msg.value}")
```

One decorator gives you automatic deserialization (`format="json"`), config resolution (reads `KAFKA_SERVER` or defaults to `localhost:9092`) and a clean shutdown. No manual loops or offset commits to get started.

## Beyond strings

Kafka is not just ASCII. WKafka ships native serializers for **JSON, YAML, images (OpenCV/NumPy/PIL) and files (PDF, ZIP, TXT)** via `format="file"`, plus **Pydantic validation** for typed payloads and image/file streaming mode. It runs on Python 3.9 through 3.14.

## The result

> 🎯 One decorator, one handler, one topic. Less plumbing, more message: the wrapper does the heavy lifting and you keep the business logic.

## Why it fits your stack

It is reusable, open source (MIT), strictly typed (mypy), with professional logging via `loguru` and multi-version testing via `tox`. One repo with 14+ reproducible examples and a stable LTS public API.

## Try it yourself

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + 14+ reproducible examples:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

---

*How many lines of Kafka boilerplate would a decorator save you? Drop the number in the comments.*

#Kafka #Python #Streaming #Decorators #OpenSource #DataEngineering #Wisrovi
