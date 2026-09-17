# Stop writing raw Kafka boilerplate in Python. A decorator is enough.

Most Kafka clients force you to wire up the consumer loop, the deserializers, the error handling and the shutdown hook by hand — every single time. For a library, that boilerplate is the real tax on velocity.

> Day 01 of the WKafka open-source series — a decorator-based Kafka wrapper for Python, MIT-licensed and reproducible.

## The problem

Every producer and consumer in your codebase ends up with the same scaffolding: a `while True` loop, a manual deserializer switch, a commit policy, and a try/except that swallows the partition you were reading. It works — until you have ten of them.

## How WKafka removes it

`WKafka` is a professional, decorator-based Kafka wrapper. You declare the behavior, not the plumbing:

```python
from wkafka import WKafka

kafka = WKafka(dynamic_group_id=True)

@kafka.consumer(topic="basic_json", format="json")
def on_message(msg):
    print(f"Received: {msg.value}")
```

One decorator gives you the consumer handler with serialization (`format`), the config resolution (it reads `KAFKA_SERVER` or defaults to `localhost:9092` automatically) and a clean shutdown. No loop, no deserializer switch, no manual commits to get started.

## Beyond strings

Kafka is not just ASCII. WKafka ships native serializers for **JSON, YAML, images (OpenCV/NumPy/PIL) and arbitrary files (PDF, ZIP, TXT)** via `format="file"`, plus **Pydantic validation** for typed payloads and a **file/image streaming mode** for multimedia. It runs on Python 3.9 through 3.14.

## Why it matters for a lean team

A one-line decorator that reads your config, validates your payloads and lets you focus on the message instead of the client. That is the difference between a Kafka binding and a Kafka affordance.

## Try it yourself

- **GitHub + examples (14 runnable):** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io
- **PyPI:** https://pypi.org/project/wkafka

*Have you spent a Friday fixing a Kafka consumer that silently stopped consuming? I'd love to read your story below.*

#Python #Kafka #Streaming #Decorators #OpenSource #MLOps #DataEngineering #Wisrovi
