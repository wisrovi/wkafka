### Stop writing raw Kafka boilerplate. A decorator is enough.

Code is read twice as often as it is written — and the Kafka consumer loop is the code nobody enjoys re-reading. Today we shipped a small, MIT open-source library for Python that turns the entire consumer skeleton into a single decorator.

**WKafka: decorator-based Kafka wrapper for Python**

```python
from wkafka import WKafka

kafka = WKafka(dynamic_group_id=True)

@kafka.consumer(topic="analytics", format="json")
def on_event(event):
    print(f"Event: {event.value}")
```

One decorator gives you the consuming loop, automatic deserialization, config resolution (env `KAFKA_SERVER` or `localhost:9092`) and clean shutdown.

**Why teams adopt it**
- JSON, YAML, image (OpenCV/NumPy/PIL) and file (PDF/ZIP/TXT) serialization natively
- SASL PLAIN/SCRAM, KRaft, manual offset commit, retries + DLQ, async handlers, Pydantic validation
- Python 3.9 → 3.14, strict mypy typing, loguru, tox multi-version, MIT
- 14 reproducible examples + stable LTS API

**Try it:** https://github.com/wisrovi/wkafka · https://pypi.org/project/wkafka

*What Kafka boilerplate would you delete first? Reply in the comments.*

#Kafka #Python #DataEngineering #OpenSource #Streaming #Wisrovi