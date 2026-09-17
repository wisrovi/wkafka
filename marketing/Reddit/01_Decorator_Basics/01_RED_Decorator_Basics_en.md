[P] Open sourcing a Kafka wrapper for Python that kills almost all consumer boilerplate with one decorator — 14 reproducible examples included

Maintainer here. After tackling a bare-metric pipeline as open research, the same lesson showed up again for Kafka: most of a consumer file is scaffolding, not your logic. So I built and now maintain the decorator-first wrapper behind this.

**The hook:** one decorator replaces the consumer loop, deserialization and clean shutdown.

```python
from wkafka import WKafka

kafka = WKafka(dynamic_group_id=True)

@kafka.consumer(topic="orders", format="json")
def on_order(msg):
    print(f"New order: {msg.value}")
```

**Beyond strings it ships:**
- Dedicated serializers for JSON, YAML, images (OpenCV/NumPy/PIL), files (PDF, ZIP, TXT) and Pydantic validation
- SASL PLAIN/SCRAM, KRaft, manual offset commit, retries + DLQ, async handlers
- Python 3.9–3.14, strict typing (mypy), loguru, tox multi-version, MIT
- 14 runnable examples in the repo
- Stable public LTS API

**Why it earned a spot on my stack:** reusable, open source, typed, with real serializers — not just ASCII.

Repo: https://github.com/wisrovi/wkafka
PyPI: https://pypi.org/project/wkafka

*What Kafka boilerplate would you delete first? Fighting in the comments below.*

#Kafka #Python #DataEngineering #OpenSource #Streaming #Wisrovi