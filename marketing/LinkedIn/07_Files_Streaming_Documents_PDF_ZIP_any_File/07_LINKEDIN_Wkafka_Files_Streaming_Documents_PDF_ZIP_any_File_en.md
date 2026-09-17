### Day 07: Kafka files with a NAME + MIME. `format="file"` turns "raw blob, you figure it out" into a typed round-trip.

Day 07 of the WKafka open-source series (decorator-based, MIT, reproducible): most file-demos ship raw bytes and make the consumer guess the extension. WKafka carries `file_name` + `content_type` in headers — so the consumer writes a REAL file, name and MIME intact, bytes untouched. Deterministic.

> MIT · Python 3.9→3.14 · mypy · loguru · tox · 14+ reproducible examples.

```python
with kafka.producer() as p:
    pdf = open("invoice_2024.pdf", "rb").read()
    p.send(topic="file_sharing_topic",
           value=pdf,
           key=f"invoice_{i}",                      # per-invoice order (Day 02)
           headers={"file_name": "invoice_2024.pdf",
                    "content_type": "application/pdf"},
           format="file")

@kafka.consumer(topic="file_sharing_topic", format="file")
def on_file(msg):
    name = msg.headers["file_name"]
    with open(name, "wb") as f:
        f.write(msg.value)
    print(f"📄 {name} ({msg.headers['content_type']}) — {len(msg.value)} bytes")
```

No extension-guessing, no base64 bloat. Keys → order (Day 02), headers → context (Day 02), manual commit → `msg.commit()` after disk write (Day 06).

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `examples/07_files` (runnable, reproducible):** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*What file did you once ship through Kafka as raw mystery bytes that deserved a `file_name`?*

#Kafka #FileStreaming #PDF #ZIP #Python #OpenSource #Wisrovi