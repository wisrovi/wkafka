**WKafka Day 07 — stop shipping Kafka file demos as mystery blobs. Give bytes a name + MIME.**

The "file streaming" tutorial every demo copies sends raw bytes and lets the consumer guess whether it's a PDF, ZIP or TXT. WKafka's `format="file"` puts `file_name` + `content_type` in headers (Day 02/03 contract) so the bytes ride intact with their identity — round-trip deterministic, reproducible.

> Day 07 in the WKafka open-source series — decorator-based, MIT, decorators from Day 01 stay identical.

```python
# producer — bytes + documentación viajan juntos
with kafka.producer() as p:
    pdf = open("invoice_2024.pdf", "rb").read()
    p.send(topic="file_sharing_topic",
           value=pdf,
           key=f"invoice_{i}",                     # order by invoice (Day 02)
           headers={"file_name": "invoice_2024.pdf",
                    "content_type": "application/pdf"},
           format="file")

# consumer — writes a REAL file to disk, name and MIME intact
@kafka.consumer(topic="file_sharing_topic", format="file")
def on_file(msg):
    name = msg.headers["file_name"]
    with open(name, "wb") as f:
        f.write(msg.value)
    print(f"📄 {name} ({msg.headers['content_type']}) — {len(msg.value)} bytes")
```

No extension-guessing, no base64 inflation, keys still guarantee per-invoice orderhol0920, manual commit (Day 06) still protects the fan-out.

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `examples/07_files` (runnable, reproducible):** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io · MIT · Python 3.9→3.14 · mypy · loguru · tox

*What file did you once stream through Kafka as "raw bytes, figure it out" that deserved a real name?*

#Kafka #FileStreaming #PDF #ZIP #Python #OpenSource #Wisrovi