## PDFs, ZIPs and TXT are Kafka citizens too — stop re-encoding them as "bytes nobody can name."

Day 07 in the WKafka open-source series: `format="file"` + the header contract (Day 03 headers, Day 02 keys) gives binary messages the *one thing* raw-byte demos never show: a **name and a MIME type** riding along.

The honest round-trip — producer keeps `file_name` + `content_type` in headers while bytes flow in the value; consumer writes a real file to disk:

```python
from wkafka import WKafka

kafka = WKafka(dynamic_group_id=True)

with kafka.producer() as p:
    with open("contract_final.pdf", "rb") as f:
        pdf = f.read()
    p.send(
        topic="file_sharing_topic",
        value=pdf,
        key=f"contract_{i}",                    # order by contract (Day 03)
        headers={"file_name": "contract_final.pdf",
                 "content_type": "application/pdf"},  # Day-03 header contract
        format="file",
    )

@kafka.consumer(topic="file_sharing_topic", format="file")
def on_file(msg):
    name, ctype = msg.headers["file_name"], msg.headers["content_type"]
    with open(name, "wb") as f:
        f.write(msg.value)
    print(f"📄 {name} ({ctype}, {len(msg.value)}B) -> disk")
```

No "guess the extension" step, no base64 blowup, same Day-01 decorator. MIT · Python 3.9→3.14 · mypy · loguru · tox.

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `examples/07_files` runnable:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*What file did you first wire through Kafka as a raw payload? Tell us the MIME that should've traveled with it — in the comments.*

#Kafka #Files #Streaming #PDF #ZIP #Python #OpenSource #Wisrovi