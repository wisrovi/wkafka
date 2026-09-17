### WKafka Day 07: binary messages deserve a name. `format="file"` + headers give Kafka's mute bytes a contract.

Raw-byte Kafka demos teach you to send "bytes and hope the consumer recognizes a PDF". Day 07 of the WKafka open-source series (MIT, decorator-based, reproducible) makes the file a first-class message: `file_name` + `content_type` ride in headers (Day-02/03 contract), bytes round-trip untouched.

The two-sided typed file streaming — same Day-01 decorator, both faces:

```python
with kafka.producer() as p:
    data = open("report_2024.pdf", "rb").read()
    p.send(topic="file_sharing_topic", value=data,
           key=f"report_{i}",
           headers={"file_name": "report_2024.pdf",
                    "content_type": "application/pdf"},
           format="file")

@kafka.consumer(topic="file_sharing_topic", format="file")
def on_file(msg):
    with open(msg.headers["file_name"], "wb") as f:
        f.write(msg.value)     # real bytes → real disk file
    print(f"📄 {msg.headers['file_name']} ({msg.headers['content_type']})")
```

- Keys → order per requester (Day 02); headers → context (Day 02/03); nothing invented
- MIT · Python 3.9→3.14 · mypy · loguru · tox · 14+ reproducible examples

**PyPI:** https://pypi.org/project/wkafka · **GitHub + `07_files`:** https://github.com/wisrovi/wkafka · **Docs:** https://wkafka.readthedocs.io

*Which "file" do you currently ship as raw bytes when it deserves a name? Comments are open.*

#Kafka #Files #PDF #ZIP #Streaming #Python #OpenSource #Wisrovi