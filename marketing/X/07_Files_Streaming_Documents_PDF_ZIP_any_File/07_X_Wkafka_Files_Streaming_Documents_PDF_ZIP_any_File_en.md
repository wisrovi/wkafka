### Files: "Kafka file demos" lie, the day they stream a contract they name it `blob`. Day 07 fixes the round-trip.

WKafka Day 07 of the open-source series — decorator-based, MIT, reproducible. `format="file"` carries `file_name` + `content_type` in headers (Day-02/03 contract) while `msg.value` carries the bytes untouched; the consumer materializes a *real* file on disk with name and MIME — no "bytes + let's guess the extension" ritual.

```python
# both sides, same Day-01 decorator, runnable example `07_files`
with kafka.producer() as p:
    data = open("contract_final.pdf", "rb").read()
    p.send(topic="file_sharing_topic", value=data,
           key=f"contract_{i}",
           headers={"file_name": "contract_final.pdf",
                    "content_type": "application/pdf"},
           format="file")

@kafka.consumer(topic="file_sharing_topic", format="file")
def on_file(msg):
    name = msg.headers["file_name"]
    with open(name, "wb") as f:
        f.write(msg.value)
    print(f"📄 {name} ({msg.headers['content_type']})")
```

Keys → per-entity order (Day 02), headers → typed context (Day 02/03), `msg.commit()` after disk write (Day 06). MIT · Python 3.9→3.14 · mypy · loguru · tox · reproducible.

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `07_files` runnable:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*Which "real file" of yours deserves a name+MIME through Kafka instead of being a raw blob?*