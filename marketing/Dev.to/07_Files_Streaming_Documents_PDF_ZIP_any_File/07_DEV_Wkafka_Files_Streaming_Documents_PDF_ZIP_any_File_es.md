## PDFs, ZIPs y TXT son ciudadanos Kafka — deja de re-enviar "bytes que nadie sabe nombrar".

Día 07 de la serie open-source WKafka: `format="file"` + el contrato de headers (Día 03 headers, Día 02 keys) les da a los mensajes binarios *lo único* que las demos de bytes crudos nunca muestran: un **nombre y un tipo MIME** viajando junto.

El round-trip honesto — el productor lleva `file_name` + `content_type` en headers mientras los bytes fluyen en el value; el consumidor escribe un archivo real a disco:

```python
with kafka.producer() as p:
    with open("contrato_final.pdf", "rb") as f:
        pdf = f.read()
    p.send(topic="file_sharing_topic",
           value=pdf,
           key=f"contract_{i}",                     # orden por contrato
           headers={"file_name": "contrato_final.pdf",
                    "content_type": "application/pdf"},
           format="file")

@kafka.consumer(topic="file_sharing_topic", format="file")
def on_file(msg):
    name, ctype = msg.headers["file_name"], msg.headers["content_type"]
    with open(name, "wb") as f:
        f.write(msg.value)
    print(f"📄 {name} ({ctype}, {len(msg.value)}B) → disco")
```

Sin paso de "adivina la extensión", sin inflado base64, mismo decorador del Día 01. MIT · Python 3.9→3.14 · mypy · loguru · tox.

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `examples/07_files` ejecutable:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*¿Qué archivo mandaste primero por Kafka como payload crudo? Dinos el MIME que debió viajar con él — en los comentarios.*

#Kafka #Files #Streaming #PDF #ZIP #Python #OpenSource #Wisrovi