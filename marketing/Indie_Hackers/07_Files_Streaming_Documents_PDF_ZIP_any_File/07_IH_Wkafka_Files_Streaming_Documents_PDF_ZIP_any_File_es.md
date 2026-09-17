### WKafka Día 07: los mensajes binarios merecen un nombre. `format="file"` + headers les dan contrato a los bytes mudos.

Las demos de bytes crudos te enseñan a enviar "bytes y esperar que el consumidor reconozca un PDF". El Día 07 de la serie open-source WKafka (MIT, decorator-based, reproducible) hace del archivo un mensaje de primera clase: `file_name` + `content_type` viajan en headers (contrato Día 02/03), los bytes intactos round-trip.

El file streaming tipado bilateralmente — mismo decorador del Día 01, ambas caras:

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
        f.write(msg.value)     # bytes reales → archivo real en disco
    print(f"📄 {msg.headers['file_name']} ({msg.headers['content_type']})")
```

- Claves → orden por peticionante (Día 02); headers → contexto (Día 02/03); nada inventado
- MIT · Python 3.9→3.14 · mypy · loguru · tox · 14+ ejemplos reproducibles

**PyPI:** https://pypi.org/project/wkafka · **GitHub + `07_files`:** https://github.com/wisrovi/wkafka · **Docs:** https://wkafka.readthedocs.io

*¿Qué "archivo" mandas hoy como bytes crudos cuando merece un nombre? Comentarios abiertos.*

#Kafka #Files #PDF #ZIP #Streaming #Python #OpenSource #Wisrovi