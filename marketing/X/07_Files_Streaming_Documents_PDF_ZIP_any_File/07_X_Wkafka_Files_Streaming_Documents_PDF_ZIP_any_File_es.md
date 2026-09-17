### Día 07: los "file demos" de Kafka mienten — suben el contrato como `blob`. Esto le da nombre y MIME al round-trip.

Día 07 de la serie open-source WKafka — decorator-based, MIT, reproducible. `format="file"` manda `file_name` + `content_type` en headers (contrato Día 02/03) y `msg.value` lleva los bytes intactos; el consumidor materializa un archivo *real* con nombre y MIME — sin el rito de "bytes + adivina la extensión".

```python
# ambas caras, mismo decorador Día 01, ejemplo ejecutable `07_files`
with kafka.producer() as p:
    data = open("contrato_final.pdf", "rb").read()
    p.send(topic="file_sharing_topic", value=data,
           key=f"contrato_{i}",
           headers={"file_name": "contrato_final.pdf",
                    "content_type": "application/pdf"},
           format="file")

@kafka.consumer(topic="file_sharing_topic", format="file")
def on_file(msg):
    nombre = msg.headers["file_name"]
    with open(nombre, "wb") as f:
        f.write(msg.value)
    print(f"📄 {nombre} ({msg.headers['content_type']})")
```

Claves → orden por entidad (Día 02), headers → contexto tipado (Día 02/03), `msg.commit()` tras escribir a disco (Día 06). MIT · Python 3.9→3.14 · mypy · loguru · tox · reproducible.

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `07_files` ejecutable:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*¿Qué archivo tuyo merece nombre+MIME por Kafka en vez de ser blob crudo?*