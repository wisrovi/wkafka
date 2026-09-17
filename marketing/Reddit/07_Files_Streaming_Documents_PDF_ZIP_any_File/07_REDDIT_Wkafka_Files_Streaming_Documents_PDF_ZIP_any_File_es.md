**WKafka Día 07 — deja de mandar "demostraciones de archivos" como blobs misteriosos. Los bytes merecen nombre + MIME.**

El tutorial de "file streaming" que copia toda demo manda bytes crudos y deja que el consumidor adivine si era PDF, ZIP o TXT. Con `format="file"` de WKafka, `file_name` + `content_type` viajan en los headers (contrato Día 02/03) y los bytes viajan intactos con su identidad — round-trip determinista, reproducible.

> Día 07 de la serie WKafka open-source — basada en decoradores, MIT, decoradores del Día 01 idénticos.

```python
# productor — bytes + documentación viajan juntos
with kafka.producer() as p:
    pdf = open("factura_2024.pdf", "rb").read()
    p.send(topic="file_sharing_topic",
           value=pdf,
           key=f"factura_{i}",                     # orden por factura (Día 02)
           headers={"file_name": "factura_2024.pdf",
                    "content_type": "application/pdf"},
           format="file")

# consumidor — escribe un archivo REAL a disco, nombre + MIME intactos
@kafka.consumer(topic="file_sharing_topic", format="file")
def on_file(msg):
    nombre = msg.headers["file_name"]
    with open(nombre, "wb") as f:
        f.write(msg.value)
    print(f"📄 {nombre} ({msg.headers['content_type']}) — {len(msg.value)} bytes")
```

Sin adivinar extensiones, sin inflar con base64, las claves siguen garantizando orden por factura (Día 02) y el commit manual (Día 06) sigue protegiendo el fan-out.

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `examples/07_files` (ejecutable, reproducible):** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io · MIT · Python 3.9→3.14 · mypy · loguru · tox

*¿Qué archivo enviaste alguna vez por Kafka como "bytes crudos, a ver qué era" cuando merecía un nombre real?*

#Kafka #FileStreaming #PDF #ZIP #Python #OpenSource #Wisrovi