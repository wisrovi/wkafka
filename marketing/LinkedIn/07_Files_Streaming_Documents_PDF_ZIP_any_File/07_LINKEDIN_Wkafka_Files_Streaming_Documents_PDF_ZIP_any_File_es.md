### Los PDFs, ZIPs y TXT también viajan por Kafka — con nombre y MIME, no como "blob misterioso que adivina el consumidor".

Día 07 de la serie open-source WKafka: los demos de "file streaming" te dejan mandando bytes crudos y rezando para que el otro lado sepa si era PDF o ZIP. Con `format="file"` de WKafka, `file_name` + `content_type` viajan en los headers (contrato Día 02) — el consumidor escribe un archivo **real** a disco, con nombre y tipo intactos, round-trip reproducible.

> WKafka — decorator-based, MIT, open-source, reproducible (14+ examples). Día 07.

```python
# productor — bytes + identidad (nombre+MIME) viajan juntos en headers
with kafka.producer() as p:
    pdf = open("factura_2024.pdf", "rb").read()
    p.send(topic="file_sharing_topic",
           value=pdf,
           key=f"factura_{i}",                      # orden por factura (Día 02)
           headers={"file_name": "factura_2024.pdf",
                    "content_type": "application/pdf"},
           format="file")

# consumidor — materializa un archivo real con su nombre, no "payload.bin"
@kafka.consumer(topic="file_sharing_topic", format="file")
def on_file(msg):
    nombre = msg.headers["file_name"]
    with open(nombre, "wb") as f:
        f.write(msg.value)
    print(f"📄 {nombre} ({msg.headers['content_type']}) — {len(msg.value)} bytes")
```

Con claves → orden por entidad (Día 02), headers → contexto (Día 02), commit manual → `msg.commit()` tras el write a disco (Día 06). Sin adivinar extensiones, sin inflado base64.

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `examples/07_files` (ejecutable, reproducible):** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io · MIT · Python 3.9→3.14 · mypy · loguru · tox

*¿Qué archivo enviaste alguna vez por Kafka como bytes crudos sin nombre cuando merecía un `file_name` bien tipado? Comentarios abiertos.*

#Kafka #FileStreaming #PDF #ZIP #Python #OpenSource #Wisrovi