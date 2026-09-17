### "Hasta los PDF y ZIP deberían viajar por Kafka?" — la respuesta no es un tweet, es un contrato de bytes.

Los archivos son el hijo multimillonario abandonado del streaming: la demo de Kafka "manda el archivo" muestras a bytes → la otra mitad del carruaje (que el consumidor sepa si es PDF malformado, ZIP corrupto o TXT truncado) se resuelve a lo luck. WKafka lo vuelve un **contrato tipado de bytes** con headers (Día 02) que viajan sobre el decorador del Día 01 — archivando el round-trip sin guesses.

> Día 07 — serie open-source WKafka, decorator-based, MIT, reproducible.

## Seal the deal con `format="file"`

Un PDF/PDF/zip/cualquier cosa es bytes + metadatos que la única forma de perder es no commitearlos. Producer:

```python
with kafka.producer() as p:
    with open("report_final.pdf", "rb") as f:
        pdf_bytes = f.read()
    p.send(topic="file_sharing_topic",
           value=pdf_bytes,
           key=f"report_{i}",
           headers={"file_name": "report_final.pdf",
                    "content_type": "application/pdf"},
           format="file")
```

## El otro lado: el nombre y el tipo NO se adivinan

WKafka entrona los bytes con su nombre real: deck, ZIP, PDF, script, imagen — el consumidor escribe un archivo real con su nombre y MIME correctos, no "recibí_bytes.bin":

```python
@kafka.consumer(topic="file_sharing_topic", format="file")
def on_file(msg):
    filename = msg.headers["file_name"]         # "factura_2024.pdf"
    content_type = msg.headers["content_type"] # "application/pdf"
    with open(filename, "wb") as f:
        f.write(msg.value)                     # round-trip bytes → disco
    print(f"📄 {filename} ({content_type}) — {len(msg.value)} bytes")
```

Round-trip: `<intacto?` → sí. Pdfs, ZIPs, TXTs, imágenes y scripts reproducibles con las claves/headers/offets de los Días 02/06 — y la orden por clave se preserva byte a byte.

## Pruébalo tú mismo

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + 14+ examples reproducibles (`07_files`):** https://github.com/wisrovi/wkafka?tab=readme-ov-file#examples
- **Docs:** https://wkafka.readthedocs.io
- MIT · Python 3.9→3.14 · mypy · loguru · tox multi-versión

*¿Qué archivo "para toda la vida" envíasTÚ por Kafka? PDF del contrato, backup de BD, ZIP del release… suelta la lista.*