### "Que Kafka mande el PDF" no es un estribillo — es un contrato de bytes con nombre y tipo.

Los archivos son el corpus millonario olvidado del streaming: todos los weeklies "Kafka para archivos" mandan el bytes y dejan la otra mitad del problema al consumidor — ¿PDF malformado, ZIP corrupto, TXT truncado? Adivina. WKafka los convierte en **mensajes tipados por headers** (`file_name`+`content_type`, Día 02 vivos) sobre el decorador del Día 01 — bytes intactos, round-trip honesto, reproducible.

> Día 07 — serie open-source WKafka, decorator-based, MIT.

## El contrato: `format="file"` en ambos lados

Productor con metadatos de archivo viajando en headers:

```python
with kafka.producer() as p:
    p.send(topic="file_sharing_topic",
           value=pdf_bytes,                     # PDF/ZIP/TXT/cualquier bytes
           key=f"report_{i}",
           headers={"file_name": "factura_final.pdf",
                    "content_type": "application/pdf"},
           format="file")
```

Consumidor que recupera el archivo real, no "recibí_bytes.bin":

```python
@kafka.consumer(topic="file_sharing_topic", format="file")
def on_file(msg):
    name    = msg.headers["file_name"]
    ctype   = msg.headers["content_type"]
    with open(name, "wb") as f:
        f.write(msg.value)
    print(f"📄 {name} ({ctype}) — {len(msg.value)} bytes")
```

## Por qué es el "flecos que sí se reproduce"

- **Bytes intactos round-trip**: igual dejaOS que el Día-03 con imágenes; ahora el value es un archivo entero (PDF/ZIP/TXT/deck).
- **Order por clave (Día 02)** se mantiene — el `key` de la factura es la moneda del orden.
- **Commit manual (Día 06)** compatible: `msg.commit()` cuando escribiste a disco, no antes.
- **MIT · Python 3.9→3.14 · mypy · loguru · tox · 14+ examples reproducibles**

## Pruébalo tú mismo

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `07_files` ejecutable:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*¿Qué archivo de producción te gustaría nombrar y tipar de verdad — y no "message.body"? La lista honesta en comentarios.*

#Kafka #Files #Streaming #PDF #Python #DataEngineering #OpenSource #Wisrovi
