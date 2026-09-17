### Kafka solo está "ordenado" si lo ganas tú — claves y headers: los dos mecanismos que cuestan una línea

La diapositiva de un vendor dice "ordered, exactly-once, idempotent". La versión que puedes defender ante una auditoría: Kafka está ordenado **por partición**, y solo si enrutas de forma determinstica. Hoy, en el Día 02 de la serie WKafka, los dos mecanismos que hacen real esa promesa — y cuestan una línea cada uno.

## Qué es
WKafka es un wrapper de Kafka para Python basado en **decoradores** (MIT, open source, tipado estricto). Obtienes orden real por entidad y metadatos portátiles sin tocar el esquema del payload.

**1) Claves — el enrutado determinístico**

Misma clave → misma partición → mismo orden. Punto. Es la única forma de conseguir orden *por entidad* en Kafka, y en WKafka es un argumento:

```python
with kafka.producer() as p:
    p.send(
        topic="advanced_topic",
        value={"event_id": i, "status": "active"},
        key=f"user_{i}",                 # ← misma clave → misma partición → orden preservado
        headers={"correlation_id": f"corr_{i}"},
        format="json",
    )
```

**2) Headers — metadatos que viajan gratis**

Del lado consumidor lees ambas caras del mensaje:

```python
print(f"Key: {msg.key}")
print(f"Headers: {msg.headers}")
```

Trace IDs, sistema origen, prioridad — contexto buscable que viaja con el mensaje **sin inflar el payload**. Ideal para correlación entre servicios (trazas distribuidas) sin romper el esquema del value.

## Por qué importa

> 🎯 Claves = **orden por entidad** sin polling ni sorting. Headers = **contexto buscable** sin tocar el esquema. Ambos, una línea en WKafka.

## Lo verificable
- Python 3.9 → 3.14, MIT, mypy estricto, loguru, tox multi-versión
- 14+ ejemplos reproducibles (`02_advanced` incluido), API LTS estable
- Un solo repo, sin inventar números ni DOIs — solo el repo real

## Pruébalo tú mismo
- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + 14 ejemplos reproducibles, `02_advanced`:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

---

*¿Cuál fue la cosa más sorprendente que aprendiste sobre cómo Kafka realmente ordena? Escríbelo abajo.*

#Kafka #Python #Streaming #DataEngineering #OpenSource #Wisrovi