### El orden en Kafka no es gratis — se gana con claves y se enriquece con headers.

El gancho de las diapositivas, escrito en una sola línea: Kafka garantiza orden **por partición** — en paralelo, no como una sola cola FIFO. Dos eventos de la misma entidad que caen en particiones distintas llegan en orden de cara o cruz. La corrección no es una feature del framework: es una estrategia de enrutado que eliges tú.

**En WKafka, un argumento para cada palanca:**

```python
with kafka.producer() as p:
    p.send(
        topic="advanced_topic",
        value={"event_id": i, "status": "active"},
        key=f"user_{i}",                 # misma clave → misma partición → orden preservado
        headers={"correlation_id": f"corr_{i}"},
        format="json",
    )
```

- **Clave** → enrutado determinístico → mismo orden por entidad.
- **Headers** → metadatos que viajan gratis (trace ID, sistema origen, prioridad) sin inflar el payload ni tocar el esquema; el consumidor los lee así:

```python
print(f"Key: {msg.key}")
print(f"Headers: {msg.headers}")
```

**Por qué construimos WKafka**
- Wrapper decorator-based de Kafka para Python, MIT, open source
- Python 3.9 → 3.14, mypy estricto, loguru, tox multi-versión
- 14+ ejemplos reproducibles, API pública LTS estable
- Cero números inventados — solo el repo real con 14 ejemplos que corren

**Pruébalo tú mismo:**
- PyPI: https://pypi.org/project/wkafka
- GitHub + 14 ejemplos reproducibles: https://github.com/wisrovi/wkafka
- Docs: https://wkafka.readthedocs.io

---

*¿Qué suposición de orden en Kafka sigue sin revisar en tu código? Esa es la que hay que verificar esta semana.*