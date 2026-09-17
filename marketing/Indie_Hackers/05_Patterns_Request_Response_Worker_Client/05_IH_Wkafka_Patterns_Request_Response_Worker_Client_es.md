### Deja de tratar Kafka como eventos de una sola dirección. La otra mitad es request-response — WKafka tipa el round-trip.

La mayoría de demos de "Kafka como bus" se quedan en fire-and-forget. La mitad incómoda que nadie demuestra: un worker que *responde*. Así que el ejemplo Día-05 de WKafka es el trío honesto: worker ↔ client ↔ microservicio, request-response sobre Kafka, mismo decorador Día-01 en ambas caras.

```python
# worker.py — consume la petición, responde por el mismo bus
@kafka.consumer(topic="srpc_requests", format="json")
def on_request(req):
    result = compute(req.value)                       # tu lógica
    producer.send(topic="srpc_responses",
                  value=result,
                  key=req.key,                        # misma clave → mismo cliente → misma partición
                  headers={"correlation_id": req.headers["correlation_id"]},
                  format="json")
```

```python
# client.py — envía y deja que clave/header hagan la correlación
with kafka.producer() as p:
    p.send(topic="srpc_requests",
           value={"op": "compute", "payload": req},
           key=f"client_{i}",
           headers={"correlation_id": f"corr_{i}"},
           format="json")
```

**Por qué RPC-sobre-Kafka en vez de pegamento HTTP:**
- Worker y client son **contratos tipados**, no "llama POST y cruza los dedos por un 200". El decorador es el mismo del Día 01: loop, serialización, orden por clave, apagado limpio.
- **Claves** preservan el orden del peticionante (Día 02); el **header `correlation_id`** lleva la traza de ida y vuelta (headers del Día 01, auth del Día 04) sin tocar el esquema.
- Reproducible: 14+ ejemplos, `05_patterns` corre el trío worker+client+microservice completo.

**Pruébalo:**
- PyPI: https://pypi.org/project/wkafka
- GitHub + `05_patterns`: https://github.com/wisrovi/wkafka
- Docs: https://wkafka.readthedocs.io

*¿Qué servicio desearías que respondiera por Kafka en vez de una llamada REST? Deja la lista.*

#Kafka #RPC #Microservicios #Python #Streaming #OpenSource #Wisrovi
