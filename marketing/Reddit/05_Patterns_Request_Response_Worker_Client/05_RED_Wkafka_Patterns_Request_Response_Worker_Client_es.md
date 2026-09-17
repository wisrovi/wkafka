### "Event-driven" se lee genial en la diapositiva. Hasta que un servicio necesita *una respuesta* — y Kafka se le queda mirando.

El 80% de los demos de "Kafka como bus" enseñan eventos fire-and-forget. La mitad incómoda que nadie demuestra: **request-response** — un worker que responde, un client que correlaciona. Hazlo a mano y reinventas la tabla de correlación, el timeout y el DLQ a la vez. WKafka Día 05 lo expone como contrato tipado, reproducible, MIT.

> Día 05 de la serie open-source de WKafka — MIT, reproducible, 14+ ejemplos.

## Un worker que responde (no solo consume)

Ambas caras usan el decodificador del Día 01 — loop, serialización, orden por clave, apagado limpio. La respuesta es un productor una sola vez desde dentro del handler:

```python
@kafka.consumer(topic="srpc_requests", format="json")
def on_request(req):
    result = compute(req.value)
    kafka.producer_send(
        topic="srpc_responses",
        value=result,
        key=req.key,                     # clave → mismo peticionante → orden coherente
        headers={"correlation_id": req.headers["correlation_id"]},
        format="json",
    )
```

## El cliente: una llamada, correlación que viaja

```python
with kafka.producer() as p:
    p.send(topic="srpc_requests",
           value={"op": "compute", "payload": req},
           key=f"client_{i}",
           headers={"correlation_id": f"corr_{i}"},
           format="json")
```

## Por qué RPC-sobre-Kafka y no tuerca HTTP

- **Contrato tipado bidireccional**: worker y client son el mismo contrato Día 01, no "POST y cruza los dedos por un 200".
- **Claves preservan el orden por peticionante** (Día 02); el header `correlation_id` (Días 02/04) hace el round-trip sin tocar el esquema.
- **Cero hilos extra**: la respuesta sale del propio handler; el cliente sigue siendo un consumidor por decorador.

## Verificado y reproducible

- **`05_patterns` = worker + client + microservice ejecutable:** https://github.com/wisrovi/wkafka
- **PyPI:** https://pypi.org/project/wkafka
- **Docs:** https://wkafka.readthedocs.io
- Python 3.9 → 3.14, mypy, loguru, tox, MIT, 14+ examples

*¿Qué servicio desearías que te respondiera por Kafka en lugar de una llamada HTTP? Lista honesta en comentarios.*

#Kafka #RPC #Microservicios #Python #Streaming #OpenSource #Wisrovi