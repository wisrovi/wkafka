# Kafka como bus de microservicios significa que vas a responder peticiones. Mejor tipea ese contrato.

La mayoría de tutoriales de "Kafka como bus" muestran eventos fire-and-forget. La otra mitad del bus es *request-response* — y si lo atornillas a la API de eventos sin pensar, reinventas la correlación, el timeout y el DLQ a la vez. WKafka lo expone como el patrón reproducible del Día 05: un contrato tipado de trio client ↔ worker.

> Día 05 de la serie open-source de WKafka — basado en decoradores, MIT, reproducible.

## La forma request-response, sin correlación cableada a mano

Un worker responde a un topic de peticiones; el origen (client) lo sigue con claves + headers `correlation_id` (Día 02 + Día 04, ambos siguen en pie):

```python
# microservice.py — ambas caras en un módulo tipado
@kafka.consumer(topic="srpc_requests", format="json")
def on_request(req):
    # misma clave → misma partición → ordenado por peticionante
    result = process_payload(req.value)
    producer.send(topic="srpc_responses",
                  value=result,
                  key=req.key,                  # ruta de vuelta a la entidad que preguntó
                  headers={"correlation_id": req.headers["correlation_id"]})

# client.py — envía + correlaciona, una llamada
with kafka.producer() as p:
    p.send(topic="srpc_requests",
           value={"op": "compute", "payload": ...},
           key=f"user_{i}",
           headers={"correlation_id": f"corr_{i}"})
```

El decorador consumidor sigue siendo 100% Día-01: loop, serialización, orden por clave, apagado limpio. La respuesta es solo un write de productor *desde dentro del handler* — sin thread extra, sin tabla de correlación manual.

## Por qué merece su propio día

- **Reproduce el modelo mental de RPC**: llamar al servicio A desde el servicio B sigue siendo una llamada, no "lanzar un evento y esperar".
- **Orden por peticionante** (clave → partición): las respuestas de cada caller vuelven en el orden de las peticiones — sin reordenar en el client.
- **`correlation_id` en headers** recorre el round-trip sin tocar el esquema — trázalo entre servicios (Día 02 y Día 04 ya conectados).

## El resultado

> 🎯 `05_patterns` es un trío worker/client/microservice ejecutable. Mismo decorador, mismo contrato tipado, bidireccional, reproducible — el RPC-sobre-Kafka que realmente querías.

## Pruébalo tú mismo

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `05_patterns` (trío worker/client/microservice):** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*¿Qué petición preferirías responder por Kafka en lugar de una llamada HTTP? Me interesa tu versión de "los eventos son la forma equivocada aquí".*

#Kafka #Microservicios #RPC #Streaming #Python #OpenSource #Wisrovi