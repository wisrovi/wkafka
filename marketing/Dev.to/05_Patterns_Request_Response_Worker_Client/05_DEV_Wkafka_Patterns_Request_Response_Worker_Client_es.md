# Kafka como bus de microservicios implica responder peticiones. WKafka tipa ese contrato.

Los eventos son solo la mitad del bus. La otra mitad es **request-response** — un worker que responde, un client que correlaciona. Hazlo mal y reinventas correlación, timeout y DLQ a la vez. El Día 05 de WKafka demuestra el trío tipado worker/client/microservice.

> Día 05 de la serie open-source de WKafka — acceso abierto, MIT, reproducible.

## El contrato sin correlación cableada a mano

El worker consume peticiones y responde en un topic de respuesta. Las claves mantienen el orden del peticionante; los headers llevan el round-trip:

```python
# worker.py
@kafka.consumer(topic="srpc_requests", format="json")
def on_request(req):
    result = compute(req.value)                                   # tu lógica, nada más
    kafka.producer_send(
        topic="srpc_responses",
        value=result,
        key=req.key,                                             # de vuelta a la entidad que preguntó
        headers={"correlation_id": req.headers["correlation_id"]},
        format="json",
    )

# client.py
p.send(topic="srpc_requests", value=pkt, key=f"user_{i}",
       headers={"correlation_id": f"corr_{i}"}, format="json")
```

El decorador consumidor es 100% Día-01: loop, serialización, orden por clave y apagado limpio sin cambios. La respuesta es solo una línea de productor dentro del handler — sin préstamo de threads, sin tabla de correlación.

## Por qué gana RPC-sobre-Kafka aquí

- **Un server por servicio es un despliegue, no una decisión del framework.** WKafka no construye un servidor HTTP embebido; mantiene el bus honesto: el worker maneja el topic de peticiones, la respuesta va al topic de respuestas, ambos tipados.
- **Orden por peticionante** (clave → partición): las respuestas vuelven en el orden de las peticiones.
- **`correlation_id` en header** hace el round-trip sin tocar el esquema (Día 02) — y la auth sigue siendo transporte (Día 04).

## El resultado

> 🎯 `05_patterns` = `worker.py` + `client.py` + `microservice.py` ejecutables. Mismo decorador, bidireccional, reproducible. RPC-sobre-Kafka, tipado.

## Pruébalo

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `05_patterns` trío ejecutable:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*¿Qué llamada preferirías hacer por Kafka antes que por HTTP? Los comentarios son el registro RPC honesto.*

#Kafka #Microservicios #Python #RPC #Streaming #OpenSource #Wisrovi