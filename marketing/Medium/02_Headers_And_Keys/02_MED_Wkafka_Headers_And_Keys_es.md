# Kafka solo garantiza orden si lo ganas tú — y tu clave es la moneda.

"Idempotente, ordenado, exactly-once" — dice la diapositiva de marketing. La versión cercana a la realidad: Kafka garantiza orden **por partición de tópico**, y solo si enrutas de forma determinística. WKafka te entrega las dos palancas que hacen real esa promesa: la **clave** y los **headers**.

> Día 02 de la serie open-source de WKafka — basado en decoradores, MIT, reproducible.

## El problema de "¡está ordenado!"

La mayoría asume que Kafka es una gran cola ordenada. No lo es. Es una cola ordenada por partición, en paralelo. Si dos mensajes de la misma entidad lógica (un usuario, un pedido, un dispositivo) caen en particiones distintas, el "orden" es un volado.

## Cómo lo gana WKafka

Las claves no son decoración — son la estrategia de enrutado. El productor lo muestra literalmente:

```python
with kafka.producer() as p:
    p.send(
        topic="advanced_topic",
        value={"event_id": i, "status": "active"},
        key=f"user_{i}",                 # <- misma clave → misma partición → orden intacto
        headers={"source": "api_gateway",
                 "correlation_id": f"corr_{i}"},
        format="json",
    )
```

Cada evento de `user_7` cae en la misma partición, en el mismo orden en que se envió. El consumidor los lee en ese orden — sin trabajo extra.

## Headers: metadatos que viajan gratis

El lado consumidor lee ambos lados del mensaje:

```python
print(f"Key: {msg.key}")
print(f"Headers: {msg.headers}")
```

Trace IDs, sistema origen, prioridad — metadatos de contexto que viajan junto al payload sin tocarlo. Perfectos para correlación entre servicios (piensa en tracing distribuido) sin romper el esquema del value.

## La conclusión

> 🎯 Enrutar por clave te da **orden por entidad**; los headers te dan **contexto buscable** sin inflar el esquema. Ambos son una línea en WKafka.

## Pruébalo tú mismo

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + 14 ejemplos reproducibles, `02_advanced`:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*¿Qué es lo más sorprendente de cómo Kafka ordena los mensajes en realidad? Compártelo abajo.*

#Kafka #Python #Streaming #DataEngineering #OpenSource #Wisrovi