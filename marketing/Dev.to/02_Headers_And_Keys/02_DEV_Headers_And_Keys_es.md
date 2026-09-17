# Kafka solo garantiza orden si lo ganas tú — la clave es la moneda

"Exactly-once, ordenado, idempotente." Excelente diapositiva de marketing. La versión que puedes llevar a producción: Kafka preserva el orden **por partición de tópico**, y solo cuando enrutas de forma determinística. Este post muestra las dos primitivas de WKafka que convierten esa promesa en comportamiento: **claves** y **headers**.

> Día 02 de la serie open-research de WKafka — open source, MIT, reproducible.

## La trampa del "ordenado"

La gente trata a Kafka como una gran cola FIFO. No lo es. Es una cola ordenada *por partición*, corriendo en paralelo. Dos mensajes para la misma entidad de negocio (un usuario, un pedido, un dispositivo) que caen en particiones distintas van en orden de volado — aunque cada partición por separado se vea perfectamente secuencial.

## Claves: enrutado determinístico que se ve

Las claves no son decoración; son la estrategia de enrutado. Misma clave → misma partición → mismo orden. WKafka lo hace explícito:

```python
with kafka.producer() as p:
    p.send(
        topic="advanced_topic",
        value={"event_id": i, "status": "active"},
        key=f"user_{i}",          # misma clave → misma partición → orden preservado
        headers={"source": "api_gateway",
                 "correlation_id": f"corr_{i}"},
        format="json",
    )
```

Cada evento de `user_7` cae en la misma partición, en el orden exacto en que se envió. El consumidor los lee secuencialmente — cero trabajo extra.

## Headers: metadatos que viajan gratis

Del lado consumidor lees ambas caras del mensaje:

```python
print(f"Key: {msg.key}")
print(f"Headers: {msg.headers}")
```

Trace IDs, sistema de origen, prioridad — metadatos de contexto que viajan con el mensaje sin tocar el payload. Ideales para correlación entre servicios (piensa en trazas distribuidas) sin romper el esquema del value.

## La conclusión

> 🎯 Las claves te dan **orden por entidad**; los headers te dan **contexto buscable** sin inflar el esquema. Ambos son una línea en WKafka.

## Pruébalo tú mismo

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + 14 ejemplos reproducibles, `02_advanced`:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*¿Qué es lo más sorprendente de cómo Kafka ordena realmente los mensajes? Déjalo en los comentarios.*

#Kafka #Python #Streaming #DataEngineering #OpenSource #Wisrovi
