## Deja de presuponer que Kafka "solo ordena". Los ganchos para orden REAL: clave + headers.

El gancho honesto de hoy: **casi nadie sabe cómo Kafka garantiza el orden de verdad**. Y esa ignorancia termina en bugs de "el evento llegó antes que el pedido". Esto es lo que aprendimos al construir el wrapper open-source WKafka (MIT, 14 ejemplos reproducibles).

**La verdad que nadie te cuenta en la diapositiva:**
Kafka no es una cola FIFO gigante. Es una cola ordenada **por partición**, en paralelo. Dos eventos de la misma entidad (un usuario, un pedido, un dispositivo) que caen en particiones distintas llegan en orden de volado.

**Cómo consigues orden REAL con WKafka:**

```python
from wkafka import WKafka
kafka = WKafka()

@kafka.consumer(topic="advanced_topic", format="json")
def on_advanced(msg):
    print(f"Key: {msg.key} | Headers: {msg.headers} | Value: {msg.value}")
```

**Detrás del gancho — las 2 primitivas que importan:**

1. **Clave (`key`)** → misma clave = misma partición = orden preservado. Es la *estrategia de enrutado*, no decoración:
   ```python
   p.send(topico="advanced_topic",
          value={"event_id": i, "status": "active"},
          key=f"user_{i}",                      # → misma partición → orden
          format="json")
   ```

2. **Headers** → metadatos que viajan gratis (trace ID, sistema origen, prioridad) sin tocar el payload:
   ```python
   headers={"source": "api_gateway",
            "correlation_id": f"corr_{i}"}
   ```

**El resultado:**
- ✅ **Orden por entidad** (per-user) garantizado por enrutado determinístico — una línea.
- ✅ **Correlación/contexto** via headers sin romper el esquema del value.
- ✅ Cero código de ordenamiento manual.

**Pruébalo tú mismo:**
- Repo + 14 ejemplos: https://github.com/wisrovi/wkafka
- PyPI: https://pypi.org/project/wkafka
- Docs: https://wkafka.readthedocs.io

*¿Qué bug de "desorden" en Kafka te ha costado una noche? Cuéntanoslo abajo — los comentarios son el mejor log de producción.*

#Kafka #Streaming #Python #DataEngineering #OpenSource #Wisrovi