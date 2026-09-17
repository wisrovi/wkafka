### Deja de escribir el boilerplate de Kafka a pie. Un decorador es suficiente.

El detector de código se ejercita igual que el detector de accesos: cada `while True` del consumer y cada deserializador manual rayan el reposo. Del mismo modo que la librería fixa la plomería del cliente para que te quedes con el mensaje, escribimos el wrapper para Kafka.

**Día 01 de la serie WKafka — open source, MIT, reproducible.**

## El problema
Sin importar el tamaño del equipo, cada consumer termina con el mismo andamiaje: un loop manual, un switch de deserialización, una política de commit y un try/except que se traga la partición que estabas leyendo. Funciona — hasta que tienes diez.

## Cómo WKafka lo elimina
`WKafka` es un wrapper profesional de Kafka para Python basado en decoradores. Declaras el comportamiento, no la plomería:

```python
from wkafka import WKafka

kafka = WKafka(dynamic_group_id=True)

@kafka.consumer(topic="orders", format="json")
def on_order(msg):
    print(f"Nuevo pedido: {msg.value}")
```

Un solo decorador te da el loop del consumidor con serialización (`format`), la resolución de configuración (lee `KAFKA_SERVER` o usa `localhost:9092`) y un apagado limpio.

## Más allá de los strings
Kafka no es solo ASCII. WKafka trae serializadores nativos para JSON, YAML, imágenes (OpenCV/NumPy/PIL) y archivos (PDF, ZIP, TXT) vía `format="file"`, además de validación Pydantic. Corre en Python 3.9 a 3.14.

## El resultado
> Un decorador, un handler, un topic. Menos plomería, más mensaje: el wrapper hace el trabajo pesado y tú te quedas con la lógica de negocio.

## Pruébalo tú mismo
- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + 14 ejemplos reproducibles:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

---

*¿Cuántas líneas de boilerplate Kafka te ahorrarías con un decorador? Escríbelo en los comentarios.*

#Kafka #Python #Streaming #Decorators #OpenSource #DataEngineering #Wisrovi