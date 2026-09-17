# Deja de escribir el boilerplate de Kafka en Python a mano. Un decorador es suficiente.

_Cuando tu consumer empieza a complicarse con loops `while True`, deserializadores manuales, gestión de offsets y `try/except` para cada partición, el código de infraestructura devora tu backlog. La mayoría de clientes Kafka te dejan cableando esa plomería cada vez._

> Día 01 de la serie open-research de WKafka — Acceso abierto, MIT, reproducible.

## Por qué importa

En WPipe ya aprendimos que la telemetría de los pipelines revela el coste real: el tiempo que pasas escribiendo el esqueleto del consumidor es tiempo que no inviertes en el mensaje. Un wrapper decorador reordena la ecuación: declaras el comportamiento, no la plomería.

## Lo que construimos

`WKafka` es un wrapper profesional de Kafka para Python basado en **decoradores**. Configuras clúster, serialización y validación de forma declarativa; el handler es una función normal:

```python
from wkafka import WKafka

kafka = WKafka(dynamic_group_id=True)

@kafka.consumer(topic="orders", format="json")
def on_order(msg):
    print(f"Nuevo pedido: {msg.value}")
```

Un solo decorador te da: deserialización automática (`format="json"`), resolución de configuración (lee `KAFKA_SERVER` o usa `localhost:9092` por defecto) y un apagado limpio. Ya no hay loops manuales ni commits a mano para arrancar.

## Más allá de los strings

Kafka no es solo ASCII. WKafka trae serializadores nativos para **JSON, YAML, imágenes (OpenCV/NumPy/PIL) y archivos (PDF, ZIP, TXT)** vía `format="file"`, además de **validación Pydantic** para payloads tipados y modo de streaming de imágenes/archivos. Corre en Python 3.9 a 3.14.

## El resultado

> 🎯 Un decorador, un handler, un topic. Menos plomería, más mensaje: el wrapper hace el trabajo pesado y tú te quedas con la lógica de negocio.

## Por qué encaja en tu stack

Es reusable, open source (MIT), con tipado estricto (mypy), logging profesional con `loguru` y tests multi-versión con `tox`. Un solo repo con 14+ ejemplos reproducibles y una API pública estable (LTS).

## Pruébalo tú mismo

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + 14+ examples reproducibles:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

---

*¿Cuántas líneas de boilerplate Kafka te ahorrarías con un decorador? Deja tu número en los comentarios.*

#Kafka #Python #Streaming #Decorators #OpenSource #DataEngineering #Wisrovi
