## ¿Reinventando la rueda en cada proyecto Kafka? Dilo una vez, ya está.

¿Cansado de copiar el loop del consumer, el switch de deserializadores y el try/except a cada servicio nuevo? Tras 20 días de la serie open-source de WKafka, este es el patrón que nos eliminó todo eso.

> Publicamos WKafka: un wrapper de Kafka para Python basado en decoradores (MIT, open source). Código > opiniones.

### Qué hicimos
Un decorador reemplaza todo el esqueleto del consumidor:

```python
from wkafka import WKafka

kafka = WKafka(dynamic_group_id=True)

@kafka.consumer(topic="basic_json", format="json")
def on_message(msg):
    print(f"Recibido: {msg.value}")
```

La config se resuelve desde `KAFKA_SERVER` (o `localhost:9092`), la serialización la maneja `format` y el apagado es limpio. Sin `while True`, sin commits manuales para arrancar.

### Números que podemos defender
- **Python 3.9 → 3.14** soportado (CI multi-versión con tox).
- **14 ejemplos reproducibles** en el repo, desde imágenes/PDF (OpenCV, NumPy, PIL) hasta auth SASL y control manual de offsets.
- **Licencia MIT**, tipado (mypy), logging con `loguru`, **API pública LTS**.

### Justificaciones (por qué Kafka ≠ ASCII crudo)
Serializadores nativos para JSON, YAML, imágenes, archivos y validación Pydantic — tus mensajes son objetos de negocio tipados, no sopa de bytes.

### Referencias
- Código: https://github.com/wisrovi/wkafka
- PyPI: https://pypi.org/project/wkafka
- Docs: https://wkafka.readthedocs.io

*¿Cuál es la feature de Kafka que quisieras que fuera de una línea? Cuéntanos abajo.*
