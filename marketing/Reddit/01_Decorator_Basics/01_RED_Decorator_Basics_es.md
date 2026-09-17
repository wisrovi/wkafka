[P] Publicamos un wrapper de Kafka para Python (MIT) que elimina casi todo el boilerplate del consumer con un solo decorador — 14 ejemplos reproducibles incluidos

Soy el mantenedor de WKafka, un wrapper de Kafka basado en decoradores, open source (MIT). Después de construir pipelines sobre telemetría cruda, la misma lección se repetía para el mensajero: la mayor parte del código de un consumer es andamiaje, no lógica de negocio.

El gancho: un decorador reemplaza el loop del consumer, la deserialización y el apagado.

```python
from wkafka import WKafka

kafka = WKafka(dynamic_group_id=True)

@kafka.consumer(topic="basic_json", format="json")
def on_message(msg):
    print(f"Recibido: {msg.value}")
```

Por qué merece la pena mirarlo más allá de la demo:
- Serializadores nativos para JSON, YAML, imágenes (OpenCV/NumPy/PIL), archivos (PDF, ZIP, TXT) y validación Pydantic
- Auth SASL PLAIN/SCRAM, soporte KRaft, commit manual de offsets, retries + DLQ, handlers asíncronos
- Corre en Python 3.9 a 3.14, tipado estricto (mypy), logging con loguru, CI multi-versión (tox)
- 14 ejemplos reproducibles en el repo, licencia MIT
- API pública estable (LTS), v1.0.0 desde mayo 2024

Quiero feedback real sobre la forma de la API. ¿Qué workflow de Kafka todavía no puedes expresar de forma declarativa? Esa es mi siguiente iteración.

Repo: https://github.com/wisrovi/wkafka  |  PyPI: https://pypi.org/project/wkafka