### "Tú decides cuándo hecho es hecho" — y Kafka te llama mentiroso si auto-committeas antes.

WKafka Día 06 prueba lo que el Día-08 manual_commit hace de verdad: at-least-once, offset que TU handler mueve cuando TU negocio terminó. auto_commit=False + msg.commit() en el momento exacto. MIT, reproducible, decorador idéntico al Día-01.

```python
kafka = WKafka(auto_commit=False)

@kafka.consumer(topic="financial_ops", format="json")
def on_financial(msg):
    apply_ledger(msg.value)          # 1. el efecto SE HACE
    msg.commit()                     # 2. recién acá "hecho" significa hecho
```

- Python 3.9→3.14 · mypy estricto · loguru · tox multi-versión
- PyPI: https://pypi.org/project/wkafka
- GitHub + 14 ejemplos reproducibles: https://github.com/wisrovi/wkafka
- Docs: https://wkafka.readthedocs.io

*¿Cuántos mensajes "procesados" crees que tu auto-commit dejó en el limbo el año pasado? Te escucho.*
