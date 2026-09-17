### El offset automático de Kafka es una ficción cortés: "leído" ≠ "procesado". Día 06: el commit manual vuelve explícita la diferencia.

`auto_commit=True` dice "hecho" cuando aterriza el fetch — antes de tu handler, de tus efectos secundarios, de tu todo. Para operaciones financieras, inventario o pedidos, ese hueco es donde "at-least-once" se convierte en "perdí exactamente ese". WKafka Día 06 devuelve la decisión: `auto_commit=False` + `msg.commit()` te da **at-least-once tipado**, disfrutado con conciencia.

> Día 06, serie open-source de WKafka — basada en decoradores, MIT, reproducible.

## El único cambio

```python
kafka = WKafka(auto_commit=False)

@kafka.consumer(topic="financial_ops", format="json")
def on_op(msg):
    apply_operation(msg.value)      # el efecto secundario pasa PRIMERO
    msg.commit()                    # el "hecho" se declara DESPUÉS
```

**Garantía:** si el proceso muere entre `apply_operation` y `commit()`, el mensaje se reenvía al reiniciar. El offset avanza solo cuando tu código dice "procesado".

## Por qué es el at-least-once honesto

- El handler de ops financieras se re-ejecuta y commitea solo al final — "un cargo no ocurre dos veces" se vuelve defendible.
- El orden por clave (Día 02) sigue intacto; el commit viaja detrás del handler, no delante.
- Apagado limpio (Día 01) sin tocarlo: mismo decorador, mismo `block=True` — solo que la garantía ahora es de TU código.

## Verificado y reproducible

- **`08_manual_commit` ejecutable:** https://github.com/wisrovi/wkafka
- **PyPI:** https://pypi.org/project/wkafka
- **Docs:** https://wkafka.readthedocs.io
- MIT · Python 3.9→3.14 · mypy · loguru · tox multi-versión · 14+ ejemplos

*¿Cuántas veces te ha mentido auto_commit? Cuenta el ladrón de offsets en los comentarios…*

#Kafka #Python #AtLeastOnce #ExactlyOnce #OffsetCommit #DataEngineering #Streaming #Wisrovi
