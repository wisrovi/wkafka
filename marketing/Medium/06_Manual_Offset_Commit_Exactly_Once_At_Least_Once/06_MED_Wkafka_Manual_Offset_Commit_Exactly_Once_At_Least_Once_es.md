# Kafka solo te dice "recibido". El "procesado" es TU decisión — y el commit manual es donde se decide.

El auto-commit es la cortesía que te miente: Kafka marca el mensaje como "hecho" en cuanto **llega el fetch**, no cuando tu handler terminó. Para una operación financiera, un inventario o un pedido, esa diferencia es exactamente el hueco donde pasan los incidentes de 2am.

> Día 06 de la serie open-source de WKafka — decorator-based, MIT, reproducible.

## El problema del auto-commit

En la práctica de "recibir → procesar → commit" automático, Kafka confirma que el mensaje fue procesado *antes* de que tu lógica de negocio termine. El offset avanza; tus efectos secundarios, no. Cuando el proceso muere a mitad del handler, el clúster cree que "no hay nada pendiente" — y la mensaje quedó huérfano de warehouse.

## La solución honesta: `msg.commit()` cuando TÚ lo digas

```python
kafka = WKafka(
    auto_commit=False,          # 🚫 Kafka ya no decide "hecho" por ti
    format_policy="json",
    loguru_logging=True,        # 🕐 gor registro estructurado
)

@kafka.consumer(topic="financial_ops", format="json")
def on_transaction(msg):
    print(f"📥 Procesando {msg.value['transaction_id']} (offset {msg.offset})")
    result = process_business_logic(msg.value)     # tu lógica — efectos reales
    msg.commit()                                    # ✅ "hecho" SOLO aquí
    print(f"📤 Commit manual: offset {msg.offset} confirmado")
```

**At-Least-Once honesto:** el offset solo avanza después de que tu handler completó. Si el proceso muere entre `process_business_logic` y `msg.commit()`, el mensaje **se reprocesa** (idempotencia + claves del Día 02/05 siguen en pie) → nunca se pierde, nunca se duplica como "perdido".

## Por qué "exactly-once" no cabe en una promesa de 12 palabras

- At-most-once e at-least-once son **combinables en un solo handler**: `auto_commit=False` te da la palanca, `msg.commit()` la decisión.
- Pair con **retries + DLQ (Día 09)** si el offset manual falla por tercera vez — el mensaje va al dead-letter, no a la nada.
- `08_manual_commit` corre el ciclo completo: productor → consumidor → **offset manual** → "MANUAL COMMIT" en loguru — reproducible.

## Pruébalo tú mismo

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `08_manual_commit` (productor/consumidor listos para `poetry run`):** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*¿Cuántos offsets crees que has perdido por auto-commit en un año? Haz la cuenta y compártela — es la confesión más valiosa del thread.*

#Kafka #Python #DataEngineering #OffsetCommit #ExactlyOnce #Streaming #Wisrovi
