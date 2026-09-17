#### Mátalo con una declaración tipada, una sola vez: SASL en WKafka.

Listener abierto, auth cero, y el clásico copy-paste de staging a prod. El día que alguien fuera de tu org alcanza el `:9092`, "red interna" deja de ser un argumento. Aquí WKafka trata SASL como un contrato tipado, no como un ritual de copy-paste (Día 04 — serie open-source, MIT, reproducible).

```python
from wkafka import WKafka

kafka = WKafka(
    bootstrap_servers="localhost:30092",
    security_protocol="SASL_PLAINTEXT",      # ó SASL_SSL
    sasl_mechanism="SCRAM-SHA-256",          # también PLAIN / SCRAM-SHA-512
    sasl_plain_username="external-user",
    sasl_plain_password=os.getenv("KAFKA_PASSWORD"),   # nunca en código
    dynamic_group_id=True,
)
```

La ruta del consumidor queda 100% de Día 01 — decorador, orden por clave, apagado limpio. La auth es transporte, no lógica de handler.

Límites verificados: WKafka cablea SASL_PLAINTEXT + SASL_SSL, mecanismos PLAIN y SCRAM-SHA-256/512, en los decoradores de consumidor/productor. **No** dice gestionar tus certificados PKI (eso queda en la capa de TLS/platforma) y no hay métrica "enterprise security" inventada — solo el ejemplo `04_security_sasl` que prueba el ciclo de auth manual completo.

- PyPI: https://pypi.org/project/wkafka
- GitHub + `04_security_sasl`, round-trip SASL completo: https://github.com/wisrovi/wkafka
- Docs: https://wkafka.readthedocs.io

*¿Alguna vez pusheaste una contraseña de Kafka en un yaml commiteado? Todos pasamos por eso. Rincón de confesiones abajo.*

#Kafka #SASL #Security #SCRAM #Python #DataEngineering #OpenSource #Wisrovi