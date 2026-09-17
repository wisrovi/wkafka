### "Es nuestra red interna, nadie llega a :9092." — dijeron todos los equipos antes de la sorpresa del demo day.

SASL/SCRAM es la diferencia entre "Kafka es un servicio" y "Kafka es un pasillo sin puerta". El Día 04 de WKafka trata la autenticación como una declaración tipada, de una sola vez — no un ritual que copias entre servicios.

**El contrato tipado que reemplaza el snippet de auth:**

```python
from wkafka import WKafka

kafka = WKafka(
    bootstrap_servers="localhost:30092",
    security_protocol="SASL_PLAINTEXT",      # o SASL_SSL
    sasl_mechanism="SCRAM-SHA-256",          # también PLAIN / SCRAM-SHA-512
    sasl_plain_username="external-user",
    sasl_plain_password=os.getenv("KAFKA_PASSWORD"),   # credenciales en env, nunca en código
)
```

**Por qué elimina toda una clase de incidentes:**
- La contraseña nunca aterriza en un archivo commiteado — se inyecta por env y se resuelve una sola vez al construir.
- Un typo en el mecanismo (`SCRAM-SHA-25` → error de validación) falla en build time, no a las 2 am después de un rebatch.
- El decorador de consumidor sigue siendo 100% Día-01 — la auth es transporte, no fuga de lógica del handler.

**Verificado (del repo real, `04_security_sasl`):**
- SASL_PLAINTEXT y SASL_SSL; mecanismos PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
- `04_security_sasl` es un ciclo completo de auth SASL de punta a punta — reproducible
- Python 3.9 → 3.14, mypy estricto, loguru, tox multi-versión, MIT
- 14+ ejemplos, API LTS, cero métricas inventadas — solo el código reproducible

**Recursos:**
- PyPI: https://pypi.org/project/wkafka
- GitHub con `04_security_sasl` ejecutable: https://github.com/wisrovi/wkafka
- Docs: https://wkafka.readthedocs.io

*Si alguna vez subiste una contraseña de Kafka en un config commiteado, este es el sexto espacio seguro para admitirlo. No juzgamos — arreglamos el wrapper.*

#Kafka #SASL #Seguridad #SCRAM #Python #DataEngineering #OpenSource #Wisrovi