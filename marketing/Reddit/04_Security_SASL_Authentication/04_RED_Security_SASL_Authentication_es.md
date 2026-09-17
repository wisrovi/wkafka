[P] Día 04 de WKafka — la auth SASL que dejaste de copiar mal. Open source, MIT, reproducible.

El gancho de hoy: la primera pregunta de seguridad de Kafka es "¿quién eres?" y el formato equivocado de la respuesta cuesta un servicio entero. SASL sobre SASL_PLAINTEXT (ó SASL_SSL) con PLAIN o SCRAM-SHA-256/512 es una declaración tipada de una sola vez en WKafka — no un snippet tatuado en cinco servicios.

```python
kafka = WKafka(
    bootstrap_servers="localhost:30092",
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username="external-user",
    sasl_plain_password=os.getenv("KAFKA_PASSWORD"),
)
```

Como la auth es transporte, el decorador consumidor queda exactamente con el contrato de Día 01 (loop, orden por clave, apagado limpio). Nada de contraseñas en código commiteado — env, tipado una vez, validado en construcción.

Verificado, nada inventado:
- SASL_PLAINTEXT y SASL_SSL; mecanismos PLAIN y SCRAM-SHA-256/512
- Python 3.9 → 3.14, mypy estricto, loguru, tox multi-versión, MIT
- `04_security_sasl` ejecuta un ciclo completo de auth SASL manual — reproducible
- Links: PyPI https://pypi.org/project/wkafka · GitHub https://github.com/wisrovi/wkafka · Docs https://wkafka.readthedocs.io

*¿Qué config de auth de Kafka hardcodeada has visto que te hiciera encoger?*