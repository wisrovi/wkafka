# La caída de Kafka más cara no es la del broker — es la del listener sin auth que nadie nota hasta el demo day.

Un `:9092` abierto sin SASL no es "un detalle interno", es un cable sin handshake entre tú y cualquiera que alcance el puerto. Los brokers gestionados te pasan la config de auth y esperan en silencio que no desparrames tokens. Hoy: WKafka convierte la auth en una declaración tipada única, y muestra un round-trip SASL_PLAINTEXT completo en un ejemplo ejecutable.

> Día 04, serie open-source de WKafka — basada en decoradores, MIT, reproducible.

## La config que casi todos pegan mal una vez

La versión de conocedor, ambas caras — productor *y* consumidor tras el mismo contrato:

```python
kafka = WKafka(
    bootstrap_servers="localhost:30092",
    security_protocol="SASL_PLAINTEXT",   # ó SASL_SSL con TLS
    sasl_mechanism="PLAIN",               # ó SCRAM-SHA-256 / SCRAM-SHA-512
    sasl_plain_username="external-user",
    sasl_plain_password=os.getenv("KAFKA_PASSWORD"),
)
```

Tres victorias frente al config disperso:
1. **Un objeto, resuelto y validado una vez** — un typo en el mechanism falla en construcción, no a las 2am.
2. **La contraseña nunca en código.** Viene de env, así rotar es un redeploy, no un cambio de código.
3. **La API por decorador no cambia.** La auth es transporte; tu `@kafka.consumer(...)` queda exactamente igual (contrato del Día 01 intacto).

## Límite honesto

WKafka cablea SASL PLAIN y SCRAM (SHA-256/512), SASL_PLAINTEXT y SASL_SSL. Lo que **no** hace es volverte type-aware de PKI: la gestión de certificados TLS sigue siendo trabajo de la plataforma — el wrapper la respeta, no la reinventa.

## Pruébalo

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub, `04_security_sasl` corre un ciclo de auth completo sobre SASL_PLAINTEXT:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*¿Qué secreto de Kafka dejaste una vez en un archivo commiteado? Todos hemos pasado por ahí — confiésalo abajo, esto es un espacio seguro.*

#Kafka #Seguridad #SASL #SCRAM #Python #DataEngineering #OpenSource #Wisrovi