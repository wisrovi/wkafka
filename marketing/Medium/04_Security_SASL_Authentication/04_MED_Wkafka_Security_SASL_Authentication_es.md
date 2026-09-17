# Tu Kafka es un servicio de red. A veces lo más aterrador que leerás hoy.

Un listener abierto en `:9092` sin auth no es "un detalle interno" — es una puerta sin llave entre tú y cualquiera que alcance el puerto. Los equipos de clústeres gestionados te entregan una config SASL y esperan en silencio que la conectes bien. Aquí WKafka trata la autenticación como un *contrato declarado*, no un misterio.

> Día 04 de la serie open-source de WKafka — basada en decoradores, MIT, reproducible.

## La config que solía ser un blob desordenado

Cada broker te da alguna versión de esto. En el lado consumidor pesa aún más — la cargas a todos lados:

```python
from wkafka import WKafka

kafka = WKafka(
    security_protocol="SASL_PLAINTEXT",   # ó SASL_SSL
    sasl_mechanism="PLAIN",               # ó SCRAM-SHA-256/512
    sasl_plain_username="external-user",
    sasl_plain_password=os.environ["KAFKA_PASSWORD"],
)
```

Un contrato de clúster en un solo objeto, resuelto una vez, tipado, validado en construcción — no tokens sueltos flotando por el código de servicio. Rota la contraseña en env, redeploy, listo.

## Ojo con el módulo SASL

El ejemplo `04_security_sasl` de WKafka corre un ciclo de auth completo sobre **SASL_PLAINTEXT** de punta a punta. Apunta el productor a un broker asegurado y produce/consume como siempre — la API por decorador no cambia. La autenticación es transporte, no interfaz.

## La regla de la que no fabricamos nada

> 🔐 **La autenticación no es una feature que agregas después.** Es lo primero que configuras cuando el broker no está en tu laptop. WKafka hace que eso sea una declaración tipada de una sola vez — y mantiene SCRAM-SHA-256/512 al alcance para el miedo legítimo a "contraseñas en tránsito".

## Pruébalo tú mismo

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub, `04_security_sasl` ejecutable sobre SASL_PLAINTEXT:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io
- **MIT, tipado (mypy), Python 3.9–3.14, loguru, tox**

*¿Cuál es el error de broker más enfadado que recibiste "solo probando en dev"? Cuéntanoslo en comentarios.*

#Kafka #Seguridad #SASL #Python #DataEngineering #OpenSource #Wisrovi