# marketing/README.md — WKafka v1.0.0 LTS open-library marketing program.

## Qué es esto

Campaña de lanzamiento del wrapper **WKafka** (librería Python, MIT, decorator-based, Kafka).
20 días de contenido por plataforma, EN+ES por igual, en 6 plataformas: **Medium · Dev.to · Indie_Hackers · Reddit · LinkedIn · X**.

## Regla de oro (heredada de la serie wyoloservice2)

> **Nada inventado.** Cada hook se ancla a un hecho verificable del repositorio (README, examples/ o docs/).
> Prohibido inventar números (mAP, latencias, throughput), DOIs o telemetría que no exista: WKafka es una librería, no un paper.
> Los únicos "links" son los reales del repo: GitHub del proyecto y rutas de ejemplo específicas.

## Mapa día → hecho verificable (referente)

| Día | Feature real (examples/ o README) | Hecho medible/verificable |
|-----|-----------------------------------|---------------------------|
| 01 | `01_basic` — decorator consumer/producer | `@kafka.consumer(topic=...)`; config vía `KAFKA_SERVER` o `localhost:9092` |
| 02 | `02_advanced` — headers_and_keys | mensajes con clave + headers particionando determinísticamente |
| 03 | `03_multimedia` — images streaming | format="image" nativo para cuadros (OpenCV/NumPy/PIL) |
| 04 | `04_security_sasl` — SASL PLAIN/SCRAM | autenticación enterprise: SASL_PLAINTEXT (PLAIN, SCRAM) |
| 05 | `05_patterns` — microservice/worker | patrón worker independiente con consumer dedicado |
| 06 | `06_images` — image streaming | bytes→array sin boilerplate; serialización dedicada |
| 07 | `07_files` — file streaming | format="file" para PDF/ZIP/TXT — no solo texto |
| 08 | `08_manual_commit` — at-least-once | `auto_commit=False` + `msg.commit()`: control exacto de offsets |
| 09 | `09_retries_and_dlq` — resiliencia | retries con backoff exponencial + Dead Letter Queue |
| 10 | `10_multi_topic_regex` — suscripción | consumir múltiples topics vía regex sin código condicional |
| 11 | `11_async_handlers` — concurrency | handlers `async def` en hilos/event loop |
| 12 | `12_pydantic_validation` — schema | validación Pydantic en el mensaje de entrada |
| 13 | `13_partition_scale` — scaling | inspección `describe_topics` + autoscaling de particiones |
| 14 | `14_interactive_producer` — DevX | productor interactivo para pruebas manuales/demos |
| 15 | Core — multi-python | Compatible Python 3.9→3.14; CI tox multi-versión |
| 16 | Core — logging | Logging estructurado profesional vía `loguru` |
| 17 | Core — KRaft | Kafka sin Zookeeper (KRaft mode) soportado |
| 18 | Core — serialización | Serializadores dedicados (JSON, YAML, imágenes) |
| 19 | Core — LTS/estabilidad | v1.0.0 LTS, SemVer, refactor limpio (CHANGELOG) |
| 20 | Wrap-up | Todo el programa: 14 examples reproducibles + serie completa |

## Estructura de carpetas (por día)

```
marketing/
  Medium/01_basic/{01_MED_Basic_en.md,01_MED_Basic_es.md,title.txt}
  Dev.to/01_basic/{01_DEV_Basic_en.md,01_DEV_Basic_es.md,title.txt}
  Indie_Hackers/01_basic/{01_IH_Basic_en.md,01_IH_Basic_es.md,title.txt}
  Reddit/01_basic/01_RED_Basic_en.md+es.md
  LinkedIn/dia01/post_en.md+post_es.md+title.txt
  X/PROMO_X.md (una línea EN + una línea ES)
```

_Nombre slug de carpeta = nombre del example (01_basic, 02_advanced, …, 14_interactive_producer, 15_core_multipython, …)._

## Links reales (únicos permitidos)

- GitHub: https://github.com/wisrovi/wkafka
- (No existe DOI/Zenodo/PyPI para este repo — no enlazar lo que no exista. Verificar antes de agregar.)
