# WKafka — WHEN TO USE (Guía del Agente)

Documento de decisión para agentes: **cuándo** usar WKafka y **cuál** feature usar.

---

## ¿Cuándo usar WKafka?

Usa **WKafka** cuando necesites:

- **Comunicación asíncrona entre servicios/microservicios** vía Apache Kafka.
- **Event sourcing / integración de eventos** (órdenes, pagos, telemetría, logs).
- **Streaming de multimedia (imágenes/video)** entre cámaras, workers y modelos de IA.
- **Desacoplar productores de consumidores** con replay y retención de mensajes.
- **Múltiples consumidores concurrentes** sobre el mismo topic (consumer groups).

> NO uses WKafka para caché de funciones ni estado compartido en memoria (usa WRedis),
> ni para persistencia relacional local (usa WSQLite). WKafka es la capa de eventos
> distribuidos y duraderos, no una base de datos.

---

## Tabla de decisión: ¿cuál feature de WKafka usar?

| Necesidad | Feature | API |
|---|---|---|
| Mensajes JSON | JSON | `@kafka.consumer(topic, format="json")` + `p.send(topic, value=..., format="json")` |
| Configuración YAML | YAML | `format="yaml"` |
| Imágenes / frames de video | Image | `format="image"` (NumPy/PIL, JPEG, `quality=`) |
| Autenticación SASL | SASL | `security_protocol="SASL_PLAINTEXT"`, `sasl_mechanism`, `sasl_plain_username/password` |
| Kafka sin Zookeeper | KRaft | `bootstrap_servers` apuntando a controllers KRaft |
| Procesar solo ciertas keys | Key filter | `@kafka.consumer(topic, key_filter="payments")` |
| Trazabilidad/metadata | Headers | `p.send(topic, headers={"trace_id": "...", "tenant": "..."})` |
| Vários consumers a la vez | Threads | Decorar varios handlers + `kafka.run_consumers(block=True)` |
| Grupo nuevo por ejecución | Dynamic group | `WKafka(dynamic_group_id=True)` |
| Compresión | Compression | `pip install wkafka[snappy]` (fallback gzip automático) |
| Serialización custom | Serializer ABC | Subclase de `wkafka.serializers.base.Serializer` |
| API legacy | Controller bridge | `from wkafka.controller.wkafka import Wkafka` (`server`, `name`, `retry_delay`, `max_retries`, `value_type`) |

---

## Patrón arquitectónico estándar

```
Servicio A  →  producers/*.py (with kafka.producer() as p: p.send(...))  →  [Kafka topic]
Servicio B  →  consumers/*.py (@kafka.consumer(topic, format=...))  →  main.py → run_consumers(block=True)
```

Reglas:
1. **Config centralizada** en `config/settings.py` (env-driven, sin credenciales hardcodeadas).
2. **Un handler por consumer** decorado con `@kafka.consumer(...)`.
3. **`with kafka.producer() as p:`** para producción segura (flush + close automáticos).
4. **`client_id` único** por servicio; `dynamic_group_id=True` cuando cada run necesita grupo fresco.
5. **Logging loguru** automático a `wkafka.log` (rotación 10 MB) con `logger.exception` en callbacks.

---

## Instalación

```bash
pip install wkafka
# con compresión snappy:
pip install wkafka[snappy]
```

---

*Generado por WKafka MCP by wisrovi.*
