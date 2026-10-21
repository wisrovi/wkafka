# Tareas Pesadas de Larga Duración y Workers en Background en WKafka ⚡

Este ejemplo demuestra cómo procesar cargas de trabajo de cómputo intensivo o de larga duración (como inferencias de modelos de IA, transcodificación de vídeo, resumen de miles de fotogramas o generación de documentos PDF) sin bloquear el bucle de consumo de Kafka ni provocar desconexiones por vencimiento de `max_poll_interval_ms`.

---

## 🎯 El Problema que Resuelve

En Apache Kafka, si un callback de consumo tarda más de 5 minutos (el valor estándar de `max_poll_interval_ms`), el clúster determina que el consumidor se ha congelado o desconectado, desencadena un rebalanceo del grupo de consumo y reenvía el mismo mensaje una y otra vez.

**WKafka** soluciona esto de manera elegante mediante dos parámetros nativos:
* **`commit_on_receive=True`**: Confirma el offset ante el broker de Kafka en el instante exacto en que llega el mensaje (garantía at-most-once / confirmación temprana).
* **`background=True`**: Despacha la ejecución del callback a un pool de workers desacoplados (`ThreadPoolExecutor`).
* **`background_workers=N`**: Permite definir cuántas tareas pesadas pueden ejecutarse simultáneamente en segundo plano.

Mientras los workers procesan la lógica pesada, el hilo del consumidor sigue respondiendo inmediatamente a los heartbeats de Kafka y ejecutando `poll()` de forma continua y saludable.

---

## 🚀 Cómo Ejecutar

1. **Inicia el Consumidor (Terminal 1):**
   ```bash
   poetry run python examples/15_long_running_background_tasks/consumer.py
   ```

2. **Inicia el Productor (Terminal 2):**
   ```bash
   poetry run python examples/15_long_running_background_tasks/producer.py
   ```

---

## 🛠️ Tecnologías y Librerías Relevantes

- **WKafka**: Framework Python con soporte nativo para workers en segundo plano y confirmación inmediata de offsets.
- **Apache Kafka**: Plataforma distribuida de mensajería y eventos en tiempo real.
- **Python ThreadPoolExecutor**: Gestión de hilos de ejecución concurrentes desacoplados.
- **Loguru**: Sistema estructurado de logging y trazabilidad de eventos.
