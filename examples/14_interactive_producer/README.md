# 🎮 Interactive CLI Producer Example (`14_interactive_producer`)

Este módulo implementa una consola interactiva de línea de comandos (CLI) que permite enviar mensajes arbitrarios (JSON o texto plano) a cualquier tópico de Kafka de manera dinámica y en tiempo real.

---

### 📖 Descripción

El Productor Interactivo es ideal para **depuración, pruebas manuales e inspección de servicios en tiempo real**. En lugar de programar valores fijos en un script, permite escribir directamente el contenido del mensaje y su clave desde la terminal.

---

### 🏃 Cómo Ejecutarlo

1. **Iniciar el Consumidor (Terminal 1)**:
   ```bash
   python consumer.py
   ```

2. **Ejecutar el Productor Interactivo (Terminal 2)**:
   ```bash
   python producer.py
   ```
   - Escribe el tópico destino o presiona `ENTER` para usar el tópico por defecto `interactive_topic`.
   - Escribe cualquier payload (ejemplo: `{"event": "ping", "user": "alice"}`).
   - Presiona `ENTER` para enviar.
   - Escribe `exit` o `quit` para cerrar el productor.

---

## 🛠️ Tecnologías y Librerías Relevantes

- **Python (3.9+)**: Entorno de ejecución principal e interfaz interactiva `input()`.
- **json**: Decodificación e inspección dinámica de payloads estructurados.
- **WKafka**: Transmisión de mensajería interactiva basada en decoradores y gestores de contexto.
- **Apache Kafka Broker**: Servidor de mensajería distribuido.
