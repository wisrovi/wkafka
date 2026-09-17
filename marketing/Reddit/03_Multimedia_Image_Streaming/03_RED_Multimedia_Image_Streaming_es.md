Día 03 — tus frames de cámara NO son bytes crudos. Son mensajes con shape, dtype y canales.

Hoy en la serie WKafka: streaming de imágenes con `format="image"` — envías un `np.ndarray` (OpenCV/NumPy/PIL) y el consumidor lo recibe así, sin adivinar nada: shape/dtype/canales/orden viajan en el contrato, no en el cariño. Y se mantiene exactamente lo que ganaste el Día 02: orden por clave → orden por frame.

```python
from wkafka import WKafka
import numpy as np, cv2

kafka = WKafka(dynamic_group_id=True)

@kafka.consumer(topic="stream_images", format="image")
def on_frame(frame):
    print(f"shape={frame.shape}")   # (480, 640, 3) llega intacto
```

Por qué rompe el patrón "strings only" de Kafka:
- **Round-trip determinístico**: mismo serializador para enviar y recibir → cero "¿pero esto era BGR o RGB?"
- **Frames como primera clase**: el orden del producto ES el orden de los frames (claves + imagen combinados)
- **OpenCV/NumPy/PIL** interoperan sin pegamento — tu array es el mensaje, no un blob a desentrañar
- **14+ ejemplos reproducibles** (`03_multimedia` es un script)

Pruébalo:
- PyPI: https://pypi.org/project/wkafka
- GitHub: https://github.com/wisrovi/wkafka
- Docs: https://wkafka.readthedocs.io

*¿Qué tipo de dato NO textual estás forzando por Kafka como bytes sueltos? Lista honesta abajo.*

#Kafka #Python #ComputerVision #OpenCV #NumPy #Streaming #OpenSource #Wisrovi