### Kafka no opina sobre tu imagen. `format="image"` sí.

Streaming no es solo ASCII — para equipos que mandan datos de visión son frames: teselas de satélite, inspecciones de línea de montaje, muros de cámaras. Enviarlos como "bytes crudos" exporta la adivinanza de decoding a cada consumidor. El gancho de hoy en WKafka: que la **imagen** sea el mensaje, no los bytes.

```python
frame = np.zeros((480, 640, 3), dtype=np.uint8)
cv2.circle(frame, (320, 240), 100, (0, 0, 255), -1)

with kafka.producer() as p:
    p.send(topic="stream_images", value=frame, format="image", quality=90)

@kafka.consumer(topic="stream_images", format="image")
def on_frame(image):
    print(f"shape={image.shape}")   # (480, 640, 3) — round-trip intacto
```

**Por qué elimina código:** un solo serializador codifica y decodifica (sin adivinar shape/dtype/canales), los frames mantienen el orden por entidad de las claves, y el array interopera con OpenCV/NumPy/PIL tal cual. Mismo decorador, mismo apagado que tus handlers JSON.

**Pruébalo:**
- PyPI: https://pypi.org/project/wkafka
- GitHub + `03_multimedia` reproducible: https://github.com/wisrovi/wkafka
- Docs: https://wkafka.readthedocs.io

*¿Qué payload no textual has enviado por Kafka como bytes crudos? Dinos cuál abajo.*

#Kafka #ComputerVision #OpenCV #Python #Streaming #Wisrovi