# Enviar un frame (no un byte) es una decisión de API. WKafka lo resuelve con `format="image"`.

Kafka entrega bytes. No le importa si son un contrato, una foto de gato o un frame de satélite. Para un equipo que transmite datos de visión, "bytes crudos" significa que cada consumidor reimplementa decoding, adivinanza de shape y manejo de canales. La respuesta de WKafka es un contrato que la librería hace cumplir: las imágenes son un mensaje de primera clase.

> Día 03 de la serie open-source de WKafka — basado en decoradores, MIT, reproducible.

## Qué significa "imagen" como formato

El productor envía un array de NumPy, no un blob:

```python
frame = np.zeros((480, 640, 3), dtype=np.uint8)   # lienzo BGR
cv2.circle(frame, (320, 240), 100, (0, 0, 255), -1)

with kafka.producer() as p:
    p.send(
        topic="stream_images",
        value=frame,
        format="image",        # serializador: array → JPEG (quality) → bytes
        quality=90,
    )
```

El consumidor devuelve bytes a un array utilizable — misma tupla, mismos canales:

```python
@kafka.consumer(topic="stream_images", format="image")
def on_frame(image):
    print(f"shape={image.shape}")      # (480, 640, 3) — llegó intacto
```

## Por qué `format="image"` es un contrato, no una conveniencia

- **Round-trip determinístico.** El mismo serializador que codifica es el que decodifica. Los consumidores nunca re-detectan shape/dtype/canales por heurísticas.
- **Orden de frames = orden de producto.** En pipelines de visión la unidad con sentido es el frame, no el byte. WKafka preserva el orden por entidad (claves, Día 02) sobre un tipo de imagen de primera clase.
- **Interop OpenCV/NumPy/PIL** de fábrica — el array que envías es el array que reciben los consumidores, sin pegamento.

## El gancho

> 🖼️ Enviar una imagen por Kafka no debería significar "enviar bytes y esperar que el otro lado sepa qué eran". Significa `format="image"` — un mensaje tipado que viaja con el mismo decorador, el mismo orden y el mismo apagado limpio que tus eventos JSON.

## Pruébalo

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `03_multimedia`, ejecutable y reproducible:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*¿Cuál es el tipo de dato más incómodo que has enviado por Kafka como bytes crudos? Queremos ver esa lista en los comentarios.*

#Kafka #ComputerVision #Python #OpenCV #NumPy #Streaming #OpenSource #Wisrovi