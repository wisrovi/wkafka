# Kafka has no opinion about your image. That's the whole point.

Most Kafka tutorials stop at strings, because the client politely ignores the hard part: binary is not a payload type, it's a negotiation (encoding, channels, frames, dimensions). WKafka's stance: image streaming should be a `format` flag, not a weekend project.

> Day 03 of the WKafka open-source series — decorator-based, MIT, reproducible.

## What WKafka does with an image

Native `format="image"` serialization. Producer side:

```python
with kafka.producer() as p:
    # streaming una imagen (frame) como transporte nativo
    p.send(
        topic="image_topic",
        value=frame,          # np.ndarray de OpenCV/NumPy
        format="image",
    )
```

Consumer side flips bytes back into a usable array without touching a single decode call:

```python
@kafka.consumer(topic="image_topic", format="image")
def on_image(image):
    print(f"Imagen recibida: shape={image.shape}")
    # ya es un np.ndarray manipulable (OpenCV/NumPy/PIL compatible)
```

The deserializer handles JSON-compatible? No — it handles **dimensional arrays**: shape, dtype, channel order — metadata that a raw `bytes` blob would force your consumer to reverse-engineer.

## Why it matters

Video surveillance, satellite frames, drawing board states, defect detection on a factory line. Anywhere the message is a *frame*, not a *fact* — the order-of-bytes is meaningless; the order-of-frames is the product.

## The result

> 🎯 In WKafka, an image is a first-class Kafka citizen: same decorator, same `format`, same shutdown. The `03_multimedia` example is a single runnable script.

## Try it yourself

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `03_multimedia` runnable, reproducible:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*What "non-text" message does your team keep sending through Kafka as raw bytes? I'd love to see that list.*

#Kafka #Python #ComputerVision #OpenCV #Streaming #OpenSource #Wisrovi