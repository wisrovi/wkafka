# Sending a frame (not a byte) is a Kafka API choice. WKafka makes it `format="image"`.

Kafka delivers bytes. It does not care if they are a contract, a cat photo or a satellite frame. For a team that streams vision data, "raw bytes" means every consumer re-implements decoding, shape-guessing and channel-order handling. WKafka's answer is a contract the library enforces: images are a first-class message.

> Day 03 of the WKafka open-source series — decorator-based, MIT, reproducible.

## What "image" means as a format

Producer sends a NumPy array, not a blob:

```python
frame = np.zeros((480, 640, 3), dtype=np.uint8)   # BGR canvas
cv2.circle(frame, (320, 240), 100, (0, 0, 255), -1)

with kafka.producer() as p:
    p.send(
        topic="stream_images",
        value=frame,
        format="image",        # serializer: array → JPEG (quality) → bytes
        quality=90,
    )
```

Consumer flips bytes back to a working array — same tuple, same channels:

```python
@kafka.consumer(topic="stream_images", format="image")
def on_frame(image):
    print(f"shape={image.shape}")      # (480, 640, 3) — came back intact
```

## Why `format="image"` is a contract, not a convenience

- **Deterministic round-trip.** The same serializer that encoded is the one that decodes. Consumers never re-detect shape/dtype/channels by heuristics.
- **Frame order = product order.** In vision pipelines the meaningful unit is the frame, not the byte. WKafka preserves per-entity order (keys, Day 02) on top of a first-class image type.
- **OpenCV/NumPy/PIL interop** out of the box — the array you send is the array consumers receive, no glue.

## The hook

> 🖼️ Sending an image through Kafka shouldn't mean "sending bytes and hoping the other side knows what they were." It means `format="image"` — a typed message that rides on the same decorator, the same ordering and the same clean shutdown as your JSON events.

## Try it

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `03_multimedia`, runnable & reproducible:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*What's the most awkward data type you've forced through Kafka as raw bytes? Let's see that list in the comments.*

#Kafka #ComputerVision #Python #OpenCV #NumPy #Streaming #OpenSource #Wisrovi
