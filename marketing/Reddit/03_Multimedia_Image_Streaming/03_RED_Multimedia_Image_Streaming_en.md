[P] Kafka has no idea your value is an image. WKafka gives it a contract — and computer-vision teams lose an entire decoding layer.

We stream camera frames over Kafka the same way we stream JSON — decorator, key routing, clean shutdown. The only difference is `format="image"`, one serializer both sides.

```python
frame = np.zeros((480, 640, 3), dtype=np.uint8)
cv2.circle(frame, (320, 240), 80, (0, 0, 255), -1)

with kafka.producer() as p:
    p.send(topic="stream_images", value=frame, format="image", quality=90)

@kafka.consumer(topic="stream_images", format="image")
def on_frame(img):
    print(f"shape={img.shape}")   # (480, 640, 3) arrives intact
```

Why it matters beyond the demo: shape/dtype/channel guessing disappears; frames keep the per-entity order you win with keys (Day 02); OpenCV/NumPy/PIL round-trip with zero glue. `03_multimedia` is one runnable script.

- PyPI: https://pypi.org/project/wkafka
- Repo + `03_multimedia` reproducible: https://github.com/wisrovi/wkafka
- Docs: https://wkafka.readthedocs.io

*What non-text data do YOU push through Kafka as raw bytes? Honest list in the thread.*

#Kafka #Python #ComputerVision #OpenCV #Streaming #OpenSource