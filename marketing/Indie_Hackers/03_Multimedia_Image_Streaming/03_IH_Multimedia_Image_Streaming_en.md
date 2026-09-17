### Kafka does not care what your "wide" is. `format="image"` does.

Ship is not just ASCII — for teams streaming vision data it's frames: satellite tiles, assembly-line inspections, camera wall feeds. Sending those as "raw bytes" exports the decoding guesswork to every consumer. Today's WKafka hook: make the **image** the message, not the bytes.

```python
frame = np.zeros((480, 640, 3), dtype=np.uint8)
cv2.circle(frame, (320, 240), 100, (0, 0, 255), -1)

with kafka.producer() as p:
    p.send(topic="stream_images", value=frame, format="image", quality=90)

@kafka.consumer(topic="stream_images", format="image")
def on_frame(image):
    print(f"shape={image.shape}")   # (480, 640, 3) — round-trip intact
```

**Why it deletes code:** one serializer both encodes and decodes (no shape/dtype/channel guessing), frames keep per-entity order from keys, and the array interops with OpenCV/NumPy/PIL as-is. Same decorator, same shutdown as your JSON handlers.

**Run it:**
- PyPI: https://pypi.org/project/wkafka
- GitHub + `03_multimedia` reproducible: https://github.com/wisrovi/wkafka
- Docs: https://wkafka.readthedocs.io

*What non-text payload have you forced through Kafka as raw bytes? Drop it below.*

#Kafka #ComputerVision #OpenCV #Python #Streaming #Wisrovi