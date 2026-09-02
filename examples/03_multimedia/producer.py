import numpy as np

from wkafka import WKafka

kafka = WKafka()
if __name__ == "__main__":
    frame = np.random.randint(0, 255, (480, 640, 3), dtype=np.uint8)
    with kafka.producer() as p:
        p.send("image_stream", value=frame, format="image")
        print("📸 Imagen enviada.")
