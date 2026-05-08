import cv2
import numpy as np
import time
import threading
from wkafka import WKafka

kafka = WKafka(dynamic_group_id=True)

@kafka.consumer(topic="stream_images", format="image")
def process_frame(msg):
    frame = msg.value
    print(f"🖼️ Frame recibido con dimensiones: {frame.shape}")
    cv2.imwrite("received_frame.jpg", frame)

threading.Thread(target=lambda: kafka.run_consumers(block=True), daemon=True).start()
time.sleep(2)

dummy_frame = np.zeros((480, 640, 3), dtype=np.uint8)
cv2.circle(dummy_frame, (320, 240), 100, (255, 0, 0), -1)

print("📸 Enviando frame...")
with kafka.producer() as p:
    p.send("stream_images", value=dummy_frame, format="image", quality=90)
time.sleep(3)
