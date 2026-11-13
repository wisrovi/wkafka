"""Basic Payload Offloading (Claim-Check) Producer Example for WKafka.

Demonstrates how to send real-time multimedia (OpenCV image matrices)
offloading heavy payloads to Redis in RAM with a 5-second TTL while Kafka
transfers ultra-lightweight metadata references (~120 bytes).
"""

import time
import cv2
import numpy as np
from wkafka import WKafka

# Initialize WKafka with simple, minimal in-memory offload dictionary:
# - backend: "redis"
# - ttl: 5 (frames expire automatically after 5 seconds in RAM)
kafka = WKafka(
    bootstrap_servers="localhost:9092",
    offload={
        "backend": "redis",
        "ttl": 5,
    },
)

TOPIC_NAME = "camera_stream_offload_basic"


def generate_frame(frame_num: int) -> np.ndarray:
    """Generates a synthetic camera frame with dynamic visual content."""
    img = np.zeros((480, 640, 3), dtype=np.uint8)
    # Background gradient
    img[:, :] = [40, 40, 40]
    # Draw indicator
    cv2.circle(img, (320, 240), 100, (0, 255, 128), -1)
    cv2.putText(
        img,
        f"Frame #{frame_num}",
        (180, 250),
        cv2.FONT_HERSHEY_SIMPLEX,
        1.2,
        (255, 255, 255),
        2,
    )
    return img


def run_producer():
    """Streams frames with transparent Redis offloading."""
    print("🚀 Starting Basic Offload Producer...")
    print(f"Target Topic: {TOPIC_NAME}")
    print("Mode: Claim-Check with Redis RAM Storage (TTL = 5s)")

    for i in range(1, 11):
        frame = generate_frame(i)

        # Standard send call - format='image' triggers automatic offload to Redis
        kafka.send(
            topic=TOPIC_NAME,
            value=frame,
            format="image",
            key=f"cam-01-{i}",
        )
        print(f"✅ Frame #{i} offloaded to Redis RAM & reference emitted to Kafka")
        time.sleep(0.5)

    print("🎉 Finished producing 10 offloaded frames.")


if __name__ == "__main__":
    run_producer()
