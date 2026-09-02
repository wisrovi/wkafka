"""
Standalone Image Streaming Pipeline Example for WKafka.

This script demonstrates sending and receiving images within a single execution
using background consumer threads.
"""

import threading
import time
import cv2
import numpy as np
from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092", dynamic_group_id=True)
TOPIC_NAME = "image_pipeline_topic"


@kafka.consumer(topic=TOPIC_NAME, format="image")
def handle_received_frame(msg):
    """Callback function triggered whenever an image frame is received."""
    frame: np.ndarray = msg.value
    print(f"🖼️ [CONSUMER] Received frame with dimensions: {frame.shape}")
    cv2.imwrite("latest_stream_frame.jpg", frame)


def run_pipeline():
    """Start consumer in background, then produce sample frames."""
    # Start consumer thread in daemon mode
    consumer_thread = threading.Thread(
        target=lambda: kafka.run_consumers(block=True),
        daemon=True
    )
    consumer_thread.start()
    time.sleep(4)  # Wait for Kafka consumer group rebalance and partition assignment

    print("🚀 [PRODUCER] Starting frame generation & transmission...")
    with kafka.producer() as producer:
        for i in range(1, 4):
            # Create dynamic color frame
            frame = np.zeros((360, 640, 3), dtype=np.uint8)
            color = ((i * 80) % 256, (i * 120) % 256, (i * 160) % 256)
            cv2.rectangle(frame, (100, 100), (540, 260), color, -1)
            cv2.putText(
                frame,
                f"Frame #{i}",
                (240, 190),
                cv2.FONT_HERSHEY_SIMPLEX,
                1.2,
                (255, 255, 255),
                3,
            )

            print(f"📸 [PRODUCER] Sending Frame #{i}...")
            producer.send(TOPIC_NAME, value=frame, format="image", quality=85)
            time.sleep(1)

    # Allow time for consumer thread to process final messages
    time.sleep(2)
    print("✅ Image pipeline demo finished successfully.")


if __name__ == "__main__":
    run_pipeline()
