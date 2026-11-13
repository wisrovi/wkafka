"""Extended Payload Offloading Producer Example for WKafka.

Demonstrates loading configuration from an external YAML file (or extended dict),
specifying custom host/port, TTL, namespace key prefixes, and size thresholding.
"""

import os
import time
import cv2
import numpy as np
from wkafka import WKafka

# Option A: Initialize directly from external YAML file
config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
kafka = WKafka(config=config_path)

TOPIC_NAME = "camera_stream_offload_extended"


def create_synthetic_frame(index: int) -> np.ndarray:
    """Creates a high-definition synthetic frame."""
    frame = np.zeros((720, 1280, 3), dtype=np.uint8)
    frame[:, :] = [30, 25, 45]
    cv2.circle(frame, (640, 360), 180, (255, 100, 50), -1)
    cv2.putText(
        frame,
        f"EXTENDED STREAM #{index:03d}",
        (380, 375),
        cv2.FONT_HERSHEY_SIMPLEX,
        1.5,
        (255, 255, 255),
        3,
    )
    return frame


def run_producer():
    """Runs the extended offload producer."""
    print("🚀 Starting Extended Offload Producer...")
    print(f"Loaded Configuration from: {config_path}")
    print(f"Target Topic: {TOPIC_NAME}")
    print(
        f"Offload Backend: {kafka.offload_config.backend} -> {kafka.offload_config.host}:{kafka.offload_config.port}"
    )
    print(
        f"TTL: {kafka.offload_config.ttl}s | Key Prefix: {kafka.offload_config.key_prefix}"
    )

    for idx in range(1, 6):
        frame = create_synthetic_frame(idx)

        # 1. Multimedia offloaded message (triggers offload)
        kafka.send(
            topic=TOPIC_NAME,
            value=frame,
            format="image",
            key=f"hd-cam-{idx}",
            headers={"camera_location": "entrance-gate", "stream_hd": "true"},
        )
        print(
            f"📸 Frame #{idx} (720x1280) offloaded to Redis RAM & reference sent to Kafka"
        )

        # 2. Regular lightweight JSON metadata (bypasses offload and travels directly via Kafka)
        kafka.send(
            topic="camera_alerts",
            value={"camera_id": "hd-cam-01", "event": "stream_alive", "index": idx},
            format="json",
        )
        print(
            f"⚡ Alert metadata #{idx} sent directly through Kafka (no offload needed)"
        )

        time.sleep(1.0)

    print("🎉 Finished sending extended offloaded messages.")


if __name__ == "__main__":
    run_producer()
