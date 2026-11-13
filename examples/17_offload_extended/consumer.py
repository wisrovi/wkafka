"""Extended Payload Offloading Consumer Example for WKafka.

Demonstrates consuming offloaded frames loaded from YAML configuration,
handling custom headers, and leveraging automatic late-frame dropping.
"""

import os
import numpy as np
from wkafka import Message, WKafka

# Option A: Initialize directly from external YAML configuration
config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
kafka = WKafka(config=config_path)

TOPIC_NAME = "camera_stream_offload_extended"


@kafka.consumer(
    topic=TOPIC_NAME,
    format="image",
    group_id="extended-offload-group",
)
def handle_extended_frame(msg: Message):
    """Processes HD camera frames fetched transparently from Redis RAM."""
    frame: np.ndarray = msg.value

    # Under on_expired="drop", stale frames are automatically dropped before reaching here.
    # If on_expired="none", frame will be None if expired.
    if frame is None:
        print(f"⚠️  Offset {msg.offset}: Frame expired in Redis RAM")
        return

    location = msg.headers.get("camera_location", "unknown")
    print(
        f"✅ Consumed Frame at offset {msg.offset} (Key: {msg.key}, Location: {location}) | "
        f"Resolution: {frame.shape[1]}x{frame.shape[0]} | Mean Intensity: {np.mean(frame):.1f}"
    )


if __name__ == "__main__":
    print("👀 Starting Extended Offload Consumer...")
    print(f"Loaded config from: {config_path}")
    print(f"Listening on: {TOPIC_NAME}")
    print("Press Ctrl+C to terminate.")
    kafka.run_consumers()
