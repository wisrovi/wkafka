"""Basic Payload Offloading (Claim-Check) Consumer Example for WKafka.

Demonstrates transparent retrieval of image frames from Redis RAM.
The consumer function receives the original np.ndarray decoded seamlessly.
"""

import numpy as np
from wkafka import Message, WKafka

# Initialize WKafka consumer with offload enabled
kafka = WKafka(
    bootstrap_servers="localhost:9092",
    offload={
        "backend": "redis",
        "ttl": 5,
    },
)

TOPIC_NAME = "camera_stream_offload_basic"


@kafka.consumer(topic=TOPIC_NAME, format="image", group_id="basic-offload-group")
def on_frame_received(msg: Message):
    """Processes incoming frames.

    msg.value is automatically retrieved from Redis RAM and decoded to np.ndarray.
    """
    frame: np.ndarray = msg.value
    if frame is None:
        print(f"⚠️  Offset {msg.offset}: Frame expired in Redis (skipped)")
        return

    print(
        f"📸 Received frame at offset {msg.offset} (Key: {msg.key}) | "
        f"Shape: {frame.shape}, Dtype: {frame.dtype}"
    )


if __name__ == "__main__":
    print("👀 Starting Basic Offload Consumer...")
    print(f"Listening on topic: {TOPIC_NAME}")
    print("Press Ctrl+C to terminate.")
    kafka.run_consumers()
