"""
Image Consumer Example for WKafka.

This script demonstrates how to receive and process images from Apache Kafka
using WKafka's @kafka.consumer decorator with format="image".
"""

import cv2
import numpy as np
from wkafka import WKafka

# Initialize WKafka client with a dynamic group ID to always get fresh messages
kafka = WKafka(bootstrap_servers="localhost:9092", dynamic_group_id=True)

TOPIC_NAME = "image_stream_topic"


@kafka.consumer(topic=TOPIC_NAME, format="image")
def receive_image(msg):
    """
    Consumer handler for image messages.

    The message value is automatically deserialized into a NumPy ndarray
    representing the OpenCV BGR image.
    """
    img: np.ndarray = msg.value
    height, width, channels = img.shape
    print(f"📥 Received Image -> Dimensions: {width}x{height}, Channels: {channels}, dtype: {img.dtype}")

    # Optionally save the received image to disk
    output_filename = f"received_image_{msg.offset}.jpg"
    cv2.imwrite(output_filename, img)
    print(f"💾 Saved received image to '{output_filename}'")


if __name__ == "__main__":
    print(f"🎧 Listening for incoming image messages on topic '{TOPIC_NAME}'...")
    kafka.run_consumers(block=True)
