"""
Image Producer Example for WKafka.

This script demonstrates how to create and send images (both OpenCV NumPy arrays
and PIL Images) over Apache Kafka using WKafka's built-in image serializer.
"""

import time
import numpy as np
from PIL import Image, ImageDraw
import cv2
from wkafka import WKafka

# Initialize WKafka client
kafka = WKafka(bootstrap_servers="localhost:9092")

TOPIC_NAME = "image_stream_topic"


def create_opencv_image() -> np.ndarray:
    """Generate a sample OpenCV BGR image (NumPy array)."""
    img = np.zeros((480, 640, 3), dtype=np.uint8)
    # Draw a green rectangle and blue circle
    cv2.rectangle(img, (50, 50), (250, 250), (0, 255, 0), -1)
    cv2.circle(img, (400, 240), 80, (255, 0, 0), -1)
    cv2.putText(
        img,
        "OpenCV Image",
        (180, 420),
        cv2.FONT_HERSHEY_SIMPLEX,
        1.0,
        (255, 255, 255),
        2,
    )
    return img


def create_pil_image() -> Image.Image:
    """Generate a sample PIL RGB Image."""
    img = Image.new("RGB", (640, 480), color=(73, 109, 137))
    draw = ImageDraw.Draw(img)
    draw.ellipse((200, 140, 440, 340), fill=(255, 215, 0), outline=(255, 255, 255))
    return img


def send_images():
    """Produce image messages to Apache Kafka."""
    print("🚀 Starting Image Producer...")

    with kafka.producer() as producer:
        # 1. Send OpenCV image
        cv_img = create_opencv_image()
        print("📸 Sending OpenCV image (NumPy array)...")
        producer.send(
            TOPIC_NAME,
            value=cv_img,
            format="image",
            quality=90,
        )
        time.sleep(1)

        # 2. Send PIL image
        pil_img = create_pil_image()
        print("🖼️ Sending PIL image...")
        producer.send(
            TOPIC_NAME,
            value=pil_img,
            format="image",
            quality=85,
        )
        time.sleep(1)

    print("✅ All image messages produced successfully.")


if __name__ == "__main__":
    send_images()
