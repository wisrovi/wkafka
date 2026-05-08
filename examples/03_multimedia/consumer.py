import cv2
from wkafka import WKafka
kafka = WKafka(dynamic_group_id=True)
@kafka.consumer(topic="image_stream", format="image")
def show_image(msg):
    print(f"🖼️ Frame recibido: {msg.value.shape}")
    cv2.imwrite("last_received_frame.jpg", msg.value)
if __name__ == "__main__":
    kafka.run_consumers(block=True)
