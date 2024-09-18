import cv2
from wkafka2.controller.wkafka import Wkafka


kafka_instance = Wkafka(server="192.168.1.60:9092", name="video_show")


@kafka_instance.consumer(
    topic="mi_tema",
    value_convert_to="json",
)
def process_message(data):
    print(f"Mensaje recibido: {data.value}, con clave: {data.key}")


@kafka_instance.consumer(topic="image", value_convert_to="image")
def stream_video(data):
    DREAM = 600

    im0 = data.value

    cv2.imwrite("demo.jpg", im0)

    print(
        f"Message received 3: {data.value.shape}, key: {data.key}, headers: {data.header}"
    )


@kafka_instance.consumer(topic="video_topic", value_convert_to="image")
def stream_video(data):
    DREAM_WIDTH = 600

    im0 = data.value
    header = data.header

    frame_width = header.get("frame_width")
    frame_height = header.get("frame_height")

    new_size = DREAM_WIDTH / frame_width
    im0 = cv2.resize(im0, (int(frame_width * new_size), int(frame_height * new_size)))

    # Process each video frame as an image
    cv2.imshow("Video Received", im0)
    if cv2.waitKey(1) & 0xFF == ord("q"):
        return

    if header.get("frame_id") == (header.get("total_frames") - 1):
        cv2.destroyAllWindows()


kafka_instance.run_consumers()
