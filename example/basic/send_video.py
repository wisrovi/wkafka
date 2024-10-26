import cv2
from tqdm import tqdm
from wkafka.controller import Wkafka


kafka_instance = Wkafka(server="192.168.1.60:9092")


with kafka_instance.producer() as producer:

    video_capture = cv2.VideoCapture(f"../../data/videos/statistics/0007.mp4")
    assert video_capture.isOpened(), "Error reading video file"

    # read metadata of video
    frame_width, frame_height, total_frames, fps = [
        int(video_capture.get(value))
        for value in [
            cv2.CAP_PROP_FRAME_WIDTH,
            cv2.CAP_PROP_FRAME_HEIGHT,
            cv2.CAP_PROP_FRAME_COUNT,
            cv2.CAP_PROP_FPS,
        ]
    ]

    for frame_id in tqdm(
        range(total_frames), desc="Processing video frames", unit="frames"
    ):
        ret, im0 = video_capture.read()
        if not ret:
            break

        cv2.putText(
            im0,
            f"frame: {frame_id}",
            (100, 100),
            cv2.FONT_HERSHEY_SIMPLEX,
            2,
            (255, 0, 255),
            2,
        )

        producer.async_send(
            topic="video_topic",
            value=im0,
            key="image",
            value_type="image",
            header={
                "frame_width": frame_width,
                "frame_height": frame_height,
                "total_frames": total_frames,
                "fps": fps,
                "frame_id": frame_id,
            },
        )

        DREAM_WIDTH = 600
        new_size = DREAM_WIDTH / frame_width
        im0 = cv2.resize(
            im0, (int(frame_width * new_size), int(frame_height * new_size))
        )

        cv2.imshow("Video Send", im0)
        if cv2.waitKey(1) & 0xFF == ord("q"):
            break
    cv2.destroyAllWindows()
