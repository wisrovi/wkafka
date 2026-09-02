"""
Standalone File Transfer Pipeline Example for WKafka.

Demonstrates sending and receiving arbitrary files in a single execution script using background threads.
"""

import os
import threading
import time
from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092", dynamic_group_id=True)
TOPIC_NAME = "file_pipeline_topic"


@kafka.consumer(topic=TOPIC_NAME, format="file")
def handle_received_file(msg):
    filename = (msg.headers or {}).get("filename", f"file_{msg.offset}.txt")
    out_dir = "received_pipeline_files"
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, filename)

    with open(out_path, "wb") as f:
        f.write(msg.value)

    print(f"📥 [CONSUMER] Saved file '{filename}' to '{out_path}'")


def run_pipeline():
    consumer_thread = threading.Thread(
        target=lambda: kafka.run_consumers(block=True), daemon=True
    )
    consumer_thread.start()
    time.sleep(2)

    print("🚀 [PRODUCER] Transmitting files...")
    with kafka.producer() as producer:
        for i in range(1, 3):
            fname = f"document_{i}.txt"
            content = f"Binary/Text content for file #{i}".encode("utf-8")

            print(f"📄 [PRODUCER] Sending '{fname}'...")
            producer.send(
                TOPIC_NAME,
                value=content,
                format="file",
                headers={"filename": fname},
            )
            time.sleep(1)

    time.sleep(2)
    print("✅ File pipeline example finished successfully.")


if __name__ == "__main__":
    run_pipeline()
