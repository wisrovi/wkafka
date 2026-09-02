"""
File Consumer Example for WKafka.

Demonstrates receiving and saving arbitrary files transmitted over Apache Kafka using format="file".
"""

import os
from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092", dynamic_group_id=True)
TOPIC_NAME = "file_transfer_topic"


@kafka.consumer(topic=TOPIC_NAME, format="file")
def receive_file(msg):
    """
    Consumer handler for file messages.

    The message value contains raw file bytes.
    """
    file_bytes = msg.value
    headers = msg.headers or {}
    filename = headers.get("filename", f"received_file_{msg.offset}.bin")

    output_dir = "received_files"
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, filename)

    with open(out_path, "wb") as f:
        f.write(file_bytes)

    print(f"📥 Received file -> Saved '{filename}' ({len(file_bytes)} bytes) to '{out_path}'")


if __name__ == "__main__":
    print(f"🎧 Listening for incoming files on topic '{TOPIC_NAME}'...")
    kafka.run_consumers(block=True)
