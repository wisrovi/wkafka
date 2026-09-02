"""
File Producer Example for WKafka.

Demonstrates sending arbitrary files (documents, PDFs, text files, binary data)
over Apache Kafka using format="file".
"""

import os
import time
from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092")
TOPIC_NAME = "file_transfer_topic"


def create_sample_files():
    """Create temporary test files to transmit."""
    sample_txt = "sample_document.txt"
    with open(sample_txt, "w", encoding="utf-8") as f:
        f.write("Hello WKafka! This is a test text file transferred via Kafka.\n")
    return [sample_txt]


def main():
    print("🚀 Starting File Producer...")
    files = create_sample_files()

    with kafka.producer() as producer:
        for file_path in files:
            file_name = os.path.basename(file_path)
            with open(file_path, "rb") as f:
                content_bytes = f.read()

            print(f"📄 Sending file '{file_name}' ({len(content_bytes)} bytes)...")
            producer.send(
                topic=TOPIC_NAME,
                value=content_bytes,
                format="file",
                headers={
                    "filename": file_name,
                    "content_type": "text/plain",
                    "source": "file_producer_service",
                },
            )
            time.sleep(1)

    print("✅ File transmission finished.")


if __name__ == "__main__":
    main()
