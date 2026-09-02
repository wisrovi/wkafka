"""
Producer for Partition Scaling Example.
"""

import time
from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092")
TOPIC_NAME = "partition_scaled_topic"


def main():
    print("🚀 Producing messages to test partition auto-scaling...")
    with kafka.producer() as p:
        for i in range(1, 7):
            p.send(
                TOPIC_NAME,
                value={"job_id": i, "task": f"parallel_job_{i}"},
                key=f"key_{i}",
                format="json",
            )
            print(f"Sent job #{i} with key key_{i}")
            time.sleep(0.5)


if __name__ == "__main__":
    main()
