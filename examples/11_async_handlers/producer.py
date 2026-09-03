"""
Producer for Async Handlers Example.
"""

import time
from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092")
TOPIC_NAME = "async_events_topic"


def main():
    print("🚀 Sending events for async consumer processing...")
    with kafka.producer() as p:
        for i in range(1, 4):
            p.send(
                TOPIC_NAME,
                value={"job_id": i, "task": f"async_task_{i}"},
                format="json",
            )
            print(f"Sent async job #{i}")
            time.sleep(1)


if __name__ == "__main__":
    main()
