"""
Producer for Retries and Dead Letter Queue (DLQ) Example.
"""

import time
from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092")
TOPIC_NAME = "unstable_events_topic"


def main():
    print("🚀 Sending valid and invalid events to test DLQ...")
    with kafka.producer() as p:
        # Valid event
        p.send(TOPIC_NAME, value={"id": 1, "status": "ok"}, format="json")
        print("Sent valid event #1")
        time.sleep(1)

        # Invalid event that triggers consumer retries & DLQ routing
        p.send(TOPIC_NAME, value={"id": 2, "status": "corrupt_data"}, format="json")
        print("Sent invalid event #2 (will fail & route to DLQ)")
        time.sleep(1)


if __name__ == "__main__":
    main()
