"""
Advanced Producer Example with Headers and Keys.

Demonstrates producing messages with custom headers and partition routing keys.
"""

import time
from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092")


def main():
    print("🚀 Sending messages with custom headers and routing keys...")
    with kafka.producer() as p:
        for i in range(1, 4):
            p.send(
                topic="advanced_topic",
                value={"event_id": i, "status": "active"},
                key=f"user_{i}",
                headers={"source": "api_gateway", "correlation_id": f"corr_{i}"},
                format="json",
            )
            print(f"Sent message {i} with key user_{i}")
            time.sleep(1)


if __name__ == "__main__":
    main()
