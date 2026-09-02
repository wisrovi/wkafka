"""
Producer for Manual Commit Example.
"""

import time
from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092")
TOPIC_NAME = "manual_commit_topic"


def main():
    print("🚀 Sending events for manual offset commit test...")
    with kafka.producer() as p:
        for i in range(1, 4):
            p.send(
                topic=TOPIC_NAME,
                value={"transaction_id": f"tx_{i}", "amount": i * 100},
                format="json",
            )
            print(f"Sent transaction tx_{i}")
            time.sleep(1)


if __name__ == "__main__":
    main()
