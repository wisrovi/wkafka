"""
Consumer for Interactive CLI Producer Example.
"""

from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092", dynamic_group_id=True)
TOPIC_NAME = "interactive_topic"


@kafka.consumer(topic=TOPIC_NAME, format="json")
def handle_interactive_message(msg):
    print(f"📥 [INTERACTIVE CONSUMER] Received message on topic '{msg.topic}':")
    print(f"   - Key: {msg.key}")
    print(f"   - Offset: {msg.offset}")
    print(f"   - Payload: {msg.value}")


if __name__ == "__main__":
    print(f"🎧 Starting consumer listening on '{TOPIC_NAME}'...")
    kafka.run_consumers(block=True)
