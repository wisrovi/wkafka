"""
Advanced Consumer Example with Headers and Key Filtering.

Demonstrates consuming messages and accessing headers and message keys.
"""

from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092", dynamic_group_id=True)


@kafka.consumer(topic="advanced_topic", format="json")
def handle_advanced_message(msg):
    print("📥 Received Advanced Message:")
    print(f"  - Key: {msg.key}")
    print(f"  - Offset: {msg.offset}")
    print(f"  - Headers: {msg.headers}")
    print(f"  - Value: {msg.value}")


if __name__ == "__main__":
    print("🎧 Listening for advanced messages...")
    kafka.run_consumers(block=True)
