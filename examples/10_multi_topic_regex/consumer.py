"""
Multi-Topic and Regex Subscription Consumer Example for WKafka.
"""

from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092", dynamic_group_id=True)


# 1. Multi-topic subscription (list of topics)
@kafka.consumer(topic=["sensor_temp", "sensor_humidity"], format="json")
def handle_sensor_list(msg):
    print(f"📥 [MULTI-TOPIC] Received on topic '{msg.topic}': {msg.value}")


if __name__ == "__main__":
    print("🎧 Listening to multiple sensor topics...")
    kafka.run_consumers(block=True)
