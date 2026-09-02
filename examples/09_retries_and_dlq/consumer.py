"""
Consumer with Automatic Retries and Dead Letter Queue (DLQ) Routing.
"""

from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092", dynamic_group_id=True)
TOPIC_NAME = "unstable_events_topic"
DLQ_TOPIC = "unstable_events_topic.DLQ"


@kafka.consumer(
    topic=TOPIC_NAME,
    format="json",
    max_retries=2,
    retry_delay=1.0,
    dlq_topic=DLQ_TOPIC,
)
def process_event(msg):
    """
    Consumer handler with automatic exponential backoff retries (2 retries)
    and automatic routing to DLQ topic if all retries fail.
    """
    data = msg.value
    print(f"📥 Processing Event #{data.get('id')} (status: {data.get('status')})...")

    if data.get("status") == "corrupt_data":
        raise ValueError("Simulated processing failure for corrupt payload")

    print(f"✅ Successfully processed Event #{data.get('id')}")


@kafka.consumer(topic=DLQ_TOPIC, format="json")
def handle_dlq_event(msg):
    """Consumer handler to monitor messages routed to Dead Letter Queue."""
    print("☠️ [DLQ MONITOR] Received message in Dead Letter Queue:")
    print(f"  - Original Payload: {msg.value}")
    print(f"  - Error Header: {msg.headers.get('x-error-message')}")
    print(f"  - Original Topic: {msg.headers.get('x-original-topic')}")


if __name__ == "__main__":
    print(f"🎧 Listening on '{TOPIC_NAME}' with DLQ routing to '{DLQ_TOPIC}'...")
    kafka.run_consumers(block=True)
