"""
Manual Offset Commit Consumer Example for WKafka.

Demonstrates At-Least-Once processing semantics using auto_commit=False
and explicit msg.commit().
"""

from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092", dynamic_group_id=True)
TOPIC_NAME = "manual_commit_topic"


@kafka.consumer(topic=TOPIC_NAME, format="json", auto_commit=False)
def handle_transaction(msg):
    """
    Consumer handler with explicit manual commit.
    Offset is only committed AFTER business logic succeeds.
    """
    print(f"📥 Received Transaction: {msg.value} (Offset: {msg.offset})")

    # Simulate database or business operation
    print(f"⚙️ Processing transaction {msg.value.get('transaction_id')}...")

    # Explicit manual commit of the offset
    msg.commit()
    print(f"✅ Offset {msg.offset} manually committed.")


if __name__ == "__main__":
    print(f"🎧 Listening for transactions on '{TOPIC_NAME}' with manual commit...")
    kafka.run_consumers(block=True)
