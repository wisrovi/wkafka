"""Decoupled Long-Running Task Consumer Example for WKafka.

Demonstrates how to process heavy tasks (e.g. AI inference, video analytics,
or document generation) without blocking the Kafka consumer loop or triggering
rebalances due to max_poll_interval_ms expiration.
"""

import time
from wkafka import WKafka

# Initialize WKafka manager with dynamic consumer group for isolation
kafka = WKafka(bootstrap_servers="localhost:9092", dynamic_group_id=True)
TOPIC_NAME = "heavy_ai_tasks_topic"


@kafka.consumer(
    topic=TOPIC_NAME,
    format="json",
    commit_on_receive=True,  # 1. Immediately acknowledge offset to Kafka
    background=True,  # 2. Dispatch callback to background worker thread
    background_workers=3,  # 3. Allow up to 3 long-running tasks in parallel
)
def handle_heavy_task(msg):
    """Consumer handler executed asynchronously in a background worker thread.

    The main Kafka polling thread never blocks, keeping heartbeats alive!
    """
    task_id = msg.value.get("task_id", "unknown")
    estimated_duration = msg.value.get("duration", 5)

    print(
        f"🚀 [Worker Thread] Started processing task '{task_id}' (Offset: {msg.offset}). "
        f"Simulating heavy compute for {estimated_duration}s..."
    )

    # Simulate heavy workload (e.g. video frame decoding, LLM summarization, PDF generation)
    time.sleep(estimated_duration)

    print(f"✅ [Worker Thread] Finished task '{task_id}' successfully!")


if __name__ == "__main__":
    print(f"🎧 Listening on topic '{TOPIC_NAME}' with decoupled background workers...")
    print(
        "💡 Notice: The consumer thread stays 100% active while tasks run in the background!"
    )
    kafka.run_consumers(block=True)
