"""
Async / Await Consumer Handler Example for WKafka.
"""

import asyncio
from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092", dynamic_group_id=True)
TOPIC_NAME = "async_events_topic"


@kafka.consumer(topic=TOPIC_NAME, format="json")
async def handle_async_event(msg):
    """
    Async consumer handler.
    Allows non-blocking asynchronous operations inside the callback.
    """
    print(f"📥 [ASYNC START] Processing job {msg.value.get('job_id')}...")
    # Simulate asynchronous non-blocking I/O (e.g. async DB/API request)
    await asyncio.sleep(0.5)
    print(f"✅ [ASYNC END] Completed job {msg.value.get('job_id')}")


if __name__ == "__main__":
    print(f"🎧 Listening on '{TOPIC_NAME}' with async/await handler...")
    kafka.run_consumers(block=True)
