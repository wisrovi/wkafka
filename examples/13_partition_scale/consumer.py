"""
Consumer for Partition Scaling Example.

Demonstrates auto-scaling topic partitions for true parallelism using
kafka.run_consumers(partition_scale=True).
"""

from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092", dynamic_group_id=True)
TOPIC_NAME = "partition_scaled_topic"


# Worker 1
@kafka.consumer(topic=TOPIC_NAME, format="json", key_filter="key_1")
def worker_1(msg):
    print(
        f"👷 [WORKER 1] Processed job {msg.value.get('job_id')} on partition offset {msg.offset}"
    )


# Worker 2
@kafka.consumer(topic=TOPIC_NAME, format="json")
def worker_2(msg):
    print(
        f"👷 [WORKER 2] Processed job {msg.value.get('job_id')} on partition offset {msg.offset}"
    )


if __name__ == "__main__":
    print(f"🎧 Starting consumers on '{TOPIC_NAME}' with partition_scale=True...")
    # Dynamically scales topic partitions to match the number of registered workers
    kafka.run_consumers(block=True, partition_scale=True)
