"""Producer for Long-Running Heavy Task Example in WKafka.

Sends multiple computation-heavy job requests to trigger concurrent
background processing in the consumer without blocking Kafka.
"""

from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092")
TOPIC_NAME = "heavy_ai_tasks_topic"

TASKS = [
    {"task_id": "job_video_analysis_01", "type": "video_inference", "duration": 4},
    {"task_id": "job_pdf_report_02", "type": "pdf_compilation", "duration": 6},
    {"task_id": "job_milvus_indexing_03", "type": "vector_indexing", "duration": 3},
    {"task_id": "job_batch_llm_04", "type": "llm_summary", "duration": 5},
]


def main():
    """Submit heavy processing requests to Kafka."""
    print(f"🚀 Producing {len(TASKS)} heavy task events to '{TOPIC_NAME}'...")

    with kafka.producer() as p:
        for task in TASKS:
            print(
                f"📤 Enqueueing task: {task['task_id']} (Est. compute: {task['duration']}s)"
            )
            p.send(TOPIC_NAME, value=task, format="json")

    print("✅ All tasks submitted to Kafka successfully.")


if __name__ == "__main__":
    main()
