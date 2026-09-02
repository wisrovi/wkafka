import threading
import time

from wkafka import WKafka

kafka = WKafka(dynamic_group_id=True)


@kafka.consumer(topic="tasks", format="json")
def worker(msg):
    print(f"👷 Procesando tarea: {msg.value}")
    with kafka.producer() as p:
        p.send("results", value={"id": msg.value["id"], "done": True}, format="json")


@kafka.consumer(topic="results", format="json")
def client(msg):
    print(f"🏁 Resultado: {msg.value}")


threading.Thread(target=lambda: kafka.run_consumers(block=True), daemon=True).start()
time.sleep(4)  # Wait for consumer group rebalance

with kafka.producer() as p:
    p.send("tasks", value={"id": 101, "action": "compute"}, format="json")
time.sleep(4)
