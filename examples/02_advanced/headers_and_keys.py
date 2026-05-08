import time
import threading
from wkafka import WKafka

kafka = WKafka(dynamic_group_id=True)

@kafka.consumer(topic="advanced_meta", format="json")
def on_meta_message(msg):
    print(f"--- Mensaje Recibido ---")
    print(f"Key: {msg.key}")
    print(f"Headers: {msg.headers}")
    print(f"Value: {msg.value}")

threading.Thread(target=lambda: kafka.run_consumers(block=True), daemon=True).start()
time.sleep(2)

with kafka.producer() as p:
    p.send(
        topic="advanced_meta",
        value={"alert": "System high load"},
        key="system_monitor",
        headers={"priority": "high", "origin": "node-01"},
        format="json"
    )
time.sleep(3)
