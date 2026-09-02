import threading
import time

from wkafka import WKafka

kafka = WKafka(dynamic_group_id=True)


@kafka.consumer(topic="basic_json", format="json")
def on_message(msg):
    print(f"✅ Recibido: {msg.value}")


threading.Thread(target=lambda: kafka.run_consumers(block=True), daemon=True).start()
time.sleep(4)  # Wait for consumer group rebalance

print("🚀 Enviando mensaje...")
with kafka.producer() as p:
    p.send("basic_json", value={"hello": "world", "status": "online"}, format="json")
time.sleep(3)
