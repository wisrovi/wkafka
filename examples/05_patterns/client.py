import threading
import time

from wkafka import WKafka

kafka = WKafka(dynamic_group_id=True)


@kafka.consumer(topic="response_topic", format="json")
def handle_response(msg):
    print(f"🏁 Cliente recibió: {msg.value}")


if __name__ == "__main__":
    threading.Thread(
        target=lambda: kafka.run_consumers(block=True), daemon=True
    ).start()
    with kafka.producer() as p:
        p.send("request_topic", value={"id": 123}, format="json")
    print("📨 Solicitud enviada, esperando respuesta...")
    time.sleep(5)
