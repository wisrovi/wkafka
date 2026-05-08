from wkafka import WKafka
kafka = WKafka(dynamic_group_id=True)
@kafka.consumer(topic="request_topic", format="json")
def process_request(msg):
    print(f"⚙️ Procesando ID {msg.value['id']}...")
    with kafka.producer() as p:
        p.send("response_topic", value={"id": msg.value["id"], "status": "OK"}, format="json")
if __name__ == "__main__":
    kafka.run_consumers(block=True)
