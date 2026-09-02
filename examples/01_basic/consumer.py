from wkafka import WKafka

kafka = WKafka(dynamic_group_id=True)


@kafka.consumer(topic="basic_json", format="json")
def on_message(msg):
    print(f"✅ Recibido: {msg.value}")


if __name__ == "__main__":
    print("👂 Consumidor iniciado...")
    kafka.run_consumers(block=True)
