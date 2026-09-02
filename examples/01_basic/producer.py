from wkafka import WKafka

kafka = WKafka()
if __name__ == "__main__":
    with kafka.producer() as p:
        p.send("basic_json", value={"msg": "Hola desde v1.0.0"}, format="json")
        print("🚀 Mensaje enviado.")
