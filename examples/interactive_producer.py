from wkafka.controller import Wkafka

# Initialize Wkafka for local development on default port 9092
kafka_instance = Wkafka(server="localhost:9092", name="json_producer")

with kafka_instance.producer() as producer:
    # Prepare custom JSON payload
    payload = {
        "status": "active",
        "message": "Hello from interactive producer generator!",
        "data": {
            "value": 100,
            "correlation_id": "gen_98765"
        }
    }
    
    # Send the message
    producer.send(
        topic="json_topic",
        value=payload,
        key="interactive_key",
        value_type="json",
        headers={"source": "interactive_mcp_generator"}
    )
    print("🚀 Mensaje JSON enviado con éxito a 'json_topic'.")
