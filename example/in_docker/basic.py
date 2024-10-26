from wkafka import Wkafka


kf = Wkafka(server="192.168.1.137:9092")
kf = Wkafka(server="kafka:19092")


@kf.consumer(
    topic="sms",
    value_type="json",
)
def process_message(data):
    header = data.header
    value = data.value
    
    value["result"] = "OK"
    
    print(f"Message received: {value} - {header}, key: {data.key}")
    
    if "response_to" in header:
        with kf.producer() as producer:
            producer.send(
                topic=header["response_to"],
                value=value,
                key=data.key,
                value_type="json",
                header=header,
            )


kf.run_consumers()
