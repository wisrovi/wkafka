"""
Producer for Multi-Topic and Regex Pattern Examples.
"""

import time
from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092")


def main():
    print("producir a varios tópicos (sensor_temp, sensor_humidity)...")
    with kafka.producer() as p:
        p.send("sensor_temp", value={"metric": "temperature", "value": 24.5}, format="json")
        print("Sent to sensor_temp")
        time.sleep(1)

        p.send("sensor_humidity", value={"metric": "humidity", "value": 60.0}, format="json")
        print("Sent to sensor_humidity")
        time.sleep(1)


if __name__ == "__main__":
    main()
