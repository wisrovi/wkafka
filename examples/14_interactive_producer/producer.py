"""
Interactive CLI Producer Example.

Allows developers to interactively type and publish JSON or string messages to Kafka topics.
"""

import json
import sys
from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092")


def main():
    print("==================================================")
    print("🎮 WKafka Interactive CLI Producer")
    print("==================================================")
    print("Type 'exit' or 'quit' at any prompt to exit.\n")

    topic = input("Enter target Kafka topic [default: interactive_topic]: ").strip()
    if not topic:
        topic = "interactive_topic"

    with kafka.producer() as producer:
        while True:
            try:
                msg_input = input(f"\n[{topic}] Enter message payload (text or JSON): ").strip()
                if msg_input.lower() in ("exit", "quit"):
                    print("👋 Exiting interactive producer.")
                    sys.exit(0)

                if not msg_input:
                    continue

                # Try parsing payload as JSON, otherwise send as raw string
                try:
                    payload = json.loads(msg_input)
                    fmt = "json"
                except json.JSONDecodeError:
                    payload = msg_input
                    fmt = "json"

                key_input = input(f"[{topic}] Enter optional message key (press ENTER to skip): ").strip()
                key = key_input if key_input else None

                producer.send(topic, value=payload, key=key, format=fmt)
                print(f"✅ Message sent successfully to '{topic}'!")
            except (KeyboardInterrupt, EOFError):
                print("\n👋 Exiting interactive producer.")
                sys.exit(0)


if __name__ == "__main__":
    main()
