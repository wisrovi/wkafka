"""
Producer for Pydantic Schema Validation Example.
"""

import time
from pydantic import BaseModel
from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092")
TOPIC_NAME = "pydantic_users_topic"


class UserProfile(BaseModel):
    user_id: int
    username: str
    email: str
    is_active: bool = True


def main():
    print("🚀 Sending Pydantic model payloads...")
    with kafka.producer() as p:
        user = UserProfile(user_id=101, username="alice", email="alice@example.com")
        p.send(TOPIC_NAME, value=user, format="pydantic")
        print(f"Sent Pydantic user: {user.username}")
        time.sleep(1)


if __name__ == "__main__":
    main()
