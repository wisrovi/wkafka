"""
Consumer for Pydantic Schema Validation Example.
"""

from pydantic import BaseModel
from wkafka import WKafka

kafka = WKafka(bootstrap_servers="localhost:9092", dynamic_group_id=True)
TOPIC_NAME = "pydantic_users_topic"


class UserProfile(BaseModel):
    user_id: int
    username: str
    email: str
    is_active: bool = True


@kafka.consumer(topic=TOPIC_NAME, format="pydantic", model=UserProfile)
def handle_user_profile(msg):
    """
    Consumer handler with automatic Pydantic schema validation.
    msg.value is a fully validated instance of UserProfile.
    """
    user: UserProfile = msg.value
    print("📥 Received Validated Pydantic Model:")
    print(f"  - User ID: {user.user_id}")
    print(f"  - Username: {user.username}")
    print(f"  - Email: {user.email}")
    print(f"  - Active: {user.is_active}")


if __name__ == "__main__":
    print(f"🎧 Listening on '{TOPIC_NAME}' with Pydantic validation...")
    kafka.run_consumers(block=True)
