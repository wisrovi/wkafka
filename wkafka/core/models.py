from dataclasses import dataclass
from typing import Any, Dict, Optional

@dataclass(frozen=True)
class Message:
    """
    Represents a Kafka message with its metadata and deserialized value.

    Attributes:
        value (Any): The deserialized message content.
        key (Optional[str]): The message key, if any.
        topic (str): The topic from which the message was received.
        group_id (str): The consumer group ID.
        headers (Dict[str, Any]): Decoded message headers.
        offset (int): The message offset in the partition.
    """
    value: Any
    topic: str
    group_id: str
    offset: int
    key: Optional[str] = None
    headers: Dict[str, Any] = None
