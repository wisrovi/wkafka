from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional


@dataclass
class Message:
    """
    Represents a Kafka message with its metadata and deserialized value.

    Attributes:
        value (Any): The deserialized message content.
        topic (str): The topic from which the message was received.
        group_id (str): The consumer group ID.
        offset (int): The message offset in the partition.
        key (Optional[str]): The message key, if any.
        headers (Dict[str, Any]): Decoded message headers.
        _commit_fn (Optional[Callable[[], None]]): Callback for manual offset commit.
    """

    value: Any
    topic: str
    group_id: str
    offset: int
    key: Optional[str] = None
    headers: Dict[str, Any] = None
    _commit_fn: Optional[Callable[[], None]] = None

    @property
    def header(self) -> Optional[Dict[str, Any]]:
        """Alias for headers to maintain compatibility with legacy structures."""
        return self.headers

    def commit(self) -> None:
        """Manually commit the offset of this message."""
        if self._commit_fn:
            self._commit_fn()
