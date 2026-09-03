"""Controller package module providing simplified WKafka interface aliases."""

from .wkafka import Wkafka
from wkafka.core.models import Message as Consumer_data

__all__ = ["Wkafka", "Consumer_data"]
