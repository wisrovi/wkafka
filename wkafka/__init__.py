"""WKafka package initialization module."""

import sys
from wkafka.core.manager import WKafka
from wkafka.core.models import Message
from wkafka.controller import wkafka

# Support legacy import: from wkafka.wkafka import Wkafka / import wkafka.wkafka
sys.modules["wkafka.wkafka"] = wkafka

__version__ = "1.1.0"
__all__ = ["WKafka", "Message", "Wkafka", "wkafka"]

# Alias for backward compatibility if needed, promoting WKafka (CamelCase)
Wkafka = WKafka
