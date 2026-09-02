from wkafka.core.manager import WKafka
from wkafka.core.models import Message

__version__ = "1.0.0"
__all__ = ["WKafka", "Message"]

# Alias for backward compatibility if needed, but promoting WKafka (CamelCase)
Wkafka = WKafka
