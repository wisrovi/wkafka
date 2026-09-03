"""Simplified WKafka controller interface module."""

import os
from typing import Any, Callable, Dict, List, Optional, Union

from wkafka.core.manager import WKafka
from wkafka.core.models import Message as Consumer_data


class Wkafka(WKafka):
    """High-level Kafka controller providing simplified producer and consumer interfaces."""

    def __init__(
        self,
        server: Optional[Union[str, List[str]]] = None,
        name: Optional[str] = None,
        retry_delay: int = 10,
        max_retries: int = 3,
        dynamic_group_id: bool = False,
        partition_scale: bool = False,
        other_config: Optional[dict] = None,
        **kwargs: Any,
    ):
        """Initialize Wkafka instance with environment defaults and options."""
        bootstrap = (
            os.environ.get("KAFKA_SERVER")
            or server
            or kwargs.pop("bootstrap_servers", None)
        )

        config = other_config or {}
        config.update(kwargs)

        super().__init__(
            bootstrap_servers=bootstrap,
            client_id=name,
            dynamic_group_id=dynamic_group_id,
            partition_scale=partition_scale,
            **config,
        )

    def consumer(
        self,
        topic: str,
        group_id: Optional[str] = None,
        key: Optional[str] = None,
        value_type: Optional[str] = None,
        value_convert_to: Optional[str] = None,
        other_config: Optional[dict] = None,
        **kwargs: Any,
    ) -> Callable:
        """Register a decorator function to consume messages from a topic."""
        data_format = (
            value_type or value_convert_to or kwargs.pop("format", None) or "json"
        )

        # Fusionar configuraciones
        config = other_config or {}
        config.update(kwargs)
        config.pop("format", None)

        return super().consumer(
            topic=topic, group_id=group_id, key_filter=key, format=data_format, **config
        )

    def send(
        self,
        topic: str,
        value: Any,
        key: Optional[str] = None,
        value_type: Optional[str] = None,
        value_convert_to: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None,
        header: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Any:
        """Send a message to a Kafka topic."""
        data_format = (
            value_type or value_convert_to or kwargs.pop("format", None) or "json"
        )
        final_headers = headers or header
        return super().send(
            topic=topic,
            value=value,
            key=key,
            format=data_format,
            headers=final_headers,
            **kwargs,
        )
