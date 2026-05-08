import concurrent.futures
import os
import threading
import uuid
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from kafka import KafkaConsumer, KafkaProducer
from loguru import logger

from wkafka.core.models import Message
from wkafka.serializers.base import JSONSerializer, YAMLSerializer, ImageSerializer

# Global to ensure logger is configured only once
_LOG_CONFIGURED = False

class WKafka:
    """
    The main orchestrator for Kafka production and consumption.
    """

    _SERIALIZERS = {
        "json": JSONSerializer(),
        "yaml": YAMLSerializer(),
        "image": ImageSerializer(),
    }

    def __init__(
        self,
        bootstrap_servers: Optional[Union[str, List[str]]] = None,
        client_id: Optional[str] = None,
        dynamic_group_id: bool = False,
        **extra_config: Any
    ):
        global _LOG_CONFIGURED
        if not _LOG_CONFIGURED:
            logger.add("wkafka.log", rotation="10 MB", level="INFO")
            _LOG_CONFIGURED = True

        self.bootstrap_servers = bootstrap_servers or os.environ.get("KAFKA_SERVER", "localhost:9092")
        self.client_id = client_id or "wkafka-client"
        self.dynamic_group_id = dynamic_group_id
        self.extra_config = extra_config
        self._consumers_registry: List[Tuple[KafkaConsumer, Callable, Optional[str], Optional[str]]] = []
        self._producer_instance: Optional[KafkaProducer] = None
        self._lock = threading.Lock()

    def _generate_group_id(self) -> str:
        return f"wkafka-{uuid.uuid4().hex[:8]}" if self.dynamic_group_id else "wkafka-default-group"

    def _get_producer(self) -> KafkaProducer:
        with self._lock:
            if self._producer_instance is None:
                compression = "snappy"
                try:
                    import snappy
                except ImportError:
                    compression = "gzip"

                producer_config = {
                    "bootstrap_servers": self.bootstrap_servers,
                    "acks": int(os.environ.get("KAFKA_ACKS", "1")),
                    "compression_type": compression,
                    "value_serializer": lambda v: v,
                    "key_serializer": lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                }
                producer_config.update(self.extra_config)

                self._producer_instance = KafkaProducer(**producer_config)
            return self._producer_instance

    def consumer(
        self,
        topic: str,
        group_id: Optional[str] = None,
        key_filter: Optional[str] = None,
        format: str = "json",
        **kafka_kwargs: Any,
    ) -> Callable:
        def decorator(func: Callable) -> Callable:
            final_group_id = group_id or self._generate_group_id()
            
            config = {
                "bootstrap_servers": self.bootstrap_servers,
                "group_id": final_group_id,
                "auto_offset_reset": "latest",
                "enable_auto_commit": True,
            }
            # Combinar configuraciones: Clase base < kwargs del decorador < extra_config de la instancia
            config.update(kafka_kwargs)
            
            # Asegurar que el config de la instancia (SASL, etc) se aplique si no se sobreescribe
            for k, v in self.extra_config.items():
                if k not in kafka_kwargs:
                    config[k] = v

            consumer = KafkaConsumer(topic, **config)
            self._consumers_registry.append((consumer, func, key_filter, format))
            
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                return func(*args, **kwargs)
            return wrapper
        return decorator

    def _handle_message(
        self,
        consumer: KafkaConsumer,
        func: Callable,
        key_filter: Optional[str],
        data_format: str
    ) -> None:
        for raw_msg in consumer:
            if key_filter and raw_msg.key != key_filter:
                continue

            headers = {}
            if raw_msg.headers:
                for k, v in raw_msg.headers:
                    try:
                        headers[k] = v.decode("utf-8")
                    except:
                        headers[k] = v

            value = raw_msg.value
            if data_format in self._SERIALIZERS:
                try:
                    value = self._SERIALIZERS[data_format].deserialize(value)
                except Exception as e:
                    logger.error(f"Failed to deserialize message in {data_format}: {e}")

            msg_obj = Message(
                value=value,
                topic=raw_msg.topic,
                group_id=consumer.config["group_id"],
                offset=raw_msg.offset,
                key=raw_msg.key.decode("utf-8") if raw_msg.key else None,
                headers=headers
            )

            try:
                result = func(msg_obj)
                if isinstance(result, dict) and result.get("exit"):
                    break
            except Exception as e:
                logger.exception(f"Error in consumer callback: {e}")

    def run_consumers(self, block: bool = True) -> None:
        if not self._consumers_registry:
            logger.warning("No consumers registered. Nothing to run.")
            return

        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=len(self._consumers_registry),
            thread_name_prefix="WKafkaConsumer"
        )
        
        futures = []
        for consumer, func, key_filter, data_format in self._consumers_registry:
            futures.append(executor.submit(self._handle_message, consumer, func, key_filter, data_format))

        if block:
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Consumer thread crashed: {e}")

    def send(
        self,
        topic: str,
        value: Any,
        key: Optional[str] = None,
        format: str = "json",
        headers: Optional[Dict[str, Any]] = None,
        **kwargs: Any
    ) -> Any:
        producer = self._get_producer()
        
        if format in self._SERIALIZERS:
            serialized_value = self._SERIALIZERS[format].serialize(value, **kwargs)
        else:
            serialized_value = value

        kafka_headers = []
        if headers:
            for k, v in headers.items():
                val = v if isinstance(v, bytes) else str(v).encode("utf-8")
                kafka_headers.append((k, val))

        return producer.send(topic, value=serialized_value, key=key, headers=kafka_headers)

    def producer(self) -> "WKafka":
        return self

    def __enter__(self) -> "WKafka":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._producer_instance:
            self._producer_instance.flush()
            self._producer_instance.close()
            self._producer_instance = None
