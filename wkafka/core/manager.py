import asyncio
import concurrent.futures
import inspect
import os
import threading
import time
import uuid
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from kafka import KafkaConsumer, KafkaProducer
from loguru import logger

from wkafka.core.models import Message
from wkafka.serializers.base import (
    FileSerializer,
    ImageSerializer,
    JSONSerializer,
    PydanticSerializer,
    YAMLSerializer,
)

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
        "pydantic": PydanticSerializer(),
        "file": FileSerializer(),
    }

    def __init__(
        self,
        bootstrap_servers: Optional[Union[str, List[str]]] = None,
        client_id: Optional[str] = None,
        dynamic_group_id: bool = False,
        partition_scale: bool = False,
        **extra_config: Any,
    ):
        global _LOG_CONFIGURED
        if not _LOG_CONFIGURED:
            logger.add("wkafka.log", rotation="10 MB", level="INFO")
            _LOG_CONFIGURED = True

        self.bootstrap_servers = (
            os.environ.get("KAFKA_SERVER") or bootstrap_servers or "localhost:9092"
        )
        self.client_id = client_id or "wkafka-client"
        self.dynamic_group_id = dynamic_group_id
        self.partition_scale = partition_scale
        self.extra_config = extra_config
        self._consumers_registry: List[
            Tuple[KafkaConsumer, Callable, Dict[str, Any]]
        ] = []
        self._producer_instance: Optional[KafkaProducer] = None
        self._lock = threading.Lock()

    @property
    def consumers(self) -> List[Tuple[KafkaConsumer, Callable, Dict[str, Any]]]:
        return self._consumers_registry

    def _ensure_partitions(self, topic: str, target_count: int) -> None:
        """Dynamically scales the partition count of a topic to target_count using KafkaAdminClient."""
        try:
            from kafka.admin import KafkaAdminClient, NewPartitions, NewTopic

            admin_config = {
                "bootstrap_servers": self.bootstrap_servers,
                "client_id": f"{self.client_id}-admin",
            }
            for k in (
                "security_protocol",
                "sasl_mechanism",
                "sasl_plain_username",
                "sasl_plain_password",
            ):
                if k in self.extra_config:
                    admin_config[k] = self.extra_config[k]

            admin = KafkaAdminClient(**admin_config)
            try:
                consumer_temp = KafkaConsumer(
                    topic, bootstrap_servers=self.bootstrap_servers, **self.extra_config
                )
                partitions = consumer_temp.partitions_for_topic(topic)
                consumer_temp.close()

                if partitions is None:
                    logger.info(
                        f"🚀 [PARTITION SCALE] Creating topic '{topic}' with {target_count} initial partitions."
                    )
                    admin.create_topics(
                        [
                            NewTopic(
                                name=topic,
                                num_partitions=target_count,
                                replication_factor=1,
                            )
                        ]
                    )
                else:
                    current_count = len(partitions)
                    if current_count < target_count:
                        logger.info(
                            f"🚀 [PARTITION SCALE] Auto-scaling topic '{topic}' partitions from {current_count} to {target_count}."
                        )
                        admin.create_partitions(
                            {topic: NewPartitions(total_count=target_count)}
                        )
            finally:
                admin.close()
        except Exception as e:
            logger.warning(f"Could not auto-scale partitions for topic '{topic}': {e}")

    def _generate_group_id(self) -> str:
        return (
            f"wkafka-{uuid.uuid4().hex[:8]}"
            if self.dynamic_group_id
            else "wkafka-default-group"
        )

    def _get_producer(self) -> KafkaProducer:
        with self._lock:
            if self._producer_instance is None:
                compression = "snappy"
                try:
                    import snappy
                except ImportError:
                    compression = "gzip"
                    logger.info(
                        "Package 'python-snappy' not found; falling back to 'gzip' compression. "
                        "For maximum performance, install snappy via: pip install wkafka[snappy]"
                    )

                producer_config = {
                    "bootstrap_servers": self.bootstrap_servers,
                    "acks": int(os.environ.get("KAFKA_ACKS", "1")),
                    "compression_type": compression,
                    "value_serializer": lambda v: v,
                    "key_serializer": lambda k: (
                        k.encode("utf-8") if isinstance(k, str) else k
                    ),
                }
                producer_config.update(self.extra_config)

                self._producer_instance = KafkaProducer(**producer_config)
            return self._producer_instance

    def consumer(
        self,
        topic: Optional[Union[str, List[str]]] = None,
        pattern: Optional[str] = None,
        group_id: Optional[str] = None,
        key_filter: Optional[str] = None,
        format: str = "json",
        auto_commit: bool = True,
        max_retries: int = 0,
        retry_delay: float = 1.0,
        dlq_topic: Optional[str] = None,
        model: Optional[Any] = None,
        partition_scale: Optional[bool] = None,
        **kafka_kwargs: Any,
    ) -> Callable:
        def decorator(func: Callable) -> Callable:
            final_group_id = group_id or self._generate_group_id()

            config = {
                "bootstrap_servers": self.bootstrap_servers,
                "group_id": final_group_id,
                "auto_offset_reset": "latest",
                "enable_auto_commit": auto_commit,
            }
            config.update(kafka_kwargs)

            other_cfg = config.pop("other_config", None)
            if isinstance(other_cfg, dict):
                config.update(other_cfg)

            config.pop("value_type", None)
            config.pop("value_convert_to", None)

            for k, v in self.extra_config.items():
                if k not in kafka_kwargs:
                    config[k] = v

            topics_args = []
            if pattern:
                config["pattern"] = pattern
            elif isinstance(topic, list):
                topics_args = topic
            elif isinstance(topic, str):
                topics_args = [topic]

            consumer_instance = KafkaConsumer(*topics_args, **config)

            options = {
                "key_filter": key_filter,
                "format": format,
                "auto_commit": auto_commit,
                "max_retries": max_retries,
                "retry_delay": retry_delay,
                "dlq_topic": dlq_topic,
                "model": model,
                "partition_scale": partition_scale,
            }

            self._consumers_registry.append((consumer_instance, func, options))

            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                return func(*args, **kwargs)

            return wrapper

        return decorator

    def _execute_callback(self, func: Callable, msg_obj: Message) -> Any:
        """Executes sync or async callback functions."""
        if inspect.iscoroutinefunction(func):
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop and loop.is_running():
                future = asyncio.run_coroutine_threadsafe(func(msg_obj), loop)
                return future.result()
            else:
                return asyncio.run(func(msg_obj))
        else:
            return func(msg_obj)

    def _handle_message(
        self, consumer: KafkaConsumer, func: Callable, options: Dict[str, Any]
    ) -> None:
        key_filter = options.get("key_filter")
        data_format = options.get("format", "json")
        auto_commit = options.get("auto_commit", True)
        max_retries = options.get("max_retries", 0)
        retry_delay = options.get("retry_delay", 1.0)
        dlq_topic = options.get("dlq_topic")
        model = options.get("model")

        for raw_msg in consumer:
            if key_filter and raw_msg.key != key_filter:
                continue

            headers = {}
            if raw_msg.headers:
                for k, v in raw_msg.headers:
                    try:
                        val_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
                        if k == "metadata":
                            try:
                                meta_dict = json.loads(val_str)
                                if isinstance(meta_dict, dict):
                                    headers.update(meta_dict)
                            except Exception:
                                headers[k] = val_str
                        else:
                            headers[k] = val_str
                    except Exception:
                        headers[k] = v

            value = raw_msg.value
            if data_format in self._SERIALIZERS:
                try:
                    kwargs = {}
                    if model:
                        kwargs["model"] = model
                    value = self._SERIALIZERS[data_format].deserialize(value, **kwargs)
                except Exception as e:
                    logger.error(f"Failed to deserialize message in {data_format}: {e}")

            commit_callback = (lambda: consumer.commit()) if not auto_commit else None

            config_obj = getattr(consumer, "config", None)
            group_id = (
                config_obj.get("group_id", "wkafka-group")
                if isinstance(config_obj, dict)
                else "wkafka-group"
            )
            msg_obj = Message(
                value=value,
                topic=raw_msg.topic,
                group_id=group_id,
                offset=raw_msg.offset,
                key=(
                    raw_msg.key.decode("utf-8")
                    if raw_msg.key and isinstance(raw_msg.key, bytes)
                    else raw_msg.key
                ),
                headers=headers,
                _commit_fn=commit_callback,
            )

            success = False
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    result = self._execute_callback(func, msg_obj)
                    success = True
                    if isinstance(result, dict) and result.get("exit"):
                        return
                    break
                except Exception as e:
                    last_exception = e
                    logger.warning(
                        f"Consumer error (attempt {attempt + 1}/{max_retries + 1}): {e}"
                    )
                    if attempt < max_retries:
                        time.sleep(retry_delay * (2**attempt))

            if not success and dlq_topic:
                logger.error(
                    f"Routing message offset {msg_obj.offset} on topic {msg_obj.topic} to DLQ '{dlq_topic}'"
                )
                try:
                    with self.producer() as dlq_producer:
                        dlq_headers = dict(msg_obj.headers or {})
                        dlq_headers["x-error-message"] = str(last_exception)
                        dlq_headers["x-original-topic"] = msg_obj.topic
                        dlq_headers["x-original-offset"] = str(msg_obj.offset)
                        dlq_producer.send(
                            dlq_topic,
                            value=msg_obj.value,
                            key=msg_obj.key,
                            format=data_format,
                            headers=dlq_headers,
                        )
                except Exception as dlq_err:
                    logger.error(f"Failed to route message to DLQ: {dlq_err}")

    def run_consumers(
        self, block: bool = True, partition_scale: Optional[bool] = None, **kwargs: Any
    ) -> None:
        if not self._consumers_registry:
            logger.warning("No consumers registered. Nothing to run.")
            return

        should_auto_scale = (
            self.partition_scale if partition_scale is None else partition_scale
        )

        if should_auto_scale:
            topic_counts: Dict[str, int] = {}
            for consumer_inst, func, options in self._consumers_registry:
                topics = consumer_inst.subscription() or []
                for t in topics:
                    topic_counts[t] = topic_counts.get(t, 0) + 1

            for topic, count in topic_counts.items():
                self._ensure_partitions(topic, count)

        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=len(self._consumers_registry),
            thread_name_prefix="WKafkaConsumer",
        )

        futures = []
        for consumer, func, options in self._consumers_registry:
            futures.append(
                executor.submit(self._handle_message, consumer, func, options)
            )

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
        **kwargs: Any,
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

        return producer.send(
            topic, value=serialized_value, key=key, headers=kafka_headers
        )

    def producer(self) -> "WKafka":
        return self

    def __enter__(self) -> "WKafka":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._producer_instance:
            self._producer_instance.flush()
            self._producer_instance.close()
            self._producer_instance = None
