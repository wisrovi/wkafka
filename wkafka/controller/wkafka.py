from PIL import Image
from dataclasses import dataclass
import os
import threading
import time
import concurrent
import cv2
from kafka import KafkaConsumer, KafkaProducer, errors
import json
import random
from loguru import logger  # Usamos loguru en lugar de logging
import uuid
from functools import wraps
from typing import Any, Callable, Dict, Optional

import numpy as np


@dataclass
class Consumer_data:
    """
    A dataclass to store consumer message data.
    """

    value: bytes = None
    key: str = None
    topic: str = None
    group_id: str = None
    header: Optional[dict] = None
    offset: int = None


class Wkafka:
    """
    A class that manages Kafka producers and consumers with a decorator-based approach
    for easy message processing and sending.
    """

    type_formats = ["json", "file", "image"]

    is_producer = False

    def __init__(
        self,
        server: str | list,
        name: str = None,
        retry_delay: int = 10,
        max_retries: int = 3,
        dynamic_group_id: bool = False,
    ):
        """
        Initialize the Wkafka class with the server address.

        Args:
            server (str | list): Kafka server address (e.g., "localhost:9092").
            name (str, optional): Name of the Kafka instance.
            retry_delay (int, optional): Delay between retries in seconds.
            max_retries (int, optional): Maximum number of retries.
            dynamic_group_id (bool, optional): Use dynamic group IDs for consumers.
        """
        self.server = server
        self.name = name or "default"
        self.retry_delay = retry_delay
        self.max_retries = max_retries
        self.dynamic_group_id = dynamic_group_id
        self.consumers = []

        # Configurar loguru
        logger.add("wkafka.log", rotation="10 MB", level="DEBUG")

    def _random_group_id(self) -> str:
        """
        Generate a random group ID for Kafka consumers.

        Returns:
            str: A random group ID.
        """
        if self.dynamic_group_id:
            return f"{uuid.uuid4()}-{random.randint(1, 100)}"
        return "default"

    def _deserialize(self, message: bytes, format: str = "json") -> Any:
        """
        Deserialize a message from bytes to a specific format.

        Args:
            message (bytes): The message to deserialize.
            format (str): The format of the message (e.g., "json").

        Returns:
            Any: The deserialized message.
        """
        if format == "json":
            return json.loads(message.decode("utf-8"))
        elif format == "yaml":
            import yaml

            return yaml.safe_load(message.decode("utf-8"))
        else:
            raise ValueError(f"Unsupported deserialization format: {format}")

    def consumer(
        self,
        topic: str,
        group_id: Optional[str] = None,
        key: Optional[str] = None,
        value_type: Optional[str] = None,
        other_config: Optional[dict] = None,
    ) -> Callable:
        """
        Decorator to register a processing function as a Kafka consumer.

        Args:
            topic (str): The Kafka topic to subscribe to.
            group_id (Optional[str], optional): Consumer group ID.
            key (Optional[str], optional): A key filter.
            value_type (Optional[str], optional): Type of value to process.
            other_config (Optional[dict], optional): Additional Kafka consumer configurations.

        Returns:
            Callable: A decorated function that processes Kafka messages.
        """
        if value_type not in self.type_formats:
            raise ValueError(f"Invalid value_type. Must be one of: {self.type_formats}")

        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            def create_consumer():
                if group_id:
                    new_group_id = group_id
                else:
                    new_group_id = self._random_group_id()

                if isinstance(self.server, str):
                    self.server = [self.server]

                base_config = {
                    "auto_offset_reset": "latest",
                    "bootstrap_servers": self.server,
                    "enable_auto_commit": True,
                    "fetch_max_bytes": 52428800,
                    "group_id": new_group_id,
                    "key_deserializer": lambda key: (
                        key.decode("utf-8") if key else None
                    ),
                    "value_deserializer": lambda x: x,
                }

                if other_config:
                    base_config.update(other_config)

                try:
                    real_topic = topic
                    if os.environ.get("WKAFKA_PYTEST", False):
                        real_topic = f"pytest.{topic}"
                        logger.debug("Test mode activated!")

                    consumer = KafkaConsumer(real_topic, **base_config)
                except errors.NoBrokersAvailable:
                    logger.error("No brokers available")
                    raise Exception("No brokers available")
                except Exception as e:
                    logger.error(f"Error connecting to Kafka: {e}")
                    raise

                self.consumers.append((consumer, func, key, value_type, real_topic))

            create_consumer()

            return wrapper

        return decorator

    def __async_receiver(
        self,
        consumer: KafkaConsumer,
        process_func: Callable,
        key_filter: Optional[str],
        auto_value: Optional[str],
    ) -> None:
        """
        Handle asynchronous receiving of Kafka messages in a separate thread.

        Args:
            consumer (KafkaConsumer): The Kafka consumer instance.
            process_func (Callable): The function to process received messages.
            key_filter (Optional[str]): A key filter to selectively process messages.
        """
        for message in consumer:
            if key_filter is None or message.key == key_filter:
                headers = dict(message.headers) if message.headers else {}
                headers = {k: v.decode("utf-8") for k, v in headers.items()}

                data = Consumer_data(
                    value=message.value,
                    key=message.key,
                    topic=message.topic,
                    group_id=consumer.config["group_id"],
                    header=headers,
                    offset=message.offset,
                )

                try:
                    if data.header:
                        data.header = self._deserialize(
                            data.header.get("header", ""), format="json"
                        )
                except Exception:
                    pass

                if auto_value == "json":
                    try:
                        data.value = self._deserialize(data.value, format="json")
                    except Exception:
                        pass
                elif auto_value == "image":
                    try:
                        np_arr = np.frombuffer(data.value, np.uint8)
                        data.value = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
                    except Exception as e:
                        logger.error(f"Error decoding image: {e}")

                results = process_func(data)

                if (
                    results
                    and isinstance(results, dict)
                    and "exit" in results
                    and results["exit"]
                ):
                    break

    def run_consumers(self, join: bool = True) -> None:
        """
        Start all registered Kafka consumers using ThreadPoolExecutor.

        Args:
            join (bool): Wait for all threads to finish before exiting.
        """
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(self.consumers)
        ) as executor:
            futures = []

            for (
                consumer,
                process_func,
                key_filter,
                auto_value,
                real_topic,
            ) in self.consumers:
                future = executor.submit(
                    self.__async_receiver,
                    consumer,
                    process_func,
                    key_filter,
                    auto_value,
                )
                futures.append(future)

            if join:
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Consumer thread failed: {e}")

    def producer(self) -> "Wkafka":
        """
        Method to use the class as a context manager.

        Returns:
            Wkafka: The instance of the class itself.
        """
        self.is_producer = True
        return self

    def _create_producer(self) -> KafkaProducer:
        """
        Create a Kafka producer instance with necessary configurations.

        Returns:
            KafkaProducer: The configured Kafka producer instance.
        """
        if isinstance(self.server, str):
            self.server = [self.server]

        # Verificar si snappy está disponible
        compression_type = self._get_compression_type()

        return KafkaProducer(
            acks=1,
            batch_size=65536,
            bootstrap_servers=self.server,
            buffer_memory=33554432,
            compression_type=compression_type,
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            linger_ms=5,
            value_serializer=lambda v: v,
        )

    def _get_compression_type(self) -> str:
        """
        Determine the compression type to use based on availability.

        Returns:
            str: The compression type to use ('snappy', 'gzip', or 'none').
        """
        try:
            # Intenta importar python-snappy
            import snappy

            logger.info("Snappy compression is available.")
            return "snappy"
        except ImportError:
            logger.warning(
                "Snappy library not found. Falling back to gzip compression."
            )
            return "gzip"
        except Exception as e:
            logger.error(f"Error while checking Snappy availability: {e}")
            logger.warning("Falling back to no compression.")
            return "none"

    def send(
        self,
        topic: str,
        value: Any,
        key: Optional[str] = None,
        value_type: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None,
        quality: int = 80,
        verbose: bool = False,
    ) -> None:
        """
        Send a message to a specified Kafka topic.
        """
        if not hasattr(self, "is_producer"):
            raise Exception("You must use the 'producer' method as a context manager.")

        if value_type not in self.type_formats:
            raise ValueError(f"Invalid value_type. Must be one of: {self.type_formats}")

        if headers and not isinstance(headers, dict):
            raise ValueError("Headers must be a dictionary.")

        if not hasattr(self, "producer_instance"):
            self.producer_instance = self._create_producer()

        extra_headers = {}

        if value_type == "json":
            assert isinstance(value, dict), "Value must be a dictionary."
            value = json.dumps(value).encode("utf-8")
        elif value_type == "file":
            with open(value, "rb") as f:
                value = f.read()
        elif value_type == "image":
            if isinstance(value, Image.Image):
                extra_headers["frame_width"], extra_headers["frame_height"] = value.size
            elif isinstance(value, np.ndarray):
                if value.ndim == 2:
                    extra_headers["frame_width"], extra_headers["frame_height"] = (
                        value.shape[1],
                        value.shape[0],
                    )
                    extra_headers["channels"] = 1
                elif value.ndim == 3 and value.shape[2] in [1, 3, 4]:
                    (
                        extra_headers["frame_width"],
                        extra_headers["frame_height"],
                        extra_headers["channels"],
                    ) = (value.shape[1], value.shape[0], value.shape[2])
                else:
                    raise ValueError(f"Invalid OpenCV image format: {value.shape}")
                _, encoded_image = cv2.imencode(
                    ".jpg", value, [int(cv2.IMWRITE_JPEG_QUALITY), quality]
                )
                value = encoded_image.tobytes()
            else:
                raise TypeError(f"Unsupported data type: {type(value)}")

        # Convertir el diccionario de cabeceras en una lista de tuplas (str, bytes)
        if headers:
            headers.update(extra_headers)
            # Asegurarse de que todos los valores sean bytes
            headers = [(k, str(v).encode("utf-8")) for k, v in headers.items()]
        else:
            headers = [(k, str(v).encode("utf-8")) for k, v in extra_headers.items()]

        try:
            self.producer_instance.send(topic, value=value, key=key, headers=headers)
            logger.info(f"Message sent to topic: {topic}")
        except Exception as e:
            logger.error(f"Failed to send message: {e}")

    def async_send(
        self,
        topic: str,
        value: Any,
        key: Optional[str] = None,
        value_type: Optional[str] = None,
        header: Optional[Dict[str, Any]] = None,
        verbose: bool = False,
    ) -> None:
        """
        Send a message asynchronously to a specified Kafka topic.

        Args:
            topic (str): The Kafka topic to send the message to.
            value (Any): The message value to send.
            key (Optional[str]): The message key.
            value_type (Optional[str]): The type of value to send.
            header (Optional[Dict[str, Any]]): Additional headers to send.
            verbose (bool): Whether to print debug messages.
        """
        thread = threading.Thread(
            target=self.send,
            args=(topic, value, key, value_type, header, verbose),
            name=f"Producer-{uuid.uuid4()}",
        )
        thread.start()

    def __enter__(self) -> "Wkafka":
        """
        Enter the context manager, creating a producer instance.

        Returns:
            Wkafka: The instance of the class.
        """
        self.producer_instance = self._create_producer()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """
        Exit the context manager, closing the producer instance.

        Args:
            exc_type: Exception type if an error occurred.
            exc_value: Exception value if an error occurred.
            traceback: Traceback information if an error occurred.
        """
        if hasattr(self, "producer_instance"):
            self.producer_instance.close()
            logger.info("Producer connection closed.")
        else:
            logger.warning("No producer instance found.")
