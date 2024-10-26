from dataclasses import dataclass
import threading
import cv2
from kafka import KafkaConsumer, KafkaProducer, errors
import json
import random
import logging
import uuid
from functools import wraps
from typing import Callable, Optional

import numpy as np


@dataclass
class Consumer_data:
    value: bytes = None
    key: str = None
    topic: str = None
    group_id: str = None
    header: str = None
    offset: int = None


class Wkafka:
    """
    A class that manages Kafka producers and consumers with a decorator-based approach
    for easy message processing and sending.
    """

    type_formats = ["json", "file", "image"]

    is_producer = False

    def __init__(self, server: str, name: str = None):
        """
        Initialize the CustomKafka class with the server address.

        Args:
            server (str): Kafka server address (e.g., "localhost:9092").
        """

        self.server = server
        self.name = name

        self.consumers = []

    def _random_group_id(self) -> str:
        """
        Generate a random group ID for Kafka consumers.

        Returns:
            str: A random group ID.
        """
        return f"{str(uuid.uuid4())}-{str(random.randint(1, 100))}"

    """
    Consumer Section
    """

    def _deserialize(self, message: bytes) -> dict:
        """
        Deserialize a message from bytes to a dictionary.

        Args:
            message (bytes): The message to deserialize.

        Returns:
            dict: The deserialized message as a dictionary.
        """

        return json.loads(message.decode("utf-8"))

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
            group_id (Optional[str], optional): Consumer group ID. Defaults to None (randomly generated).
            key (Optional[str], optional): A key filter; if provided, only messages with this key will be processed.

        Returns:
            Callable: A decorated function that processes Kafka messages.
        """

        if value_type not in self.type_formats:
            raise Exception(
                f"Invalid value_convert_to type. Must be one of: {self.type_formats}"
            )

        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            def create_consumer():
                # Create a new consumer with a specified or random group ID
                new_group_id = group_id or self._random_group_id()

                base_config = dict(
                    bootstrap_servers=self.server,
                    auto_offset_reset="latest",
                    value_deserializer=lambda x: x,
                    key_deserializer=lambda key: key.decode("utf-8") if key else None,
                    group_id=new_group_id,
                )

                if other_config:
                    base_config.update(other_config)

                try:
                    consumer = KafkaConsumer(topic, **base_config)
                except errors.NoBrokersAvailable:
                    logging.error("NoBrokersAvailable")
                    raise Exception("NoBrokersAvailable")
                except Exception as e:
                    raise Exception("Problem with conection")
                
                self.consumers.append((consumer, func, key, value_type))

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

        Note: return {"exit"=True} for to finished the receiver
        """

        for message in consumer:
            # Check if the message key matches the filter or if no filter is set
            if key_filter is None or message.key == key_filter:
                data = Consumer_data(
                    value=message.value,
                    key=message.key,
                    topic=message.topic,
                    group_id=consumer.config["group_id"],
                    header=message.headers,
                    offset=message.offset,
                )

                try:
                    new_header = self._deserialize(data.header[0][1])
                    data.header = new_header
                except:
                    pass

                if auto_value == "json":
                    try:
                        data.value = self._deserialize(data.value)
                    except:
                        pass
                elif auto_value == "image":
                    try:
                        np_arr = np.frombuffer(data.value, np.uint8)
                        data.value = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
                    except:
                        pass

                results = process_func(data)

                # if results == {"exit"=True}
                if (
                    results
                    and isinstance(results, dict)
                    and "exit" in results
                    and isinstance(results["exit"], bool)
                    and results["exit"]
                ):
                    break

    def run_consumers(self) -> None:
        """
        Start all registered Kafka consumers in separate threads.
        """

        threads = []

        for consumer, process_func, key_filter, auto_value in self.consumers:
            thread = threading.Thread(
                target=self.__async_receiver,
                args=(consumer, process_func, key_filter, auto_value),
                name=f"Consumer-{str(uuid.uuid4())}",
            )

            threads.append(thread)

        for thread in threads:
            thread.start()
            
        logging.info("kafka consumers ready to receive data!")

        for thread in threads:
            thread.join()

    """
    Producer Section
    """

    def producer(self) -> "Wkafka":
        """
        Method to use the class as a context manager.

        Returns:
            CustomKafka: The instance of the class itself.
        """

        self.is_producer = True

        return self

    def _create_producer(self) -> KafkaProducer:
        """
        Create a Kafka producer instance with necessary configurations.

        Returns:
            KafkaProducer: The configured Kafka producer instance.
        """
        
        try:
            return KafkaProducer(
                bootstrap_servers=self.server,
                value_serializer=lambda v: v,
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            )
        except errors.NoBrokersAvailable:
            logging.error("NoBrokersAvailable")
            raise Exception("NoBrokersAvailable")

    def _serializer(self, value: dict) -> bytes:
        """
        Serialize a dictionary to JSON format.

        Args:
            value (dict): The dictionary to serialize.

        Returns:
            bytes: The serialized JSON object.
        """

        return json.dumps(value).encode("utf-8")

    def async_send(
        self,
        topic: str,
        value: dict,
        key: Optional[str] = None,
        value_type: Optional[str] = None,
        headers: Optional[dict] = None,
        verbose: bool = False,
    ):
        thread = threading.Thread(
            target=self.send,
            args=(topic, value, key, value_type, headers, verbose),
            name=f"Consumer-{str(uuid.uuid4())}",
        )
        thread.start()

    def send(
        self,
        topic: str,
        value: dict,
        key: Optional[str] = None,
        value_type: Optional[str] = None,
        header: Optional[dict] = None,
        verbose: bool = False,
    ) -> None:
        """
        Send a message to a specified Kafka topic.

        Args:
            topic (str): The Kafka topic to send the message to.
            value (dict): The message payload to send.
            key (Optional[str], optional): The key for the message. Defaults to None.
        """

        if not hasattr(self, "is_producer"):
            raise Exception("You must use the 'producer' method as a context manager.")

        if value_type not in self.type_formats:
            raise Exception(f"Invalid value_type. Must be one of: {self.type_formats}")

        if header and not isinstance(header, dict):
            raise Exception("Headers must be a dictionary.")

        try:
            if not hasattr(self, "producer_instance"):
                self.producer_instance = self._create_producer()

            extra_headers = {}

            if value_type == "json":
                assert isinstance(value, dict), "Value must be a dictionary."
                value = self._serializer(value)
            elif value_type == "file":
                with open(value, "rb") as f:
                    value = f.read()
            elif value_type == "image":
                frame_height, frame_width, _ = value.shape

                extra_headers["frame_width"] = frame_width
                extra_headers["frame_height"] = frame_height

                _, buffer = cv2.imencode(".jpg", value)
                value = buffer.tobytes()

            if header:
                extra_headers.update(header)

                header = [("metadata", self._serializer(extra_headers))]
                if self.name:
                    header.append(("name", self.name))

            self.producer_instance.send(
                topic,
                value=value,
                key=key.encode("utf-8") if key else None,
                headers=header,
            )
            self.producer_instance.flush()  # Force immediate sending

            if verbose:
                print(f"Message sent to topic '{topic}': {value}, key: {key}")
        except Exception as e:
            print(f"Error sending message to topic '{topic}': {e}")

    def __enter__(self) -> "Wkafka":
        """
        Enter the context manager, creating a producer instance.

        Returns:
            CustomKafka: The instance of the class.
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
            # print("Producer connection closed.")
