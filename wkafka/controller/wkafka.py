import PIL
from PIL import Image
from dataclasses import dataclass
import os
import threading
import time
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

    def __init__(
        self,
        server: str | list,
        name: str = None,
        retry_delay: int = 10,
        max_retries: int = 3,
        dynamic_group_id: bool = False,
    ):
        """
        Initialize the CustomKafka class with the server address.

        Args:
            server (str): Kafka server address (e.g., "localhost:9092").
        """

        self.server = server
        self.name = name

        self.retry_delay = retry_delay
        self.max_retries = max_retries

        self.dynamic_group_id = dynamic_group_id

        self.consumers = []

    def _random_group_id(self) -> str:
        """
        Generate a random group ID for Kafka consumers.

        Returns:
            str: A random group ID.
        """
        if self.dynamic_group_id:
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
                # and register the processing function
                if group_id:
                    new_group_id = group_id
                else:
                    new_group_id = self._random_group_id() or key or "default"

                if isinstance(self.server, str):
                    self.server = [self.server]

                base_config = dict(
                    auto_offset_reset="latest",
                    bootstrap_servers=self.server,
                    enable_auto_commit=True,  # Reduce latencia al confirmar offsets automáticamente
                    fetch_max_bytes=52428800,  # Permite recuperar lotes más grandes (50MB)
                    group_id=new_group_id,
                    key_deserializer=lambda key: key.decode("utf-8") if key else None,
                    max_partition_fetch_bytes=10485760,  # Ajusta el tamaño de cada partición
                    value_deserializer=lambda x: x,
                )

                if other_config:
                    base_config.update(other_config)

                try:
                    real_topic = topic
                    if os.environ.get("wkafka_pytest", False):
                        real_topic = f"pytest.{topic}"
                        logging.debug("Mode test activate!")

                    consumer = KafkaConsumer(real_topic, **base_config)
                except errors.NoBrokersAvailable:
                    logging.error("NoBrokersAvailable")
                    raise Exception("NoBrokersAvailable")
                except Exception as e:
                    raise Exception("Problem with conection")

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
                        # 🔥 Decodifica más eficientemente
                        np_arr = np.frombuffer(data.value, np.uint8)
                        data.value = cv2.imdecode(
                            np_arr, cv2.IMREAD_COLOR
                        )  # IMREAD_UNCHANGED si usas PNG o WebP
                    except Exception as e:
                        print(f"Error en decodificación de imagen: {e}")

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

    def run_consumers(self, join: bool = True) -> None:
        """
        Start all registered Kafka consumers in separate threads.
        """

        threads = []

        for (
            consumer,
            process_func,
            key_filter,
            auto_value,
            real_topic,
        ) in self.consumers:
            thread = threading.Thread(
                target=self.__async_receiver,
                args=(consumer, process_func, key_filter, auto_value),
                name=f"Consumer-{str(uuid.uuid4())}",
            )

            threads.append(thread)

        logging.info("kafka consumers ready to receive data!")

        for thread in threads:
            thread.start()

        if join:
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
            if isinstance(self.server, str):
                self.server = [self.server]

            return KafkaProducer(
                acks=1,  # Minimiza la latencia asegurando que solo un broker confirme la escritura
                batch_size=65536,  # Aumenta el tamaño de los lotes (por defecto es 16KB, aquí lo subimos a 64KB)
                bootstrap_servers=self.server,
                buffer_memory=33554432,  # Aumenta el buffer del productor (32MB)
                compression_type="snappy",  # Usa compresión rápida para reducir tamaño de mensajes
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                linger_ms=5,  # Reduce el tiempo de espera antes de enviar lotes
                value_serializer=lambda v: v,
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
        header: Optional[dict] = None,
        verbose: bool = False,
    ):
        thread = threading.Thread(
            target=self.send,
            args=(topic, value, key, value_type, header, verbose),
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
        quality: int = 80,
        verbose: bool = False,
    ) -> None:
        """
        Send a message to a specified Kafka topic.

        Args:
            topic (str): The Kafka topic to send the message to.
            value (dict): The message value to send.
            key (Optional[str], optional): The message key. Defaults to None.
            value_type (Optional[str], optional): The type of value to send. Defaults to None.
            header (Optional[dict], optional): Additional headers to send. Defaults to None.
            quality (int, optional): The quality of the image (0-100) if value_type is "image". Defaults to 80.
            verbose (bool, optional): Whether to print debug messages. Defaults to False.
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
                if isinstance(value, Image.Image):
                    # 🔥 Manejo seguro de imágenes PIL (Pillow)
                    extra_headers["frame_width"], extra_headers["frame_height"] = (
                        value.size
                    )  # `.size` devuelve (width, height)

                elif isinstance(value, np.ndarray):
                    # 🔥 Verifica que sea una imagen válida de OpenCV
                    if (
                        value.ndim == 2
                    ):  # Imagen en escala de grises (sin canal de color)
                        extra_headers["frame_width"], extra_headers["frame_height"] = (
                            value.shape[1],
                            value.shape[0],
                        )
                        extra_headers["channels"] = 1  # Indicar que es escala de grises

                    elif value.ndim == 3 and value.shape[2] in [1, 3, 4]:
                        # Imagen a color (BGR/RGB o con canal alfa)
                        (
                            extra_headers["frame_width"],
                            extra_headers["frame_height"],
                            extra_headers["channels"],
                        ) = (value.shape[1], value.shape[0], value.shape[2])

                    else:
                        raise ValueError(
                            f"Formato de imagen OpenCV no válido: dimensiones {value.shape}"
                        )

                else:
                    raise TypeError(f"Tipo de dato no soportado: {type(value)}")

                if quality is not None and not (30 <= quality <= 100):
                    raise ValueError("Quality must be an integer between 30 and 100.")

                if quality is None or quality == 100:
                    # 🔥 Codifica la imagen sin compresión
                    _, buffer = cv2.imencode(".jpg", value)
                else:
                    # 🔥 Codifica la imagen con compresión optimizada
                    encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), quality]
                    _, buffer = cv2.imencode(".jpg", value, encode_param)

                # 🔥 Envía los metadatos y la imagen comprimida
                value = buffer.tobytes()

            if header:
                extra_headers.update(header)

                header = [("metadata", self._serializer(extra_headers))]
                if self.name:
                    header.append(("name", self.name))

            for attempt in range(self.max_retries):
                try:
                    future = self.producer_instance.send(
                        topic,
                        value=value,
                        key=key.encode("utf-8") if key else None,
                        headers=header,
                    )
                    self.producer_instance.flush()  # Force immediate sending

                    if verbose:
                        logging.info(f"Message sent: {future.get(timeout=10)}")
                    break
                except errors.KafkaTimeoutError:
                    logging.error("Kafka timeout, retrying...")
                    time.sleep(self.retry_delay)
                except Exception as e:
                    logging.error(f"Producer error: {e}")
                    time.sleep(self.retry_delay)

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
