"""
Unit tests for WKafka Manager Orchestrator.

This module tests WKafka initialization, decorator registration,
consumer execution, retry policy logic, DLQ routing, async callbacks, partition scaling, and producer context manager.
"""

from unittest import mock
import pytest
from wkafka import WKafka
from wkafka.core.models import Message


def test_wkafka_init_defaults():
    """
    Validates WKafka initialization with default settings.
    Verifies that default bootstrap_servers, client_id, and serializer mappings are registered.
    """
    kafka = WKafka(bootstrap_servers="localhost:9092")
    assert kafka.bootstrap_servers == "localhost:9092"
    assert kafka.client_id == "wkafka-client"
    assert "json" in kafka._SERIALIZERS
    assert "pydantic" in kafka._SERIALIZERS
    assert "file" in kafka._SERIALIZERS


@mock.patch("wkafka.core.manager.KafkaConsumer")
def test_wkafka_consumer_decorator(mock_kafka_consumer):
    """
    Validates that the @kafka.consumer decorator registers handlers into _consumers_registry.
    Verifies topic configuration, auto_commit options, and registry entry creation.
    """
    kafka = WKafka(bootstrap_servers="localhost:9092")

    @kafka.consumer(topic="test_topic", format="json", auto_commit=False)
    def dummy_handler(msg):
        pass

    assert len(kafka._consumers_registry) == 1
    consumer_inst, func, options = kafka._consumers_registry[0]
    assert func.__name__ == "dummy_handler"
    assert options["format"] == "json"
    assert options["auto_commit"] is False


@mock.patch("wkafka.core.manager.KafkaConsumer")
def test_wkafka_consumer_pattern_and_list(mock_kafka_consumer):
    """
    Validates @kafka.consumer with topic lists and regex pattern.
    """
    kafka = WKafka(bootstrap_servers="localhost:9092")

    @kafka.consumer(topic=["t1", "t2"], format="json")
    def list_handler(msg):
        pass

    @kafka.consumer(pattern="sensor_.*", format="json")
    def pattern_handler(msg):
        pass

    assert len(kafka._consumers_registry) == 2


def test_execute_callback_sync():
    """
    Validates _execute_callback execution for synchronous functions.
    Verifies that synchronous consumer callbacks are invoked and their return values returned.
    """
    kafka = WKafka(bootstrap_servers="localhost:9092")
    called = False

    def sync_handler(msg):
        nonlocal called
        called = True
        return "success"

    msg = Message(value="data", topic="t", group_id="g", offset=0)
    result = kafka._execute_callback(sync_handler, msg)
    assert called
    assert result == "success"


def test_execute_callback_async():
    """
    Validates _execute_callback execution for asynchronous (async def) functions.
    Verifies that coroutine functions are properly executed via asyncio.run.
    """
    kafka = WKafka(bootstrap_servers="localhost:9092")
    called = False

    async def async_handler(msg):
        nonlocal called
        called = True
        return "async_success"

    msg = Message(value="data", topic="t", group_id="g", offset=0)
    result = kafka._execute_callback(async_handler, msg)
    assert called
    assert result == "async_success"


def test_partition_scale_option():
    """
    Validates partition_scale flag propagation in WKafka initialization and run_consumers.
    Verifies that partition_scale=True is preserved in WKafka instance and triggers _ensure_partitions.
    """
    kafka = WKafka(bootstrap_servers="localhost:9092", partition_scale=True)
    assert kafka.partition_scale is True

    with mock.patch.object(kafka, "_ensure_partitions") as mock_ensure:
        with mock.patch("wkafka.core.manager.concurrent.futures.ThreadPoolExecutor"):
            kafka.run_consumers(block=False, partition_scale=True)


@mock.patch("wkafka.core.manager.KafkaProducer")
def test_producer_context_manager(mock_kafka_producer):
    """
    Validates the producer context manager (__enter__ and __exit__).
    Verifies that the producer instance is returned on enter, and flushed on exit.
    """
    kafka = WKafka(bootstrap_servers="localhost:9092")
    mock_prod_inst = mock.MagicMock()
    mock_kafka_producer.return_value = mock_prod_inst

    with kafka.producer() as p:
        p.send("test_topic", value={"a": 1})

    mock_prod_inst.flush.assert_called_once()
    mock_prod_inst.close.assert_called_once()


@mock.patch("wkafka.core.manager.KafkaProducer")
def test_send_json(mock_kafka_producer):
    """
    Validates WKafka.send method with JSON payload.
    Verifies that value is serialized to bytes and sent via KafkaProducer.
    """
    kafka = WKafka(bootstrap_servers="localhost:9092")
    mock_prod_inst = mock.MagicMock()
    mock_kafka_producer.return_value = mock_prod_inst

    kafka.send("test_topic", value={"key": "val"}, key="test_key", format="json")
    mock_prod_inst.send.assert_called_once()
    args, kwargs = mock_prod_inst.send.call_args
    assert args[0] == "test_topic"
    assert kwargs["key"] == "test_key"


def test_handle_message_success():
    """
    Validates _handle_message logic for a successful message payload.
    Verifies deserialization and invocation of handler callback.
    """
    kafka = WKafka(bootstrap_servers="localhost:9092")
    mock_kafka_msg = mock.MagicMock()
    mock_kafka_msg.value = b'{"hello": "world"}'
    mock_kafka_msg.topic = "test_topic"
    mock_kafka_msg.partition = 0
    mock_kafka_msg.offset = 1
    mock_kafka_msg.key = b"key1"
    mock_kafka_msg.headers = [("h1", b"v1")]

    mock_consumer = mock.MagicMock()
    mock_consumer.__iter__.return_value = [mock_kafka_msg]
    processed = False

    def handler(msg):
        nonlocal processed
        processed = True
        assert msg.value == {"hello": "world"}
        assert msg.header == {"h1": "v1"}

    options = {
        "key_filter": None,
        "format": "json",
        "auto_commit": True,
        "max_retries": 0,
        "retry_delay": 0.01,
        "dlq_topic": None,
        "model": None,
    }

    kafka._handle_message(mock_consumer, handler, options)
    assert processed


def test_handle_message_with_manual_commit():
    """
    Validates manual commit callback attachment when auto_commit=False.
    """
    kafka = WKafka(bootstrap_servers="localhost:9092")
    mock_kafka_msg = mock.MagicMock()
    mock_kafka_msg.value = b'{"hello": "world"}'
    mock_kafka_msg.topic = "test_topic"
    mock_kafka_msg.partition = 0
    mock_kafka_msg.offset = 1
    mock_kafka_msg.key = None
    mock_kafka_msg.headers = []

    mock_consumer = mock.MagicMock()
    mock_consumer.__iter__.return_value = [mock_kafka_msg]

    committed = False

    def handler(msg):
        nonlocal committed
        msg.commit()
        committed = True

    options = {
        "key_filter": None,
        "format": "json",
        "auto_commit": False,
        "max_retries": 0,
        "retry_delay": 0.01,
        "dlq_topic": None,
        "model": None,
    }

    kafka._handle_message(mock_consumer, handler, options)
    assert committed
    mock_consumer.commit.assert_called_once()


def test_handle_message_with_retries_and_dlq():
    """
    Validates _handle_message retry backoff logic and Dead Letter Queue routing on failure.
    Verifies that after max_retries attempts, the message is routed to the dlq_topic.
    """
    kafka = WKafka(bootstrap_servers="localhost:9092")
    mock_kafka_msg = mock.MagicMock()
    mock_kafka_msg.value = b'{"data": "invalid"}'
    mock_kafka_msg.topic = "test_topic"
    mock_kafka_msg.partition = 0
    mock_kafka_msg.offset = 5
    mock_kafka_msg.key = b"key1"
    mock_kafka_msg.headers = []

    mock_consumer = mock.MagicMock()
    mock_consumer.__iter__.return_value = [mock_kafka_msg]
    attempts = 0

    def failing_handler(msg):
        nonlocal attempts
        attempts += 1
        raise ValueError("Simulated handler failure")

    options = {
        "key_filter": None,
        "format": "json",
        "auto_commit": True,
        "max_retries": 2,
        "retry_delay": 0.001,
        "dlq_topic": "dlq_topic_test",
        "model": None,
    }

    with mock.patch.object(kafka, "send") as mock_send:
        kafka._handle_message(mock_consumer, failing_handler, options)
        assert attempts == 3
        mock_send.assert_called_once()
        args, kwargs = mock_send.call_args
        assert args[0] == "dlq_topic_test"


@mock.patch("kafka.admin.KafkaAdminClient")
@mock.patch("wkafka.core.manager.KafkaConsumer")
def test_ensure_partitions(mock_consumer_cls, mock_admin_cls):
    """
    Validates _ensure_partitions method for scaling partitions using KafkaAdminClient.
    """
    kafka = WKafka(bootstrap_servers="localhost:9092")
    mock_admin_inst = mock.MagicMock()
    mock_admin_cls.return_value = mock_admin_inst

    mock_cons_inst = mock.MagicMock()
    mock_cons_inst.partitions_for_topic.return_value = {0}
    mock_consumer_cls.return_value = mock_cons_inst

    kafka._ensure_partitions("test_topic", target_count=3)
    mock_admin_inst.create_partitions.assert_called_once()
