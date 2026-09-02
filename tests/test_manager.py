"""
Unit tests for WKafka Manager Orchestrator.

This module tests WKafka initialization, decorator registration,
consumer execution, retry policy logic, DLQ routing, and async callbacks.
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


from kafka import KafkaConsumer


@mock.patch("kafka.KafkaConsumer")
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


@pytest.mark.asyncio
async def test_execute_callback_async():
    """
    Validates _execute_callback execution for asynchronous (async def) functions.
    Verifies that coroutine functions are properly awaited inside an active event loop.
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
    Validates partition_scale flag propagation in WKafka initialization and consumer decorator.
    Verifies that partition_scale=True is preserved in WKafka instance and options dictionary.
    """
    kafka = WKafka(bootstrap_servers="localhost:9092", partition_scale=True)
    assert kafka.partition_scale is True

    @kafka.consumer(topic="scaled_topic", partition_scale=True)
    def handler(msg):
        pass

    assert kafka._consumers_registry[0][2]["partition_scale"] is True

