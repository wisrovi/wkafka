"""
Unit tests for legacy Wkafka Controller Bridge.

This module tests initialization, consumer decorator parameters,
and send method of the legacy Wkafka class.
"""

from unittest import mock
from wkafka.controller.wkafka import Wkafka


def test_legacy_wkafka_init():
    """
    Validates legacy Wkafka controller initialization.
    Verifies server alias mapping to bootstrap_servers and name to client_id.
    """
    client = Wkafka(server="localhost:9092", name="legacy_app", partition_scale=True)
    assert client.bootstrap_servers == "localhost:9092"
    assert client.client_id == "legacy_app"
    assert client.partition_scale is True


@mock.patch("wkafka.core.manager.KafkaConsumer")
def test_legacy_wkafka_consumer_and_send(mock_kafka_consumer):
    """
    Validates legacy Wkafka consumer registration and send method.
    Verifies parameter mapping (value_type -> format, key -> key_filter, header -> headers).
    """
    client = Wkafka(server="localhost:9092")

    @client.consumer(topic="legacy_topic", value_type="json", key="filter_key")
    def legacy_handler(msg):
        pass

    assert len(client._consumers_registry) == 1
    _, _, options = client._consumers_registry[0]
    assert options["format"] == "json"
    assert options["key_filter"] == "filter_key"

    with mock.patch("wkafka.core.manager.WKafka.send") as mock_super_send:
        client.send("legacy_topic", value={"a": 1}, header={"h": 2})
        mock_super_send.assert_called_once_with(
            topic="legacy_topic",
            value={"a": 1},
            key=None,
            format="json",
            headers={"h": 2},
        )


def test_legacy_module_import_structure():
    """
    Validates legacy import structure compatibility (wkafka.wkafka).
    Ensures code expecting wkafka.wkafka (or from wkafka.wkafka import Wkafka) works.
    """
    import wkafka

    assert hasattr(wkafka, "wkafka")
    assert hasattr(wkafka.wkafka, "Wkafka")
    assert hasattr(wkafka.wkafka, "WKafka")
