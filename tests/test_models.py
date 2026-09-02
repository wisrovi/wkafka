"""
Unit tests for WKafka Data Models.

This module tests the Message dataclass, headers alias property,
and manual offset commit callback functionality.
"""

from wkafka.core.models import Message


def test_message_attributes_and_header_alias():
    """
    Validates Message initialization and property aliases.
    Verifies that value, topic, group_id, offset, key, and headers are set correctly,
    and that the legacy .header property returns the headers dictionary.
    """
    headers_dict = {"content-type": "application/json"}
    msg = Message(
        value={"foo": "bar"},
        topic="test_topic",
        group_id="test_group",
        offset=10,
        key="key1",
        headers=headers_dict,
    )
    
    assert msg.value == {"foo": "bar"}
    assert msg.topic == "test_topic"
    assert msg.group_id == "test_group"
    assert msg.offset == 10
    assert msg.key == "key1"
    assert msg.headers == headers_dict
    assert msg.header == headers_dict


def test_message_manual_commit_callback():
    """
    Validates Message manual offset commit callback invocation.
    Verifies that calling msg.commit() triggers the internal _commit_fn callback.
    """
    committed = False

    def mock_commit():
        nonlocal committed
        committed = True

    msg = Message(
        value="data",
        topic="test_topic",
        group_id="test_group",
        offset=5,
        _commit_fn=mock_commit,
    )

    assert not committed
    msg.commit()
    assert committed
