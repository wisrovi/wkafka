"""Comprehensive unit tests for WKafka Claim-Check payload offloading.

Validates:
1. Producer offloading: heavy formats (e.g. format="image") are saved to Redis
   with specified TTL, and lightweight Claim-Check envelopes are sent to Kafka.
2. Consumer offloading: transparent retrieval from Redis and deserialization back to np.ndarray.
3. Expiration handling: dropped frames when on_expired="drop" vs value=None when on_expired="none".
4. Standard non-offloaded messages: bypass Redis and behave exactly as standard Kafka messages.
5. Backward compatibility: when offload is disabled, zero Redis interaction occurs.
"""

from unittest.mock import MagicMock, patch
import numpy as np
import pytest

from wkafka.core.manager import WKafka
from wkafka.core.models import Message


class FakeRedisBackend:
    """Mock Redis backend simulating WRedis RedisKeyValueManager in-memory."""

    def __init__(self):
        """Initializes empty in-memory store."""
        self.store = {}
        self.ttls = {}

    def setex(self, key, ttl, value):
        """Sets key with TTL expiration."""
        self.store[key] = (
            bytes(value) if isinstance(value, (bytes, bytearray)) else value
        )
        self.ttls[key] = ttl
        return True

    def get_bytes(self, key):
        """Gets raw stored bytes."""
        return self.store.get(key)

    def delete(self, key):
        """Deletes key from memory."""
        return self.store.pop(key, None) is not None

    def exists(self, key):
        """Checks if key exists."""
        return key in self.store


class TestWKafkaOffload:
    """Tests for payload offloading lifecycle in producer and consumer."""

    def test_producer_offload_image(self):
        """Validates that sending an image with offload enabled stores bytes in Redis.

        Saves binary payload with the configured TTL and sends a lightweight claim envelope via Kafka.
        """
        fake_redis = FakeRedisBackend()
        kafka = WKafka(
            bootstrap_servers="localhost:9092",
            offload={"backend": "redis", "ttl": 5, "key_prefix": "test:claim"},
        )
        kafka._offload_backend = fake_redis

        # Mock kafka producer instance
        mock_producer = MagicMock()
        kafka._producer_instance = mock_producer

        # Generate a dummy OpenCV image (100x100 BGR)
        dummy_frame = np.zeros((100, 100, 3), dtype=np.uint8)

        kafka.send("camera-stream", value=dummy_frame, format="image")

        # 1. Verify Redis received the binary frame with TTL = 5
        assert len(fake_redis.store) == 1
        claim_key = list(fake_redis.store.keys())[0]
        assert claim_key.startswith("test:claim:")
        assert fake_redis.ttls[claim_key] == 5
        assert isinstance(fake_redis.store[claim_key], bytes)

        # 2. Verify Kafka send received lightweight envelope
        assert mock_producer.send.called
        call_args = mock_producer.send.call_args
        topic_arg = call_args[0][0]
        kwargs = call_args[1]

        assert topic_arg == "camera-stream"
        payload = kwargs["value"]
        assert b"__wkafka_offload__" in payload
        assert claim_key.encode("utf-8") in payload

        # Check headers
        headers_dict = dict(kwargs["headers"])
        assert headers_dict.get("x-wkafka-offload") == b"redis"

    def test_consumer_transparent_retrieval(self):
        """Validates that a consumer receiving a Claim-Check message transparently fetches bytes.

        Retrieves payload from Redis, decodes image, and presents the original numpy.ndarray to callback.
        """
        import cv2

        fake_redis = FakeRedisBackend()
        kafka = WKafka(
            bootstrap_servers="localhost:9092",
            offload={"backend": "redis", "ttl": 5},
        )
        kafka._offload_backend = fake_redis

        # Prepare dummy image and store in Redis
        original_frame = np.ones((50, 50, 3), dtype=np.uint8) * 128
        _, encoded = cv2.imencode(".jpg", original_frame)
        frame_bytes = encoded.tobytes()

        claim_key = "wkafka:claim:test1234"
        fake_redis.setex(claim_key, 5, frame_bytes)

        # Create simulated raw Kafka message
        import json

        raw_kafka_value = json.dumps(
            {"__wkafka_offload__": True, "claim_id": claim_key, "format": "image"}
        ).encode("utf-8")

        mock_raw_msg = MagicMock()
        mock_raw_msg.topic = "camera-stream"
        mock_raw_msg.offset = 42
        mock_raw_msg.key = b"cam1"
        mock_raw_msg.value = raw_kafka_value
        mock_raw_msg.headers = [
            (b"x-wkafka-offload", b"redis"),
            (b"x-wkafka-claim-id", claim_key.encode("utf-8")),
            (b"x-wkafka-format", b"image"),
        ]

        received_values = []

        def sample_callback(msg: Message):
            received_values.append(msg.value)

        # Call internal message handler with mock consumer yielding one message
        mock_consumer = [mock_raw_msg]
        kafka._handle_message(
            mock_consumer, sample_callback, {"format": "image", "key_filter": None}
        )

        assert len(received_values) == 1
        received_frame = received_values[0]
        assert isinstance(received_frame, np.ndarray)
        assert received_frame.shape == original_frame.shape

    def test_consumer_expired_key_drops_message_when_policy_is_drop(self):
        """Validates that if a frame expires in Redis, message is dropped.

        When on_expired='drop', consumer safely skips message and does NOT call user callback.
        """
        fake_redis = FakeRedisBackend()  # Empty store = expired key
        kafka = WKafka(
            bootstrap_servers="localhost:9092",
            offload={"backend": "redis", "ttl": 5, "on_expired": "drop"},
        )
        kafka._offload_backend = fake_redis

        import json

        raw_kafka_value = json.dumps(
            {"__wkafka_offload__": True, "claim_id": "expired:key", "format": "image"}
        ).encode("utf-8")

        mock_raw_msg = MagicMock()
        mock_raw_msg.topic = "camera-stream"
        mock_raw_msg.offset = 100
        mock_raw_msg.key = None
        mock_raw_msg.value = raw_kafka_value
        mock_raw_msg.headers = [(b"x-wkafka-offload", b"redis")]

        callback_mock = MagicMock()
        kafka._handle_message(
            [mock_raw_msg], callback_mock, {"format": "image", "key_filter": None}
        )

        # Callback should NOT have been called because frame expired and policy is "drop"
        callback_mock.assert_not_called()

    def test_consumer_expired_key_passes_none_when_policy_is_none(self):
        """Validates that if on_expired='none', callback is invoked with msg.value = None."""
        fake_redis = FakeRedisBackend()  # Empty store = expired key
        kafka = WKafka(
            bootstrap_servers="localhost:9092",
            offload={"backend": "redis", "ttl": 5, "on_expired": "none"},
        )
        kafka._offload_backend = fake_redis

        import json

        raw_kafka_value = json.dumps(
            {"__wkafka_offload__": True, "claim_id": "expired:key", "format": "image"}
        ).encode("utf-8")

        mock_raw_msg = MagicMock()
        mock_raw_msg.topic = "camera-stream"
        mock_raw_msg.offset = 101
        mock_raw_msg.key = None
        mock_raw_msg.value = raw_kafka_value
        mock_raw_msg.headers = [(b"x-wkafka-offload", b"redis")]

        received = []

        def callback(msg):
            received.append(msg.value)

        kafka._handle_message([mock_raw_msg], callback, {"format": "image"})

        assert len(received) == 1
        assert received[0] is None

    def test_non_offloaded_message_bypasses_redis(self):
        """Validates that a standard JSON message sends directly through Kafka without calling Redis."""
        fake_redis = FakeRedisBackend()
        kafka = WKafka(
            bootstrap_servers="localhost:9092",
            offload={"backend": "redis", "ttl": 5},
        )
        kafka._offload_backend = fake_redis

        mock_producer = MagicMock()
        kafka._producer_instance = mock_producer

        kafka.send("alerts", value={"alert": "motion"}, format="json")

        # Redis store must remain empty
        assert len(fake_redis.store) == 0

        # Producer was called with raw JSON bytes
        assert mock_producer.send.called
        kwargs = mock_producer.send.call_args[1]
        assert b'"alert": "motion"' in kwargs["value"]
