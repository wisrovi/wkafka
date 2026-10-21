"""Unit tests for WKafka Manager Orchestrator.

This module tests WKafka initialization, decorator registration,
consumer execution, retry policy logic, DLQ routing, async callbacks, partition scaling, and producer context manager.
"""

from unittest import mock

from wkafka import WKafka
from wkafka.core.models import Message


def test_wkafka_init_defaults():
    """Validates WKafka initialization with default settings.

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
    """Validates that the @kafka.consumer decorator registers handlers into _consumers_registry.

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
    """Validates @kafka.consumer with topic lists and regex pattern."""
    kafka = WKafka(bootstrap_servers="localhost:9092")

    @kafka.consumer(topic=["t1", "t2"], format="json")
    def list_handler(msg):
        pass

    @kafka.consumer(pattern="sensor_.*", format="json")
    def pattern_handler(msg):
        pass

    assert len(kafka._consumers_registry) == 2


def test_execute_callback_sync():
    """Validates _execute_callback execution for synchronous functions.

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
    """Validates _execute_callback execution for asynchronous (async def) functions.

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
    """Validates partition_scale flag propagation in WKafka initialization and run_consumers.

    Verifies that partition_scale=True is preserved in WKafka instance and triggers _ensure_partitions.
    """
    kafka = WKafka(bootstrap_servers="localhost:9092", partition_scale=True)
    assert kafka.partition_scale is True

    with mock.patch.object(kafka, "_ensure_partitions") as mock_ensure:
        with mock.patch("wkafka.core.manager.concurrent.futures.ThreadPoolExecutor"):
            kafka.run_consumers(block=False, partition_scale=True)


@mock.patch("wkafka.core.manager.KafkaProducer")
def test_producer_context_manager(mock_kafka_producer):
    """Validates the producer context manager (__enter__ and __exit__).

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
    """Validates WKafka.send method with JSON payload.

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
    """Validates _handle_message logic for a successful message payload.

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
    """Validates manual commit callback attachment when auto_commit=False."""
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
    """Validates _handle_message retry backoff logic and Dead Letter Queue routing on failure.

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
    """Validates _ensure_partitions method for scaling partitions using KafkaAdminClient."""
    kafka = WKafka(bootstrap_servers="localhost:9092")
    mock_admin_inst = mock.MagicMock()
    mock_admin_cls.return_value = mock_admin_inst

    mock_cons_inst = mock.MagicMock()
    mock_cons_inst.partitions_for_topic.return_value = {0}
    mock_consumer_cls.return_value = mock_cons_inst

    kafka._ensure_partitions("test_topic", target_count=3)
    mock_admin_inst.create_partitions.assert_called_once()


@mock.patch("kafka.admin.KafkaAdminClient")
@mock.patch("wkafka.core.manager.KafkaConsumer")
def test_ensure_partitions_create_topic(mock_consumer_cls, mock_admin_cls):
    """Validates _ensure_partitions when topic does not exist (partitions is None).

    Verifies admin.create_topics is called.
    """
    kafka = WKafka(bootstrap_servers="localhost:9092")
    mock_admin_inst = mock.MagicMock()
    mock_admin_cls.return_value = mock_admin_inst

    mock_cons_inst = mock.MagicMock()
    mock_cons_inst.partitions_for_topic.return_value = None
    mock_consumer_cls.return_value = mock_cons_inst

    kafka._ensure_partitions("new_topic", target_count=4)
    mock_admin_inst.create_topics.assert_called_once()


def test_ensure_partitions_exception():
    """Validates _ensure_partitions error handling when KafkaAdminClient raises an exception.

    Ensures that retries exhaust gracefully and warning is logged without crashing the app.
    """
    kafka = WKafka(bootstrap_servers="localhost:9092")
    with mock.patch(
        "kafka.admin.KafkaAdminClient", side_effect=Exception("Admin error")
    ):
        # Should attempt retries, log warning and not crash
        kafka._ensure_partitions("test_topic", target_count=2, max_retries=2)


@mock.patch("kafka.admin.KafkaAdminClient")
def test_ensure_partitions_describe_topics_success(mock_admin_cls):
    """Validates _ensure_partitions when describe_topics succeeds directly.

    Verifies that create_partitions is called and KafkaConsumer fallback is bypassed.
    """
    kafka = WKafka(bootstrap_servers="localhost:9092")
    mock_admin_inst = mock.MagicMock()
    mock_admin_inst.describe_topics.return_value = [
        {"error_code": 0, "topic": "test_topic", "partitions": [0]}
    ]
    mock_admin_cls.return_value = mock_admin_inst

    kafka._ensure_partitions("test_topic", target_count=3, max_retries=1)
    mock_admin_inst.create_partitions.assert_called_once()


@mock.patch("kafka.admin.KafkaAdminClient")
def test_ensure_partitions_retry_on_node_not_ready(mock_admin_cls):
    """Validates _ensure_partitions retry mechanism when NodeNotReadyError is raised.

    Succeeds on subsequent retry attempt.
    """
    from kafka.errors import NodeNotReadyError

    kafka = WKafka(bootstrap_servers="localhost:9092")
    mock_admin_inst = mock.MagicMock()
    mock_admin_inst.describe_topics.return_value = [
        {"error_code": 0, "topic": "test_topic", "partitions": [0]}
    ]

    # First call raises NodeNotReadyError, second call succeeds
    mock_admin_cls.side_effect = [
        NodeNotReadyError("Node not ready"),
        mock_admin_inst,
    ]

    kafka._ensure_partitions("test_topic", target_count=2, max_retries=2)
    assert mock_admin_cls.call_count == 2
    mock_admin_inst.create_partitions.assert_called_once()


def test_handle_message_key_filter_skip_and_header_decode_error():
    """Validates _handle_message skipping messages that do not match key_filter.

    Also handles non-utf8 headers cleanly.
    """
    kafka = WKafka(bootstrap_servers="localhost:9092")
    msg_skip = mock.MagicMock()
    msg_skip.key = b"wrong_key"

    msg_process = mock.MagicMock()
    msg_process.key = b"target_key"
    msg_process.value = b'{"a": 1}'
    msg_process.topic = "test_topic"
    msg_process.partition = 0
    msg_process.offset = 2
    msg_process.headers = [("bad_header", b"\xff\xfe\xfd")]

    mock_consumer = mock.MagicMock()
    mock_consumer.__iter__.return_value = [msg_skip, msg_process]

    processed = False

    def handler(msg):
        nonlocal processed
        processed = True
        assert msg.header["bad_header"] == b"\xff\xfe\xfd"

    options = {"key_filter": b"target_key", "format": "json", "auto_commit": True}
    kafka._handle_message(mock_consumer, handler, options)
    assert processed


def test_handle_message_deserialization_and_dlq_errors():
    """Validates _handle_message error logs when deserialization fails and DLQ send fails."""
    kafka = WKafka(bootstrap_servers="localhost:9092")
    msg_bad = mock.MagicMock()
    msg_bad.key = None
    msg_bad.value = b"invalid json data"
    msg_bad.topic = "test_topic"
    msg_bad.partition = 0
    msg_bad.offset = 3
    msg_bad.headers = None

    mock_consumer = mock.MagicMock()
    mock_consumer.__iter__.return_value = [msg_bad]

    def failing_handler(msg):
        raise ValueError("Callback crash")

    options = {
        "format": "json",
        "auto_commit": True,
        "max_retries": 0,
        "retry_delay": 0.001,
        "dlq_topic": "dlq_topic_test",
    }

    with mock.patch.object(kafka, "send", side_effect=Exception("DLQ send failed")):
        kafka._handle_message(mock_consumer, failing_handler, options)


def test_run_consumers_empty_and_execution():
    """Validates run_consumers when no consumers registered, and when consumers run in ThreadPoolExecutor."""
    kafka = WKafka(bootstrap_servers="localhost:9092")
    # Empty run
    kafka.run_consumers()

    # Registered consumer execution
    mock_cons = mock.MagicMock()
    mock_cons.subscription.return_value = ["topic1"]
    mock_cons.__iter__.return_value = []

    def dummy_handler(msg):
        pass

    kafka._consumers_registry.append((mock_cons, dummy_handler, {"format": "json"}))

    with mock.patch.object(kafka, "_ensure_partitions") as mock_ensure:
        kafka.run_consumers(block=True, partition_scale=True)
        mock_ensure.assert_called_once_with("topic1", 1)


def test_send_raw_format_and_bytes_headers():
    """Validates send method with unregistered format pass-through and bytes headers."""
    kafka = WKafka(bootstrap_servers="localhost:9092")
    mock_prod_inst = mock.MagicMock()
    kafka._producer_instance = mock_prod_inst

    kafka.send(
        "test_topic",
        value=b"raw_bytes",
        format="raw_custom",
        headers={"h_str": "str_val", "h_bytes": b"bytes_val"},
    )
    mock_prod_inst.send.assert_called_once()
    args, kwargs = mock_prod_inst.send.call_args
    assert kwargs["headers"] == [("h_str", b"str_val"), ("h_bytes", b"bytes_val")]


def test_wkafka_socks5_proxy_deprecation_normalization():
    """Validates that passing socks5_proxy converts it to proxy_url to avoid library warnings."""
    kafka = WKafka(bootstrap_servers="localhost:9092", socks5_proxy="127.0.0.1:1080")
    assert "socks5_proxy" not in kafka.extra_config
    assert kafka.extra_config["proxy_url"] == "socks5://127.0.0.1:1080"


def test_send_and_receive_complex_json_headers():
    """Validates automatic JSON serialization in send and automatic JSON deserialization in _handle_message for headers."""
    kafka = WKafka(bootstrap_servers="localhost:9092")
    mock_prod_inst = mock.MagicMock()
    kafka._producer_instance = mock_prod_inst

    # Test send with list and dict headers
    complex_list = [{"bbox": [10, 20, 30, 40], "class": "car"}]
    complex_dict = {"status": True, "code": 200}
    kafka.send(
        "test_topic",
        value={"data": "test"},
        headers={"model_results": complex_list, "status_info": complex_dict},
    )

    args, kwargs = mock_prod_inst.send.call_args
    sent_headers = kwargs["headers"]
    assert len(sent_headers) == 2
    # Verify sent as valid JSON bytes
    assert (
        "model_results",
        b'[{"bbox": [10, 20, 30, 40], "class": "car"}]',
    ) in sent_headers
    assert ("status_info", b'{"status": true, "code": 200}') in sent_headers

    # Test _handle_message deserializing back to native list/dict
    mock_msg = mock.MagicMock()
    mock_msg.value = b'{"data": "test"}'
    mock_msg.topic = "test_topic"
    mock_msg.partition = 0
    mock_msg.offset = 1
    mock_msg.key = None
    mock_msg.headers = [
        ("model_results", b'[{"bbox": [10, 20, 30, 40], "class": "car"}]'),
        ("status_info", b'{"status": true, "code": 200}'),
    ]

    mock_consumer = mock.MagicMock()
    mock_consumer.__iter__.return_value = [mock_msg]

    received_msg = None

    def handler(msg):
        nonlocal received_msg
        received_msg = msg

    options = {"format": "json", "auto_commit": True}
    kafka._handle_message(mock_consumer, handler, options)

    assert received_msg is not None
    assert received_msg.header["model_results"] == complex_list
    assert received_msg.header["status_info"] == complex_dict


def test_handle_message_with_commit_on_receive():
    """Validates commit_on_receive option in _handle_message.

    This test ensures that when commit_on_receive=True is configured:
    1. consumer.commit() is invoked IMMEDIATELY upon message receipt.
    2. consumer.commit() has already executed BEFORE the user handler callback runs.
    3. The message offset and payload are correctly parsed and passed to the callback.
    """
    kafka = WKafka(bootstrap_servers="localhost:9092")
    mock_kafka_msg = mock.MagicMock()
    mock_kafka_msg.value = b'{"task": "process_batch"}'
    mock_kafka_msg.topic = "batch_topic"
    mock_kafka_msg.partition = 0
    mock_kafka_msg.offset = 42
    mock_kafka_msg.key = b"batch_1"
    mock_kafka_msg.headers = []

    mock_consumer = mock.MagicMock()
    mock_consumer.__iter__.return_value = [mock_kafka_msg]

    commit_was_called_before_handler = False

    def handler(msg):
        nonlocal commit_was_called_before_handler
        # Verify that commit was already triggered before handler entered
        commit_was_called_before_handler = mock_consumer.commit.called
        assert msg.value == {"task": "process_batch"}
        assert msg.offset == 42

    options = {
        "key_filter": None,
        "format": "json",
        "auto_commit": False,
        "commit_on_receive": True,
        "background": False,
        "max_retries": 0,
        "retry_delay": 0.01,
        "dlq_topic": None,
        "model": None,
    }

    kafka._handle_message(mock_consumer, handler, options)

    assert commit_was_called_before_handler is True
    assert mock_consumer.commit.call_count == 1


def test_handle_message_with_background_execution():
    """Validates background execution option in _handle_message.

    This test ensures that when background=True is configured:
    1. _handle_message dispatches execution to an internal ThreadPoolExecutor.
    2. The consumer loop does not block the caller thread while the handler runs.
    3. The callback function executes successfully in a distinct worker thread.
    4. Resources are cleanly finalized during executor shutdown.
    """
    import threading

    kafka = WKafka(bootstrap_servers="localhost:9092")
    mock_kafka_msg = mock.MagicMock()
    mock_kafka_msg.value = b'{"video_id": 101, "action": "render"}'
    mock_kafka_msg.topic = "video_pipeline"
    mock_kafka_msg.partition = 0
    mock_kafka_msg.offset = 88
    mock_kafka_msg.key = None
    mock_kafka_msg.headers = []

    mock_consumer = mock.MagicMock()
    mock_consumer.__iter__.return_value = [mock_kafka_msg]

    main_thread_ident = threading.get_ident()
    handler_thread_ident = None
    processed_event = threading.Event()

    def handler(msg):
        nonlocal handler_thread_ident
        handler_thread_ident = threading.get_ident()
        assert msg.value == {"video_id": 101, "action": "render"}
        processed_event.set()

    options = {
        "key_filter": None,
        "format": "json",
        "auto_commit": True,
        "commit_on_receive": False,
        "background": True,
        "background_workers": 2,
        "max_retries": 0,
        "retry_delay": 0.01,
        "dlq_topic": None,
        "model": None,
    }

    kafka._handle_message(mock_consumer, handler, options)

    assert processed_event.wait(timeout=2.0) is True
    assert handler_thread_ident is not None
    # Verify that the handler executed in a different worker thread
    assert handler_thread_ident != main_thread_ident


def test_handle_message_with_commit_on_receive_and_background():
    """Validates combined commit_on_receive and background execution.

    This test covers the primary production pattern for long-running workflows:
    1. consumer.commit() is triggered on the consumer thread upon message arrival.
    2. The task is handed off to a background worker thread.
    3. The consumer thread is immediately free to poll without blocking or rebalance risk.
    """
    import threading

    kafka = WKafka(bootstrap_servers="localhost:9092")
    mock_kafka_msg = mock.MagicMock()
    mock_kafka_msg.value = b'{"report_id": "rep_999"}'
    mock_kafka_msg.topic = "reports_topic"
    mock_kafka_msg.partition = 0
    mock_kafka_msg.offset = 123
    mock_kafka_msg.key = None
    mock_kafka_msg.headers = []

    mock_consumer = mock.MagicMock()
    mock_consumer.__iter__.return_value = [mock_kafka_msg]

    task_completed = threading.Event()

    def handler(msg):
        assert msg.value == {"report_id": "rep_999"}
        task_completed.set()

    options = {
        "key_filter": None,
        "format": "json",
        "auto_commit": False,
        "commit_on_receive": True,
        "background": True,
        "background_workers": 2,
        "max_retries": 0,
        "retry_delay": 0.01,
        "dlq_topic": None,
        "model": None,
    }

    kafka._handle_message(mock_consumer, handler, options)

    assert task_completed.wait(timeout=2.0) is True
    assert mock_consumer.commit.call_count == 1


def test_consumer_decorator_options_propagation():
    """Validates consumer decorator options propagation.

    Ensures that the consumer decorator properly captures and propagates
    commit_on_receive, background, and background_workers parameters into
    the consumers registry options dictionary.
    """
    with mock.patch("wkafka.core.manager.KafkaConsumer") as mock_kc_cls:
        mock_instance = mock.MagicMock()
        mock_kc_cls.return_value = mock_instance

        kafka = WKafka(bootstrap_servers="localhost:9092")

        @kafka.consumer(
            topic="test_options_topic",
            commit_on_receive=True,
            background=True,
            background_workers=4,
        )
        def custom_handler(msg):
            pass

        assert len(kafka.consumers) == 1
        registered_consumer, registered_func, registered_options = kafka.consumers[0]
        assert registered_func.__name__ == "custom_handler"
        assert registered_options["commit_on_receive"] is True
        assert registered_options["background"] is True
        assert registered_options["background_workers"] == 4


def test_handle_message_commit_on_receive_handles_exception():
    """Validates error resilience during commit_on_receive.

    If consumer.commit() encounters an unexpected exception (e.g. transient broker issue),
    _handle_message logs a warning and proceeds with message processing rather than
    crashing the entire consumer loop.
    """
    kafka = WKafka(bootstrap_servers="localhost:9092")
    mock_kafka_msg = mock.MagicMock()
    mock_kafka_msg.value = b'{"data": "resilient"}'
    mock_kafka_msg.topic = "resilience_topic"
    mock_kafka_msg.partition = 0
    mock_kafka_msg.offset = 77
    mock_kafka_msg.key = None
    mock_kafka_msg.headers = []

    mock_consumer = mock.MagicMock()
    mock_consumer.__iter__.return_value = [mock_kafka_msg]
    mock_consumer.commit.side_effect = Exception("Kafka commit failed")

    handler_executed = False

    def handler(msg):
        nonlocal handler_executed
        handler_executed = True
        assert msg.value == {"data": "resilient"}

    options = {
        "key_filter": None,
        "format": "json",
        "auto_commit": False,
        "commit_on_receive": True,
        "background": False,
        "max_retries": 0,
        "retry_delay": 0.01,
        "dlq_topic": None,
        "model": None,
    }

    # Should not raise exception
    kafka._handle_message(mock_consumer, handler, options)
    assert handler_executed is True
