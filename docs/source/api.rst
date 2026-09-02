API Reference
=============

This section documents every public class and method exposed by the WKafka
package. All signatures are extracted directly from the source code and
kept in sync with ``v1.0.6``.

Package: ``wkafka``
-------------------

.. code-block:: python

   from wkafka import WKafka, Message

The top-level package exposes exactly two symbols. ``Wkafka`` (lowercase ``k``)
is retained as a backward-compatible alias.

---

``wkafka.core.manager.WKafka``
-------------------------------

The main orchestrator. Instantiate once per application and use it to
register consumers and send messages.

.. autoclass:: wkafka.core.manager.WKafka
   :members:
   :undoc-members:
   :show-inheritance:

``__init__``
^^^^^^^^^^^^

.. code-block:: python

   WKafka(
       bootstrap_servers: str | None = None,
       extra_config: dict | None = None,
   )

**Parameters:**

- ``bootstrap_servers`` — Comma-separated broker addresses. When omitted,
  the client reads ``KAFKA_SERVER`` from the environment and falls back
  to ``localhost:9092``.
- ``extra_config`` — Extra keyword arguments forwarded to every
  ``KafkaConsumer`` and ``KafkaProducer`` instance created internally.

``consumer()``
^^^^^^^^^^^^^^

.. code-block:: python

   @kafka.consumer(
       topic="orders",          # str | list[str] | None
       pattern="sensor_.*",     # str | None  — regex subscription
       group_id="my-group",     # str | None  — auto-generated when omitted
       key_filter="priority",   # str | None  — filter by message key
       format="json",           # str         — json | yaml | image | file | pydantic
       auto_commit=True,        # bool
       max_retries=0,           # int         — exponential backoff retries
       retry_delay=1.0,         # float       — base delay in seconds
       dlq_topic=None,          # str | None  — Dead Letter Queue topic
       model=None,              # Pydantic model class for format="pydantic"
       partition_scale=None,    # bool | None — auto-resize partitions
   )
   def handler(msg: Message) -> None: ...

Register a consumer callback. The decorator is idempotent; the callback
is invoked once per message. ``msg`` is always a ``Message`` instance.

When ``max_retries > 0`` and a callback raises an exception, WKafka
retries with exponential backoff (delay doubled per attempt). After all
attempts are exhausted, the message is routed to ``dlq_topic`` if one is
set; otherwise the exception is logged.

``send()``
^^^^^^^^^^

.. code-block:: python

   kafka.send(
       topic: str,
       value: Any,
       key: str | bytes | None = None,
       format: str = "json",
       headers: dict | None = None,
       **kwargs,
   ) -> RecordMetadata | None

Synchronous send. Returns the broker ``RecordMetadata``.

``producer()``
^^^^^^^^^^^^^^

.. code-block:: python

   with kafka.producer() as p:
       p.send("orders", value={"id": 1}, format="json")

Context manager that yields a ``KafkaProducer`` and ensures the
connection is flushed and closed on exit.

``run_consumers()``
^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   kafka.run_consumers(block: bool = True, partition_scale: bool | None = None)

Start all registered consumer loops. When ``block=True`` (the default),
the call blocks the main thread indefinitely. When ``partition_scale`` is
``True``, each consumer topic is resized to the broker's current
partition count before starting.

---

``wkafka.core.models.Message``
------------------------------

The immutable message envelope delivered to every consumer callback.

.. autoclass:: wkafka.core.models.Message
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

---

``wkafka.serializers``
-----------------------

All serializers inherit from a single abstract base and follow a uniform
``serialize`` / ``deserialize`` interface.

``Serializer`` (abstract base)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: wkafka.serializers.base.Serializer
   :members:
   :show-inheritance:

``JSONSerializer``
^^^^^^^^^^^^^^^^^^

.. autoclass:: wkafka.serializers.base.JSONSerializer
   :members:

``YAMLSerializer``
^^^^^^^^^^^^^^^^^^

.. autoclass:: wkafka.serializers.base.YAMLSerializer
   :members:

``ImageSerializer``
^^^^^^^^^^^^^^^^^^^

Handles OpenCV/NumPy arrays and PIL ``Image`` objects. Encodes to JPEG
bytes by default.

.. autoclass:: wkafka.serializers.base.ImageSerializer
   :members:

``PydanticSerializer``
^^^^^^^^^^^^^^^^^^^^^^

Serializes and deserializes any Pydantic ``BaseModel`` subclass.

.. autoclass:: wkafka.serializers.base.PydanticSerializer
   :members:

``FileSerializer``
^^^^^^^^^^^^^^^^^^

Handles arbitrary file uploads. Expects the value to be a file path
(string) or a ``bytes`` object.

.. autoclass:: wkafka.serializers.base.FileSerializer
   :members:

---

``wkafka.controller``
---------------------

The legacy ``Wkafka`` alias. It inherits from ``WKafka`` and is retained
solely for backward compatibility.

.. autoclass:: wkafka.controller.wkafka.Wkafka
   :members:
   :undoc-members:
   :show-inheritance: