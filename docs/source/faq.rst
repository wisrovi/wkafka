FAQ
===

Frequently asked questions about WKafka.

.. contents:: On this page
   :local:
   :depth: 1

---

What Python versions are supported?
------------------------------------

Python 3.9 through 3.14. WKafka is tested against every minor version in
this range using ``tox``.

---

How does WKafka choose the broker address?
------------------------------------------

The resolution order is:

1. Explicit ``bootstrap_servers`` parameter: ``WKafka(bootstrap_servers="…")``
2. ``KAFKA_SERVER`` environment variable.
3. Fallback: ``localhost:9092``.

---

How do I subscribe to multiple topics?
---------------------------------------

Pass a list to the ``topic`` parameter:

.. code-block:: python

   @kafka.consumer(topic=["orders", "shipments"], format="json")
   def handler(msg): ...

Or use a regex pattern:

.. code-block:: python

   @kafka.consumer(pattern="sensor_.*", format="json")
   def handler(msg): ...

---

How do at-least-once semantics work?
-------------------------------------

Set ``auto_commit=False`` and call ``msg.commit()`` explicitly after
successful processing:

.. code-block:: python

   @kafka.consumer(topic="payments", format="json", auto_commit=False)
   def process(msg):
       save_to_db(msg.value)
       msg.commit()

If the process crashes before ``commit()``, the offset is not advanced
and the message will be redelivered.

---

What happens when a callback raises an exception?
--------------------------------------------------

By default, the exception is logged and the message is skipped.

If ``max_retries > 0``, WKafka retries with exponential backoff (delay
doubles each attempt). If all retries fail and ``dlq_topic`` is set, the
message is routed there. If no DLQ topic is configured, the failure is
logged and processing continues.

---

How do I enable Snappy compression?
------------------------------------

Install the optional Snappy extra and pass the standard
``kafka-python`` configuration:

.. code-block:: bash

   pip install wkafka[snappy]

.. code-block:: python

   kafka = WKafka(
       bootstrap_servers="localhost:9092",
       extra_config={"compression_type": "snappy"},
   )

---

How do I set up SASL authentication?
-------------------------------------

Pass SASL parameters through ``extra_config``:

.. code-block:: python

   kafka = WKafka(
       bootstrap_servers="broker:9093",
       extra_config={
           "security_protocol": "SASL_SSL",
           "sasl_mechanism": "SCRAM-SHA-512",
           "sasl_plain_username": "user",
           "sasl_plain_password": "secret",
       },
   )

---

Can I use async consumer callbacks?
-----------------------------------

Yes. Define the callback as ``async def`` and WKafka will execute it in a
managed event loop:

.. code-block:: python

   @kafka.consumer(topic="webhooks", format="json")
   async def handle(msg):
       await call_api(msg.value)

---

How do I manually control partition count?
-------------------------------------------

Pass ``partition_scale=True`` to ``run_consumers``:

.. code-block:: python

   kafka.run_consumers(block=True, partition_scale=True)

This resizes each subscribed topic to the broker's current partition count
before consumption starts.

---

Where can I find the changelog?
-------------------------------

See the `GitHub releases <https://github.com/wisrovi/wkafka/releases>`_ page
for the full changelog.

---

How do I contribute?
---------------------

Fork the repository, create a feature branch, and submit a pull request
against ``main``. Tests are run automatically via GitHub Actions.

Repository: `github.com/wisrovi/wkafka <https://github.com/wisrovi/wkafka>`_