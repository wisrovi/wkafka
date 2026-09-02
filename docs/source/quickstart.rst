Quickstart
==========

Install
-------

Install WKafka from PyPI:

.. code-block:: bash

   pip install wkafka

For high-throughput image/video streaming and native Snappy compression:

.. code-block:: bash

   pip install wkafka[snappy]

Set the broker address either explicitly or through an environment variable:

.. code-block:: bash

   export KAFKA_SERVER=localhost:9092

---

Basic producer and consumer
---------------------------

.. code-block:: python

   from wkafka import WKafka

   # KAFKA_SERVER env var is read automatically when bootstrap_servers is omitted.
   kafka = WKafka(bootstrap_servers="localhost:9092")

   @kafka.consumer(topic="orders", format="json")
   def handle_order(msg):
       print(f"New order received: {msg.value}")

   if __name__ == "__main__":
       kafka.run_consumers(block=True)

To produce a message, use the context-manager based producer:

.. code-block:: python

   from wkafka import WKafka

   kafka = WKafka(bootstrap_servers="localhost:9092")

   with kafka.producer() as p:
       p.send("orders", value={"id": 101, "item": "Laptop"}, format="json")

---

Manual offset commit (at-least-once)
-------------------------------------

Disable automatic commits and call ``msg.commit()`` explicitly after
successfully processing a message:

.. code-block:: python

   from wkafka import WKafka

   kafka = WKafka(bootstrap_servers="localhost:9092")

   @kafka.consumer(
       topic="transactions",
       format="json",
       auto_commit=False,
   )
   def handle_tx(msg):
       save_to_database(msg.value)
       msg.commit()  # commit only after success

This pattern guarantees at-least-once delivery semantics.

---

Retries and Dead Letter Queue
-----------------------------

Configure automatic exponential backoff retries and route failed messages
to a dedicated DLQ topic:

.. code-block:: python

   from wkafka import WKafka

   kafka = WKafka(bootstrap_servers="localhost:9092")

   @kafka.consumer(
       topic="unstable_events",
       format="json",
       max_retries=3,
       retry_delay=1.0,          # base delay in seconds
       dlq_topic="unstable_events.DLQ",
   )
   def handle_event(msg):
       process_event(msg.value)

The delay between retries doubles on each attempt (exponential backoff):
1 s, 2 s, 4 s. If all attempts fail, the message is routed to the DLQ
topic automatically.

---

Key filtering
-------------

Subscribe only to messages whose key matches a specific value:

.. code-block:: python

   @kafka.consumer(topic="orders", format="json", key_filter="priority")
   def handle_priority(msg):
       print(f"Priority order: {msg.value}")

---

Multi-topic and pattern subscription
-------------------------------------

Subscribe to an explicit list of topics, or use a regex pattern:

.. code-block:: python

   # Explicit list
   @kafka.consumer(topic=["orders", "shipments"], format="json")
   def handle_events(msg):
       print(f"[{msg.topic}] {msg.value}")

   # Regex pattern
   @kafka.consumer(pattern="sensor_.*", format="json")
   def handle_sensors(msg):
       print(f"Sensor reading on {msg.topic}: {msg.value}")

---

Async handlers
--------------

Consumer callbacks can be ``async def`` — WKafka detects this automatically
and runs them in a compatible event loop:

.. code-block:: python

   import aiohttp
   from wkafka import WKafka

   kafka = WKafka(bootstrap_servers="localhost:9092")

   @kafka.consumer(topic="webhooks", format="json")
   async def handle_webhook(msg):
       async with aiohttp.ClientSession() as session:
           await session.post("https://api.example.com/events", json=msg.value)

---

``KAFKA_SERVER`` environment variable
---------------------------------------

When ``bootstrap_servers`` is omitted from ``WKafka(…)``, the client reads
``KAFKA_SERVER`` from the environment and falls back to
``localhost:9092``:

.. code-block:: python

   import os

   os.environ["KAFKA_SERVER"] = "kafka.internal:9093"
   kafka = WKafka()  # resolves to kafka.internal:9093