Tutorials
=========

This section contains step-by-step recipes for common WKafka workflows.
Every example is self-contained and tested against ``v1.0.6``.

.. contents:: On this page
   :local:
   :depth: 2

---

Tutorial 1: Image streaming (OpenCV)
-------------------------------------

Send and receive real-time video frames using OpenCV and NumPy arrays.

Producer — capture and send
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   import cv2
   from wkafka import WKafka

   kafka = WKafka(bootstrap_servers="localhost:9092")

   cap = cv2.VideoCapture(0)

   with kafka.producer() as p:
       while True:
           ret, frame = cap.read()
           if not ret:
               break
           p.send("camera-frames", value=frame, format="image")
       cap.release()

Consumer — receive and display
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   import cv2
   from wkafka import WKafka

   kafka = WKafka(bootstrap_servers="localhost:9092")

   @kafka.consumer(topic="camera-frames", format="image")
   def display_frame(msg):
       frame = msg.value          # numpy array (BGR)
       cv2.imshow("Live", frame)
       if cv2.waitKey(1) & 0xFF == ord("q"):
           cv2.destroyAllWindows()

   kafka.run_consumers(block=True)

The ``format="image"`` selector invokes ``ImageSerializer`` internally,
which encodes NumPy arrays to JPEG bytes on the wire and decodes them
back on the consumer side.

---

Tutorial 2: File streaming
---------------------------

Stream arbitrary files (PDFs, ZIPs, CSVs) through Kafka.

Producer
^^^^^^^^

.. code-block:: python

   from wkafka import WKafka

   kafka = WKafka(bootstrap_servers="localhost:9092")

   with kafka.producer() as p:
       p.send("documents", value="/path/to/report.pdf", format="file")

Consumer
^^^^^^^^

.. code-block:: python

   import os
   from wkafka import WKafka

   kafka = WKafka(bootstrap_servers="localhost:9092")

   @kafka.consumer(topic="documents", format="file")
   def save_document(msg):
       os.makedirs("received", exist_ok=True)
       with open(f"received/{msg.key or 'file'}", "wb") as f:
           f.write(msg.value)

   kafka.run_consumers(block=True)

``format="file"`` reads the file from disk on the producer side and
delivers the raw bytes to the consumer as ``bytes``.

---

Tutorial 3: Pydantic schema validation
---------------------------------------

Define a model and let WKafka enforce the schema on every message.

Model definition
^^^^^^^^^^^^^^^^

.. code-block:: python

   from pydantic import BaseModel

   class Order(BaseModel):
       order_id: int
       item: str
       quantity: int = 1

Producer
^^^^^^^^

.. code-block:: python

   from wkafka import WKafka

   kafka = WKafka(bootstrap_servers="localhost:9092")

   with kafka.producer() as p:
       p.send("orders", value={"order_id": 101, "item": "Laptop"}, format="json")

Consumer with validation
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from wkafka import WKafka
   from models import Order

   kafka = WKafka(bootstrap_servers="localhost:9092")

   @kafka.consumer(
       topic="orders",
       format="pydantic",
       model=Order,              # enforce schema automatically
   )
   def handle_validated(msg):
       order: Order = msg.value  # guaranteed to be an Order instance
       print(f"Order {order.order_id}: {order.item}")

   kafka.run_consumers(block=True)

If the incoming payload does not match ``Order``, Pydantic raises a
``ValidationError`` which WKafka surfaces through the retry / DLQ
machinery when configured.

---

Tutorial 4: Async handlers
---------------------------

WKafka detects ``async def`` callbacks automatically and executes them
through an appropriate event loop.

.. code-block:: python

   import aiohttp
   from wkafka import WKafka

   kafka = WKafka(bootstrap_servers="localhost:9092")

   @kafka.consumer(topic="webhooks", format="json")
   async def process_webhook(msg):
       async with aiohttp.ClientSession() as session:
           await session.post(
               "https://api.example.com/events",
               json=msg.value,
           )

   kafka.run_consumers(block=True)

.. note::

   The ``run_consumers`` call still blocks the main thread. The async
   callback is dispatched to a background event loop managed internally
   by WKafka, so your ``await`` calls execute without blocking other
   consumer threads.

---

Tutorial 5: Manual commit and at-least-once delivery
-----------------------------------------------------

Disable automatic commits and only acknowledge messages after successful
processing.

.. code-block:: python

   from wkafka import WKafka

   kafka = WKafka(bootstrap_servers="localhost:9092")

   def save_to_database(payload):
       """Simulate a slow database write."""
       import time; time.sleep(0.5)
       return True

   @kafka.consumer(
       topic="payments",
       format="json",
       auto_commit=False,
   )
   def process_payment(msg):
       save_to_database(msg.value)
       msg.commit()  # offset committed only after success

   kafka.run_consumers(block=True)

If the process crashes between ``save_to_database`` and ``msg.commit()``,
the message will be re-delivered on restart — hence *at-least-once*.

---

Tutorial 6: Retry and DLQ for unreliable handlers
--------------------------------------------------

Combine retries and DLQ routing to handle transient failures gracefully.

.. code-block:: python

   import logging
   from wkafka import WKafka

   logger = logging.getLogger(__name__)
   kafka = WKafka(bootstrap_servers="localhost:9092")

   @kafka.consumer(
       topic="externally_fetched",
       format="json",
       max_retries=3,
       retry_delay=1.0,
       dlq_topic="externally_fetched.DLQ",
   )
   def fetch_from_external(msg):
       response = call_unreliable_api(msg.value)
       if response.status != 200:
           raise RuntimeError(f"API returned {response.status}")

   kafka.run_consumers(block=True)

Retry timing: ``1 s → 2 s → 4 s`` (doubles each attempt). After 3
failures the message is produced to ``externally_fetched.DLQ`` and
processing continues.

---

Tutorial 7: Multi-topic and pattern subscription
-------------------------------------------------

Subscribe to an explicit list of topics or use a regex pattern.

.. code-block:: python

   from wkafka import WKafka

   kafka = WKafka(bootstrap_servers="localhost:9092")

   # Explicit list
   @kafka.consumer(topic=["orders", "returns", "exchanges"], format="json")
   def handle_all(msg):
       print(f"[{msg.topic}] {msg.value}")

   # Regex pattern
   @kafka.consumer(pattern="telemetry_.*", format="json")
   def handle_telemetry(msg):
       print(f"Telemetry from {msg.topic}: {msg.value}")

   kafka.run_consumers(block=True)