WKafka
======

.. image:: https://img.shields.io/pypi/v/wkafka?color=blue
   :target: https://pypi.org/project/wkafka/
   :alt: PyPI version

.. image:: https://img.shields.io/pypi/pyversions/wkafka
   :target: https://pypi.org/project/wkafka/
   :alt: Python versions

.. image:: https://img.shields.io/badge/License-MIT-yellow.svg
   :target: https://opensource.org/licenses/MIT
   :alt: MIT license

**WKafka** is a professional, decorator-based Apache Kafka wrapper for
Python. It turns low-level broker plumbing into a clean, type-safe,
decorator-driven API — with native support for JSON, YAML, images and video
frames, arbitrary files, and Pydantic models.

Built for modern microservices, IoT, and Computer Vision pipelines, WKafka
gives you approachable developer ergonomics without sacrificing production
controls such as retries, Dead Letter Queues, manual offset commits, SASL
security, and automatic partition scaling.

---

Quick tour
----------

.. code-block:: python

   from wkafka import WKafka

   kafka = WKafka(bootstrap_servers="localhost:9092")

   @kafka.consumer(topic="orders", format="json")
   def handle_order(msg):
       print(f"New order received: {msg.value}")

   kafka.run_consumers(block=True)

   with kafka.producer() as p:
       p.send("orders", value={"id": 101, "item": "Laptop"}, format="json")

---

Key features
------------

.. list-table::
   :widths: 30 70
   :header-rows: 0

   * - **Decorator-driven API**
     - Minimal, clean message handling with ``@kafka.consumer``.
   * - **Modern Python**
     - Fully typed and PEP 8 compliant; supports Python 3.9 through 3.14.
   * - **Multimedia & files**
     - Send/receive images (OpenCV, NumPy, PIL) and arbitrary files out of the box.
   * - **Type-safe validation**
     - Automatic Pydantic schema validation with ``format="pydantic"``.
   * - **Resilience**
     - Automatic retries with backoff and Dead Letter Queue routing.
   * - **Enterprise security**
     - Built-in SASL (PLAIN, SCRAM) support and KRaft compatibility.
   * - **Operations**
     - Structured logging via ``loguru`` and multi-version testing via ``tox``.

.. toctree::
   :maxdepth: 2
   :caption: Get Started

   quickstart
   tutorials

.. toctree::
   :maxdepth: 2
   :caption: Reference

   api
   technologies

.. toctree::
   :maxdepth: 2
   :caption: Resources

   examples
   faq
   bibliography