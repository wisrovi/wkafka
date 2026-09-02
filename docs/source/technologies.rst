Technologies & Dependencies
===========================

WKafka is built on top of a focused set of well-established Python libraries.
This page explains the role of each dependency and why it was chosen.

.. list-table::
   :widths: 22 40 38
   :header-rows: 1

   * - Library
     - Role in WKafka
     - License
   * - **kafka-python-ng**
     - Low-level Kafka broker protocol client; WKafka wraps this to provide
       the decorator-driven API.
     - Apache 2.0
   * - **Pydantic v2**
     - Strongly-typed data validation and schema enforcement for
       ``format="pydantic"`` consumers and producers.
     - MIT
   * - **OpenCV (cv2)**
     - Matrix encoding/decoding for real-time image and video frame
       streaming via ``format="image"``.
     - Apache 2.0
   * - **Pillow (PIL)**
     - Image object manipulation and format conversion used by
       ``ImageSerializer``.
     - MIT-CMU
   * - **NumPy**
     - Multi-dimensional array handling for image frames decoded from
       JPEG/PNG bytes.
     - BSD-3-Clause
   * - **PyYAML**
     - Native YAML serialization and deserialization for
       ``format="yaml"`` messages.
     - MIT
   * - **python-snappy / cramjam**
     - High-throughput CPU-efficient streaming compression; available as
       an optional extra (``pip install wkafka[snappy]``).
     - BSD-3-Clause
   * - **loguru**
     - Structured logging backend used internally for consistent,
       readable log output across all WKafka components.
     - MIT
   * - **Pytest + pytest-cov**
     - Automated unit testing and code-coverage measurement.
     - MIT
   * - **tox**
     - Multi-version test runner ensuring compatibility across Python
       3.9 through 3.14.
     - MIT
   * - **Sphinx + Furo**
     - Documentation generation and the visual theme powering these
       docs, hosted on Read the Docs.
     - MIT

---

Python version support
----------------------

WKafka officially supports **Python >= 3.9 and < 3.15**. Every release is
tested against all supported versions via ``tox`` and GitHub Actions.

---

Optional extras
---------------

The ``snappy`` extra installs ``python-snappy`` and ``cramjam``:

.. code-block:: bash

   pip install wkafka[snappy]