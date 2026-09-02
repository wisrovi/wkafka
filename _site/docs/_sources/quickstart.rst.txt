Quickstart
==========

Installation
------------

Install WKafka via PyPI:

.. code-block:: bash

   pip install wkafka

For high-throughput image and real-time streaming, install Snappy support:

.. code-block:: bash

   pip install wkafka[snappy]

Basic JSON Producer & Consumer
------------------------------

.. code-block:: python

   from wkafka import WKafka

   kafka = WKafka(bootstrap_servers="localhost:9092", client_id="my_app")

   # Register Consumer Callback
   @kafka.consumer(topic="orders", format="json")
   def process_order(msg):
       print(f"Received order: {msg.value}")

   # Send a JSON event
   kafka.send("orders", value={"order_id": 101, "item": "laptop"})

   # Run consumer loop
   if __name__ == "__main__":
       kafka.run_consumers(block=True)
