=====
Papfa
=====


.. image:: https://img.shields.io/pypi/v/papfa.svg
        :target: https://pypi.python.org/pypi/papfa

.. image:: https://circleci.com/github/kamyab98/papfa.svg?style=svg
        :target: https://circleci.com/github/kamyab98/papfa

.. image:: https://readthedocs.org/projects/papfa/badge/?version=latest
        :target: https://papfa.readthedocs.io/en/latest/?version=latest
        :alt: Documentation Status


.. image:: https://pyup.io/repos/github/kamyab98/papfa/shield.svg
     :target: https://pyup.io/repos/github/kamyab98/papfa/
     :alt: Updates



Papfa is a high level pub/sub pattern Python library.



Quick Start for Django
-----------
Install Papfa::

    $ pip install papfa

Add Papfa to your settings:

.. code-block:: python
    PAPFA = {
        'BROKER': 'KAFKA',
        'KAFKA_BOOTSTRAP_SERVERS': ...,
        'KAFKA_SASL_PASSWORD': ...,
        'KAFKA_SASL_USERNAME': ...,
        'KAFKA_SASL_MECHANISM': ...,
        'KAFKA_SECURITY_PROTOCOL': ...,
        'GROUP_ID_PREFIX': ...,
        'SCHEMA_REGISTRY_URL': ...,
        'SCHEMA_REGISTRY_BASIC_AUTH': ...,
        'CONSUMER_MIDDLEWARES': [
            ...
        ]

    }

Add consumer in ``<app>/consumers.py``:

.. code-block:: python

    from papfa.consumers import consumer
    from papfa.dtos import Record

    @consumer(topic='topic_name')
    def my_consumer(messages: List[Record]):
        print(messages)


Produce Message:

.. code-block:: python

    from dataclasses import dataclass

    from dataclasses_avroschema import AvroModel
    from papfa.producer import get_message_producer
    from papfa.dtos import Record

    @dataclass
    class User(AvroModel):
        name: str
        age: int

    r1 = Record(value=User(name='thom', age=53))
    r1 = Record(value=User(name='jonny', age=50))

    message_producer = get_message_producer(topic='topic_name', User)

    message_producer.produce(messages=[r1, r2])


Middleware
-----------
Papfa provides middlewares for both consumers and producers. You can implement your own middleware by extending the
``papfa.middlewares.consumer.ConsumerMiddleware`` and ``papfa.middlwares.producer.ProducerMiddleware`` class.

**Default Middlewares**

* ``papfa.middlewares.consumer.ConsumedMessageStatsMiddleware`` - Logs the last message consumed by each topic - consumer group


Serialization
------------
For Now Papfa only support confluent avro serialization with schema registry.

Broker
-----------
For Now Papfa only support Apache Kafka.


Features
------------
* Batch Processing (Commit per batch)
* Consumed Messages Stats


Todos
------------
* Handle Idempotency
* Add Other Brokers & Serializers
* Handle Multiple Broker Cluster


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
