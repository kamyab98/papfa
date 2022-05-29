=====
Papfa
=====


.. image:: https://img.shields.io/pypi/v/papfa.svg
        :target: https://pypi.python.org/pypi/papfa

.. image:: https://circleci.com/gh/kamyab98/papfa/tree/master.svg?style=svg
        :target: https://circleci.com/gh/kamyab98/papfa/tree/master
        :alt: Pipeline Status

.. image:: https://readthedocs.org/projects/papfa/badge/?version=latest
        :target: https://papfa.readthedocs.io/en/latest/?version=latest
        :alt: Documentation Status




Papfa is a high level pub/sub pattern Python library.



Quick Start for Django
-----------------------
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
**Consumer**
Add <django_project>.papfa.py

.. code-block:: python

    from papfa import Papfa

    papfa_app = Papfa()

Import papfa in <django_project>.__init__.py

.. code-block:: python

    from <django_project>.papfa import papfa_app

    __all__ = ['papfa_app']


Add consumer in ``<app>/consumers.py``:

.. code-block:: python

    from papfa.consumers import consumer
    from papfa.dtos import Record

    @consumer(topic='topic_name')
    def my_consumer(messages: List[Record]):
        print(messages)

Use papfa cli to consume

.. code-block:: bash

     papfa consume -a <django_app> <consumer_name>

Example of project structure:

.. code-block::

    └── shop
        ├── shop
        │   ├── __init__.py
        │   ├── papfa.py
        │   ├── settings.py
        │   ├── urls.py
        │   └── wsgi.py
        ├── app1
        │   ├── __init__.py
        │   ├── admin.py
        │   ├── apps.py
        │   ├── consumers.py
        │   ├── migrations
        │   │   └── __init__.py
        │   ├── models.py
        │   ├── tests.py
        │   └── views.py
        ├── app2
        │   ├── __init__.py
        │   ├── admin.py
        │   ├── apps.py
        │   ├── consumers.py
        │   ├── migrations
        │   │   └── __init__.py
        │   ├── models.py
        │   ├── tests.py
        │   └── views.py
        └── manage.py




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


CLI
---
Papfa provides a command line interface to consume and monitor consumers.

.. list-table:: Commands
   :widths: 25 25
   :header-rows: 1

   * - Command
     - Description
   * - list
     - list of all consumers
   * - consume
     - consume messages from a known consumer
   * - stats
     - show stats of a consumer


Middleware
-----------
Papfa provides middlewares for both consumers and producers. You can implement your own middleware by extending the
``papfa.middlewares.consumer.ConsumerMiddleware`` and ``papfa.middlwares.producer.ProducerMiddleware`` class.

**Default Middlewares**

* ``papfa.middlewares.consumer.ConsumedMessageStatsMiddleware`` - Logs the last message consumed by each topic - consumer group


Serialization
---------------
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
--------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
