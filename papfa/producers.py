import abc
import signal
from datetime import timedelta, datetime
from typing import Type, List
from uuid import uuid4

from confluent_kafka import SerializingProducer, KafkaError
from confluent_kafka.schema_registry.avro import AvroSerializer
from dataclasses_avroschema import AvroModel

from .config import KafkaProducerConfig
from .consumers import BatchConfig
from .dtos import Record
from .middlewares.producer import ProducerMiddleware
from .settings import Papfa


class MessageProducer(abc.ABC):
    @abc.abstractmethod
    def produce(self, messages):
        pass


class KafkaMessageProducer(MessageProducer):

    def __init__(
        self,
        topic: str,
        kafka_producer_config: KafkaProducerConfig,
        middlewares: List[ProducerMiddleware] = None,
        batch_config: BatchConfig = None,
        producer_kwargs: dict = None,
    ):
        self.topic = topic
        self.not_flushed_count = 0
        self.__producer = None
        self.kafka_producer_config = kafka_producer_config
        self.middlewares = middlewares or []
        self.batch_config = batch_config or BatchConfig(size=100, timeout=timedelta(microseconds=10))
        self.producer_kwargs = producer_kwargs or dict()
        self.last_flush_time = None

    @property
    def producer(self):
        class KafkaSerializingProducer(SerializingProducer):
            def __init__(self, config, middlewares):
                self.middlewares = middlewares
                super().__init__(config)

            def produce(self, topic, key=None, value=None, partition=-1, on_delivery=None, timestamp=0, headers=None):
                for middleware in self.middlewares:
                    value = middleware.process_before_produce(value)
                super(KafkaSerializingProducer, self).produce(
                    topic, key, value, partition, on_delivery, timestamp, headers
                )
                for middleware in self.middlewares:
                    middleware.process_after_produce(value)

        if not self.__producer:
            self.__producer = KafkaSerializingProducer(self.get_config(), middlewares=self.middlewares)
        return self.__producer

    def get_config(self):
        _config = {
            "bootstrap.servers": ','.join(self.kafka_producer_config.kafka_config.bootstrap_servers),
            "value.serializer": self.kafka_producer_config.serializer,
            "linger.ms": self.producer_kwargs.pop('linger.ms', self.batch_config.timeout.total_seconds() * 10 ** 6),
            "security.protocol": self.kafka_producer_config.kafka_config.security_protocol,
            "sasl.mechanism": self.kafka_producer_config.kafka_config.sasl_mechanism,
            "sasl.username": self.kafka_producer_config.kafka_config.sasl_username,
            "sasl.password": self.kafka_producer_config.kafka_config.sasl_password,
            **self.producer_kwargs,
        }
        return {k: v for k, v in _config.items() if v is not None}

    def produce(self, message: Record, on_delivery: callable = None, force_flush: bool = False) -> None:
        self.producer.produce(
            topic=self.topic,
            key=message.key or uuid4(),
            value=message.value,
            on_delivery=on_delivery,
            headers=message.headers,
            timestamp=message.timestamp,
        )
        self.not_flushed_count += 1
        self.last_flush_time = self.last_flush_time or datetime.now()
        self.flush(force_flush)

    def flush(self, force=False):
        if (
            force or
            self.not_flushed_count >= self.batch_config.size or
            datetime.now() - self.last_flush_time > self.batch_config.timeout
        ):
            self.producer.flush()
            self.not_flushed_count = 0


class KafkaMessageTransactionalProducer(KafkaMessageProducer):
    class Transaction:
        def __init__(self, producer, topic):
            self.topic = topic
            self.producer = producer
            self.consumer = None
            self.producer.init_transactions()

        def _handle_interrupt(self):
            self.producer.abort_transaction()

        def __call__(self, consumer):
            self.consumer = consumer
            return self

        def __enter__(self):
            signal.signal(signal.SIGINT, self._handle_interrupt)
            signal.signal(signal.SIGTERM, self._handle_interrupt)
            self.producer.begin_transaction()
            return self.produce

        def produce(self, message: Record, on_delivery: callable = None):
            self.producer.produce(
                topic=self.topic,
                key=message.key or uuid4(),
                value=message.value,
                on_delivery=on_delivery,
                headers=message.headers,
                timestamp=message.timestamp,
            )

        def __exit__(self, exc_type, exc_val, exc_tb):
            if not exc_type:
                try:
                    if self.consumer:
                        self.producer.send_offsets_to_transaction(
                            self.consumer.position(self.consumer.assignment()),
                            self.consumer.consumer_group_metadata()
                        )
                    self.producer.commit_transaction()
                    return
                except KafkaError:
                    pass
            self.producer.abort_transaction()

    def __init__(
        self,
        topic: str,
        kafka_producer_config: KafkaProducerConfig,
        middlewares: List[ProducerMiddleware] = None,
        batch_config: BatchConfig = None,
        transaction_id: str = None,
        producer_kwargs: dict = None,
    ):
        super().__init__(topic, kafka_producer_config, middlewares, batch_config, producer_kwargs)
        self.transaction_id = transaction_id or uuid4()
        self.transaction_instance = self.Transaction(self.producer, self.topic)

    def transaction(self, consumer=None):
        if self.transaction_instance:
            return self.transaction_instance(consumer)
        raise Exception('Not a transactional producer.')

    def get_config(self):
        conf = super().get_config()
        conf.update(
            {'transactional.id': self.transaction_id, }
        )
        return conf


def get_message_producer(
    topic,
    avro_model: Type[AvroModel],
    kafka_producer_config: KafkaProducerConfig = None,
    middlewares: List[ProducerMiddleware] = None,
    batch_config=None,
    producer_kwargs=None,
):
    return KafkaMessageProducer(
        topic=topic,
        kafka_producer_config=kafka_producer_config or KafkaProducerConfig(
            serializer=AvroSerializer(
                schema_registry_client=Papfa.get_instance()["schema_registry"],
                schema_str=avro_model.avro_schema(),
            ),
            kafka_config=Papfa.get_instance()["kafka_config"],
        ),
        batch_config=batch_config,
        middlewares=middlewares,
        producer_kwargs=producer_kwargs,
    )


def get_message_transactional_producer(
    topic,
    avro_model: Type[AvroModel],
    kafka_producer_config: KafkaProducerConfig = None,
    middlewares: List[ProducerMiddleware] = None,
    transaction_id='',
    batch_config=None,
    producer_kwargs=None,
):
    return KafkaMessageTransactionalProducer(
        topic=topic,
        kafka_producer_config=kafka_producer_config or KafkaProducerConfig(
            serializer=AvroSerializer(
                schema_registry_client=Papfa.get_instance()["schema_registry"],
                schema_str=avro_model.avro_schema(),
            ),
            kafka_config=Papfa.get_instance()["kafka_config"],
        ),
        batch_config=batch_config,
        middlewares=middlewares,
        transaction_id=transaction_id,
        producer_kwargs=producer_kwargs,
    )
