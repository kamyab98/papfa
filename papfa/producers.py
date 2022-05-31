import abc
from datetime import timedelta
from typing import List, Type
from uuid import uuid4

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from dataclasses_avroschema import AvroModel

from .config import KafkaProducerConfig
from .consumers import BatchConfig
from .dtos import Record
from .middlewares.producer import ProducerMiddleware
from .settings import Papfa


class MessageProducer(abc.ABC):
    @abc.abstractmethod
    def produce(self, message):
        pass


class KafkaMessageProducer:
    def __init__(
        self,
        topic: str,
        kafka_producer_config: KafkaProducerConfig,
        middlewares: List[ProducerMiddleware] = None,
        batch_config: BatchConfig = BatchConfig(
            size=100, timeout=timedelta(microseconds=10)
        ),
        raise_exception: bool = False,
        producer_kwargs: dict = None,
    ):
        self.topic = topic
        self.middlewares = middlewares
        self.raise_exception = raise_exception
        self.middlewares = middlewares or []

        self.producer = SerializingProducer(
            {
                "bootstrap.servers": ",".join(
                    kafka_producer_config.kafka_config.bootstrap_servers
                ),
                "sasl.mechanism": kafka_producer_config.kafka_config.sasl_mechanism,
                "security.protocol": kafka_producer_config.kafka_config.security_protocol,
                "sasl.username": kafka_producer_config.kafka_config.sasl_username,
                "sasl.password": kafka_producer_config.kafka_config.sasl_password,
                "value.serializer": kafka_producer_config.serializer,
                "linger.ms": batch_config.timeout.total_seconds() * 10**6,
                **producer_kwargs,
            }
        )

    def produce(self, messages: List[Record]):
        for message in messages:
            for middleware in self.middlewares:
                message = middleware.process_before_produce(message)
            _uuid = uuid4()
            self.producer.produce(
                self.topic,
                value=message.value,
                key=message.key or _uuid,
                headers=message.headers or _uuid,
                timestamp=message.timestamp or 0,
            )
            for middleware in self.middlewares:
                middleware.process_after_produce(message)
        self.producer.flush()


def get_message_producer(topic, avro_model: Type[AvroModel]):
    return KafkaMessageProducer(
        topic=topic,
        kafka_producer_config=KafkaProducerConfig(
            serializer=AvroSerializer(
                schema_registry_client=Papfa.get_instance()["schema_registry_client"],
                schema_str=avro_model.avro_schema(),
            ),
            kafka_config=Papfa.get_instance()["kafka_config"],
        ),
    )
