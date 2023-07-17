import abc
import dataclasses
import logging
import signal
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List, Callable

from confluent_kafka import DeserializingConsumer, TopicPartition
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from .config import KafkaConsumerConfig, KafkaConfig
from .dtos import Record
from .middlewares.consumer import ConsumerMiddleware
from .settings import Papfa
from .utils import import_string

logger = logging.getLogger(__name__)


class BaseConsumer(abc.ABC):
    @abc.abstractmethod
    def consume(self):
        pass

    @abc.abstractmethod
    def get_meta_data(self):
        pass


class Deserializer(abc.ABC):
    pass


class ConfluentAvroDeserializer(Deserializer):
    def __init__(self, schema_registry_client: SchemaRegistryClient):
        pass

    def deserialize(self, value: bytes) -> dict:
        pass


class MessageHandler(abc.ABC):
    @abc.abstractmethod
    def is_satisfy(self, message: Record) -> bool:
        pass

    @abc.abstractmethod
    def handle_batch(self, message: List[Record]) -> None:
        pass


@dataclasses.dataclass(frozen=True)
class BatchConfig:
    size: int
    timeout: timedelta


class KafkaConsumer(BaseConsumer):
    def __init__(
        self,
        kafka_consumer_config: KafkaConsumerConfig,
        message_handler: MessageHandler,
        middlewares: List[ConsumerMiddleware] = None,
        batch_config: BatchConfig = BatchConfig(size=100, timeout=timedelta(seconds=1)),
        raise_exception: bool = False,
        consumer_kwargs: dict = None,
    ):
        self.batch = []
        self._consumer = None
        self.message_handler = message_handler
        self.batch_config = batch_config
        self.middlewares = middlewares or []
        self.kafka_consumer_config = kafka_consumer_config
        self.consumer_kwargs = consumer_kwargs or {}
        self.raise_exception = raise_exception

    @property
    def consumer(self):
        if not self._consumer:
            _config = {
                "bootstrap.servers": ",".join(
                    self.kafka_consumer_config.kafka_config.bootstrap_servers
                ),
                "group.id": self.kafka_consumer_config.group_id,
                "sasl.mechanism": self.kafka_consumer_config.kafka_config.sasl_mechanism,
                "security.protocol": self.kafka_consumer_config.kafka_config.security_protocol,
                "sasl.username": self.kafka_consumer_config.kafka_config.sasl_username,
                "sasl.password": self.kafka_consumer_config.kafka_config.sasl_password,
                "value.deserializer": self.kafka_consumer_config.deserializer,
                "enable.auto.commit": False,
                **self.consumer_kwargs,
            }
            if self.kafka_consumer_config.deserialize_key:
                _config["key.deserializer"] = self.kafka_consumer_config.deserializer

            self._consumer = DeserializingConsumer({
                k: v for k, v in _config.items() if v is not None
            })

            self._consumer.subscribe(self.kafka_consumer_config.topics)
        return self._consumer

    def consume(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        last_updated = datetime.now()
        while True:
            if (
                len(self.batch) >= self.batch_config.size
                or self.batch_config.timeout < datetime.now() - last_updated
            ):
                last_updated = datetime.now()
                for middleware in self.middlewares:
                    middleware.process_before_flush(self.batch)
                self.flush()
                for middleware in self.middlewares:
                    middleware.process_after_flush()
            for middleware in self.middlewares:
                middleware.process_before_poll()
            try:
                msg = self.consumer.poll(30)

            except SerializerError as e:
                if self.raise_exception:
                    raise Exception
                logger.error("Message deserialization failed for {}: {}".format(msg, e))
                break

            if msg is None:
                logger.warning("Consuming timeout")
                continue

            if msg.error():
                if self.raise_exception:
                    raise Exception
                logger.error("AvroConsumer error: {}".format(msg.error()))
                continue

            msg = Record(
                value=msg.value(),
                key=msg.key(),
                headers=msg.headers(),
                timestamp=msg.timestamp()[1] if msg.timestamp()[0] else None,
                meta={
                    "topic": msg.topic(),
                    "group_id": self.kafka_consumer_config.group_id,
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                },
            )

            for middleware in self.middlewares:
                msg = middleware.process_before_batching(msg)

            if self.message_handler.is_satisfy(msg):
                self.batch.append(msg)

        self.consumer.close()

    def commit(self):
        self.consumer.commit(asynchronous=False)

    def flush(self):
        number_of_messages = len(self.batch)
        self.message_handler.handle_batch(self.batch)
        self.commit()
        self.batch.clear()
        logger.info(
            f"{self.consumer_name} consumed {number_of_messages} messages."
        )

    def exit_gracefully(self, signum, frame):
        self.flush()
        self.consumer.close()
        logger.info(f"{self.consumer_name} exited gracefully.")
        sys.exit()

    @property
    def consumer_name(self):
        return f'{self.kafka_consumer_config.group_id}'

    def get_meta_data(self):
        return {
            "Group ID": self.kafka_consumer_config.group_id,
            "Topics": self.kafka_consumer_config.topics,
            "Batch Size": self.batch_config.size,
            "Batch Timeout": self.batch_config.timeout,
        }


consumers_list = []


def get_default_kafka_consumer(
    func,
    satisfy_method,
    topics,
    group_id,
    batch_config,
    deserialize_key,
    kafka_config=None,
    consumer_kwargs=None,
):
    class CustomMessageHandler(MessageHandler):
        def is_satisfy(_self, msg):
            return satisfy_method(msg)

        def handle_batch(_self, batch):
            return func(batch)

    _configs = {
        "kafka_consumer_config": KafkaConsumerConfig(
            group_id=Papfa.get_instance()["kafka_group_id_prefix"] + '-' + group_id,
            deserializer=AvroDeserializer(
                schema_registry_client=Papfa.get_instance()["schema_registry"]
            ),
            kafka_config=kafka_config or Papfa.get_instance()["kafka_config"],
            topics=topics,
            deserialize_key=deserialize_key
        ),
        "message_handler": CustomMessageHandler(),
        "consumer_kwargs": consumer_kwargs or {},
        "middlewares": [
            import_string(m)() for m in Papfa.get_instance()["consumer_middlewares"]
        ]
    }
    if batch_config:
        _configs["batch_config"] = batch_config

    return KafkaConsumer(**_configs)


def consumer(
    topics: List[str] = None,
    group_id: str = None,
    satisfy_method: Callable = None,
    batch_config: BatchConfig = None,
    consumer_strategy: BaseConsumer = None,
    deserialize_key: bool = False,
    kafka_config: KafkaConfig = None,
    consumer_kwargs: dict = None,
):
    _options = {
        "group_id": group_id,
        "topics": topics,
        "satisfy_method": satisfy_method,
        "batch_config": batch_config,
        "consumer": consumer_strategy,
        "kafka_config": kafka_config,
    }

    def create_consumer(**options):
        class Consumer:
            def __init__(self, func):
                self.__is_consumer__ = True
                self.func = func
                consumers_list.append(func.__name__)
                self.consumer = options.get("consumer") or get_default_kafka_consumer(
                    func=self.func,
                    topics=options.get("topics"),
                    group_id=options.get("group_id") or f"{self.func.__name__}",
                    satisfy_method=options.get("satisfy_method") or (lambda *args, **kwargs: True),
                    batch_config=batch_config,
                    deserialize_key=deserialize_key,
                    kafka_config=options.get("kafka_config"),
                    consumer_kwargs=consumer_kwargs,
                )

            def __call__(self, *args, **kwargs):
                return self.func(*args, **kwargs)

            def consume(self):
                return self.consumer.consume()

        return Consumer

    return create_consumer(**_options)
