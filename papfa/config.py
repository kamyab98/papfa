import dataclasses
from typing import List

from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer


@dataclasses.dataclass
class KafkaConfig:
    bootstrap_servers: List[str]
    sasl_mechanism: str
    security_protocol: str
    sasl_username: str
    sasl_password: str


@dataclasses.dataclass
class SchemaRegistryConfig:
    pass


@dataclasses.dataclass
class KafkaConsumerConfig:
    group_id: str
    deserializer: AvroDeserializer
    kafka_config: KafkaConfig
    topics: List[str]


@dataclasses.dataclass
class KafkaProducerConfig:
    serializer: AvroSerializer
    kafka_config: KafkaConfig
