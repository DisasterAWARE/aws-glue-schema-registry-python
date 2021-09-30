from datetime import datetime
import os

from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
import pytest

from aws_schema_registry import DataAndSchema, SchemaRegistryClient
from aws_schema_registry.avro import AvroSchema
from aws_schema_registry.adapter.kafka import (
    SchemaRegistryDeserializer, SchemaRegistrySerializer
)
from aws_schema_registry.naming import record_name_strategy

BOOTSTRAP_STRING = '127.0.0.1:9092'

TOPIC_PREFIX = 'SchemaRegistryTests'
NUMBER_OF_PARTITIONS = 1
REPLICATION_FACTOR = 1

DATE = datetime.utcnow().strftime('%y-%m-%d-%H-%M')

with open(os.path.join(os.path.dirname(__file__), 'user.v1.avsc'), 'r') as f:
    SCHEMA_V1 = AvroSchema(f.read())
with open(os.path.join(os.path.dirname(__file__), 'user.v2.avsc'), 'r') as f:
    SCHEMA_V2 = AvroSchema(f.read())

PRODUCER_PROPERTIES = {
    'bootstrap_servers': BOOTSTRAP_STRING,
    'acks': 'all',
    'retries': 0,
    'batch_size': 16384,
    'linger_ms': 1,
    'buffer_memory': 33554432,
    'request_timeout_ms': 1000
}

CONSUMER_PROPERTIES = {
    'bootstrap_servers': BOOTSTRAP_STRING,
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': False
}


@pytest.fixture(scope='session')
def topic():
    """The topic to use for testing. Name is partially random."""
    name = f'{TOPIC_PREFIX}-{DATE}'
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_STRING)
    print('creating topic %s...' % name)
    admin_client.create_topics([
        NewTopic(name, NUMBER_OF_PARTITIONS, REPLICATION_FACTOR)
    ])
    yield name
    print('deleting topic %s...' % name)
    admin_client.delete_topics([name])


def test_produce_consume_with_ser_de_schema_registry(
    glue_client, topic, registry
):
    client = SchemaRegistryClient(
        glue_client, registry_name=registry
    )
    serializer = SchemaRegistrySerializer(
        client, schema_naming_strategy=record_name_strategy
    )
    deserializer = SchemaRegistryDeserializer(client)

    producer = KafkaProducer(
        value_serializer=serializer,
        **PRODUCER_PROPERTIES
    )
    data1 = {
        'name': 'John Doe',
        'favorite_number': 6,
        'favorite_color': 'red'
    }
    producer.send(topic, DataAndSchema(data1, SCHEMA_V1))

    data2 = {
        'name': 'John Doe',
        'favorite_number': 6,
        'favorite_colors': ['red', 'blue']
    }
    producer.send(topic, DataAndSchema(data2, SCHEMA_V2))

    consumer = KafkaConsumer(
        topic,
        value_deserializer=deserializer,
        **CONSUMER_PROPERTIES
    )
    batch = consumer.poll(timeout_ms=1000)
    assert len(batch) == 1
    messages = batch[list(batch.keys())[0]]
    assert len(messages) == 2
    assert messages[0].value == data1
    assert messages[1].value == data2
