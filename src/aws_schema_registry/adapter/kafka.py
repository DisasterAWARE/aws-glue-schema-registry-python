"""Adapter for kafka-python.

https://pypi.org/project/kafka-python/
"""

from kafka import Serializer, Deserializer

from aws_schema_registry import (
    KafkaSerializer as _KafkaSerializer,
    KafkaDeserializer as _KafkaDeserializer
)


class KafkaSerializer(Serializer):
    def __init__(self, *args, **kwargs):
        self._serializer = _KafkaSerializer(*args, **kwargs)

    def serialize(self, topic, value):
        return self._serializer.serialize(topic, value)


class KafkaDeserializer(Deserializer):
    def __init__(self, *args, **kwargs):
        self._deserializer = _KafkaDeserializer(*args, **kwargs)

    def deserialize(self, topic, bytes_):
        return self._deserializer.deserialize(topic, bytes_)
