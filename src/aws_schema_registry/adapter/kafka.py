"""Adapter for kafka-python.

https://pypi.org/project/kafka-python/
"""

from kafka import Serializer, Deserializer

from aws_schema_registry import (
    SchemaRegistrySerializer as _SchemaRegistrySerializer,
    SchemaRegistryDeserializer as _SchemaRegistryDeserializer
)


class SchemaRegistrySerializer(Serializer):
    def __init__(self, *args, **kwargs):
        self._serializer = _SchemaRegistrySerializer(*args, **kwargs)

    def serialize(self, topic, value):
        return self._serializer.serialize(topic, value)


class SchemaRegistryDeserializer(Deserializer):
    def __init__(self, *args, **kwargs):
        self._deserializer = _SchemaRegistryDeserializer(*args, **kwargs)

    def deserialize(self, topic, bytes_):
        return self._deserializer.deserialize(topic, bytes_)
