from __future__ import annotations

import functools
import logging
from typing import Any, NamedTuple
from uuid import UUID

from aws_schema_registry.avro import AvroSchema
from aws_schema_registry.jsonschema import JsonSchema
from aws_schema_registry.client import SchemaRegistryClient
from aws_schema_registry.codec import decode, encode, UnknownEncodingException
from aws_schema_registry.exception import SchemaRegistryException
from aws_schema_registry.schema import CompatibilityMode, Schema, SchemaVersion

LOG = logging.getLogger(__name__)


class DataAndSchema(NamedTuple):
    """Data and its schema.

    Can be used to wrap the data and schema together before calling the
    producer's producing methods.
    """
    data: Any
    schema: Schema


class KafkaSerializer:
    """Kafka serializer that uses the AWS Schema Registry.

    Arguments:
        client: instance of SchemaRegistryClient
        compatibility_mode (optional): the compatibility mode t use if
            creating a new schema in the registry. Defaults to the
            registry's default compatibility setting if not specified.
        auto_register_schema (optional): whether to register new schema or
                new schema version if one or the other does not exist
                in registry
    """

    def __init__(
        self,
        client: SchemaRegistryClient,
        compatibility_mode: CompatibilityMode = 'BACKWARD',
        auto_register_schema: bool = False
    ):
        self.client = client
        self.compatibility_mode: CompatibilityMode = compatibility_mode
        self.auto_register_schema = auto_register_schema

    def serialize(self, data_and_schema: DataAndSchema):
        if data_and_schema is None:
            return None
        if not isinstance(data_and_schema, tuple):
            raise TypeError('KafkaSerializer can only serialize',
                            f' {tuple}, got {type(data_and_schema)}')
        data, schema = data_and_schema
        schema_name = schema.fqn.split(".")[-1]
        schema_version = self._get_schema_version(schema, schema_name)
        serialized = schema.write(data)
        return encode(serialized, schema_version.version_id)

    @functools.lru_cache(maxsize=None)
    def _get_schema_version(self, schema: Schema, schema_name: str) -> SchemaVersion:
        return self.client.get_or_register_schema_version(definition=str(schema),
                                                          schema_name=schema_name,
                                                          data_format=schema.data_format,
                                                          compatibility_mode=self.compatibility_mode,
                                                          auto_register_schema=self.auto_register_schema)


class KafkaDeserializer:
    """Kafka serializer that uses the AWS Schema Registry.

    Arguments:
        client: instance of SchemaRegistryClient.
        return_record_name: if true, when reading a union of records,
            the result will be a tuple where the first value is the
            name of the record and the second value is the record
            itself
        secondary_deserializer: optional deserializer to pass through
            to when processing values with an unrecognized encoding.
            This is primarily use to migrate from other schema
            registries or handle schema-less data. The secondary deserializer
            should either be a callable taking the same arguments as
            deserialize or an object with a matching deserialize method.
    """

    def __init__(
        self,
        client: SchemaRegistryClient,
        return_record_name: bool = False,
        secondary_deserializer=None
    ):
        self.client = client
        self.return_record_name = return_record_name
        self.secondary_deserializer = secondary_deserializer

    def deserialize(self, topic: str, bytes_: bytes):
        if bytes_ is None:
            return None
        try:
            data_bytes, schema_version_id = decode(bytes_)
        except UnknownEncodingException as e:
            if self.secondary_deserializer:
                if callable(self.secondary_deserializer):
                    return self.secondary_deserializer(topic, bytes_)
                return self.secondary_deserializer.deserialize(topic, bytes_)
            else:
                raise SchemaRegistryException(
                    'no secondary deserializer provided to handle'
                    ' unrecognized data encoding'
                ) from e
        writer_schema_version = self._get_schema_version(schema_version_id)
        writer_schema = self._schema_for_version(writer_schema_version)
        return DataAndSchema(writer_schema.read(data_bytes), writer_schema)

    @functools.lru_cache(maxsize=None)
    def _get_schema_version(self, version_id: UUID):
        LOG.info('Fetching schema version %s...', version_id)
        return self.client.get_schema_version(version_id)

    @functools.lru_cache(maxsize=None)
    def _schema_for_version(self, version: SchemaVersion) -> Schema:
        if version.data_format == 'AVRO':
            return AvroSchema(version.definition)
        elif version.data_format == 'JSON':
            return JsonSchema(version.definition)
