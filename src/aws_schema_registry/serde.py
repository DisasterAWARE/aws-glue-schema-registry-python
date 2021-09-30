from __future__ import annotations

import logging
import sys
from typing import Any, Dict, NamedTuple
from uuid import UUID

if sys.version_info[1] < 8:  # for py37
    from typing_extensions import Protocol
else:
    from typing import Protocol

from aws_schema_registry.avro import AvroSchema
from aws_schema_registry.client import SchemaRegistryClient
from aws_schema_registry.codec import decode, encode, UnknownEncodingException
from aws_schema_registry.exception import SchemaRegistryException
from aws_schema_registry.naming import (
    SchemaNamingStrategy, topic_name_strategy
)
from aws_schema_registry.schema import CompatibilityMode, Schema, SchemaVersion

LOG = logging.getLogger(__name__)


class DataAndSchema(NamedTuple):
    """Data and its schema.

    Should be used to wrap the data and schema together before calling the
    producer's producing methods.
    """
    data: Any
    schema: Schema


class Serializer(Protocol):
    def serialize(self, topic: str, record: DataAndSchema): ...


class Deserializer(Protocol):
    def deserialize(self, topic: str, bytes_: bytes): ...


class SchemaRegistrySerializer:
    """Kafka serializer that uses the AWS Schema Registry.

    Arguments:
        client: instance of SchemaRegistryClient
        is_key (optional): whether the serializer is serializing keys as
            opposed to values. Defaults to false. Setting this to the
            appropriate value is important to avoid mixing key and value
            schemas if using the default schema name strategy.
        compatibility_mode (optional): the compatibility mode t use if
            creating a new schema in the registry. Defaults to the
            registry's default compatibility setting if not specified.
        schema_naming_strategy (optional): how to choose the schema name
            when creating new schemas. Defaults to the topic name
            strategy. See the `naming` module for more information and
            alternate strategies.
    """

    _cache: Dict[Schema, SchemaVersion]

    def __init__(
        self,
        client: SchemaRegistryClient,
        is_key: bool = False,
        compatibility_mode: CompatibilityMode = 'BACKWARD',
        schema_naming_strategy: SchemaNamingStrategy = topic_name_strategy
    ):
        self.client = client
        self.is_key = is_key
        self.compatibility_mode: CompatibilityMode = compatibility_mode
        self.schema_naming_strategy = schema_naming_strategy
        self._cache = {}

    def serialize(self, topic, data_and_schema: DataAndSchema):
        if data_and_schema is None:
            return None
        if not isinstance(data_and_schema, DataAndSchema):
            raise TypeError('AvroSerializer can only serialize',
                            f' {DataAndSchema}, got {type(data_and_schema)}')
        data, schema = data_and_schema
        schema_version = self._cache.get(schema)
        if not schema_version:
            schema_name = self.schema_naming_strategy(
                topic, self.is_key, schema
            )
            LOG.info('Schema %s not cached locally, registering...',
                     schema_name)
            schema_version = self.client.get_or_register_schema_version(
                definition=schema.string,
                schema_name=schema_name,
                data_format=schema.data_format,
                compatibility_mode=self.compatibility_mode
            )
            self._cache[schema] = schema_version
        serialized = schema.write(data)
        return encode(serialized, schema_version.version_id)


class SchemaRegistryDeserializer:
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
            registries or handle schema-less data.
    """

    _writer_schemas: Dict[UUID, Schema]

    def __init__(
        self,
        client: SchemaRegistryClient,
        return_record_name: bool = False,
        secondary_deserializer: Deserializer = None
    ):
        self.client = client
        self.return_record_name = return_record_name
        self.secondary_deserializer = secondary_deserializer
        self._writer_schemas = {}

    def deserialize(self, topic: str, bytes_: bytes):
        if bytes_ is None:
            return None
        try:
            data_bytes, schema_version_id = decode(bytes_)
        except UnknownEncodingException as e:
            if self.secondary_deserializer:
                return self.secondary_deserializer.deserialize(topic, bytes_)
            else:
                raise SchemaRegistryException(
                    'no secondary deserializer provided to handle'
                    ' unrecognized data encoding'
                ) from e
        writer_schema = self._writer_schemas.get(schema_version_id)
        if not writer_schema:
            LOG.info('Schema version %s not found locally, fetching from'
                     ' registry...', schema_version_id)
            schema_version = self.client.get_schema_version(
                version_id=schema_version_id
            )
            if schema_version.data_format == 'AVRO':
                writer_schema = AvroSchema(schema_version.definition)
            elif schema_version.data_format == 'JSON':
                raise NotImplementedError('JSON schema not supported')
            self._writer_schemas[schema_version_id] = writer_schema
            LOG.info('Schema %s fetched', schema_version_id)
        return writer_schema.read(data_bytes)
