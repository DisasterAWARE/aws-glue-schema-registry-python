from __future__ import annotations

from typing import Union

import orjson

import fastjsonschema

from aws_schema_registry.schema import DataFormat, Schema, ValidationError


class JsonSchema(Schema):
    """Implementation of the `Schema` protocol for JSON schemas.

    Arguments:
        definition: the schema, either as a parsed dict or a string
    """

    def __init__(self, definition: Union[str, dict]):
        if isinstance(definition, str):
            self._dict = orjson.loads(definition)
        else:
            self._dict = definition
        self._compiled_validation_method = fastjsonschema.compile(self._dict)

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return isinstance(other, JsonSchema) and \
               self._dict == other._dict

    def __str__(self):
        return orjson.dumps(self._dict).decode()

    def __repr__(self):
        return '<JsonSchema %s>' % self._dict

    @property
    def data_format(self) -> DataFormat:
        return 'JSON'

    @property
    def fqn(self) -> str:
        return ""

    def read(self, bytes_: bytes):
        data = orjson.loads(bytes_)
        self.validate(data)
        return data

    def write(self, data) -> bytes:
        self.validate(data)
        return orjson.dumps(data)

    def validate(self, data):
        try:
            self._compiled_validation_method(data)
        except fastjsonschema.exceptions.JsonSchemaValueException as e:
            raise ValidationError(str(e)) from e
