from __future__ import annotations

from io import BytesIO
import json

import fastavro

from aws_schema_registry.schema import DataFormat, Schema


class AvroSchema(Schema):
    """Implementation of the `Schema` protocol for Avro schemas.

    Arguments:
        string: the stringified schema definition
        return_record_name: if true, when reading a union of records,
            the result will be a tuple where the first value is the
            name of the record and the second value is the record
            itself
    """

    def __init__(self, string: str, return_record_name: bool = False):
        self._dict = json.loads(string)
        self._parsed = fastavro.parse_schema(self._dict)
        self.return_record_name = return_record_name

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return isinstance(other, AvroSchema) and \
            self._parsed == other._parsed and \
            self.return_record_name == other.return_record_name

    def __str__(self):
        return json.dumps(self._dict)

    def __repr__(self):
        return '<AvroSchema %s>' % self._dict

    @property
    def data_format(self) -> DataFormat:
        return 'AVRO'

    @property
    def fqn(self) -> str:
        # https://github.com/fastavro/fastavro/issues/415
        return self._parsed.get('name', self._parsed['type'])

    def read(self, bytes_: bytes):
        b = BytesIO(bytes_)
        value = fastavro.schemaless_reader(
            b,
            self._parsed,
            return_record_name=self.return_record_name
        )
        b.close()
        return value

    def write(self, data) -> bytes:
        b = BytesIO()
        fastavro.schemaless_writer(b, self._parsed, data)
        value = b.getvalue()
        b.close()
        return value
