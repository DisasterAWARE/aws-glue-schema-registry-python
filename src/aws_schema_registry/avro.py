from __future__ import annotations

from io import BytesIO
import json

import fastavro

from aws_schema_registry.schema import DataFormat


class AvroSchema:
    """Implementation of the `Schema` protocol for Avro schemas.

    Arguments:
        string: the stringified schema definition
        return_record_name: if true, when reading a union of records,
            the result will be a tuple where the first value is the
            name of the record and the second value is the record
            itself
    """

    data_format: DataFormat = 'AVRO'
    parsed: dict

    def __init__(self, string: str, return_record_name: bool = False):
        self.string = string
        self.parsed = fastavro.parse_schema(json.loads(string))
        # https://github.com/fastavro/fastavro/issues/415
        self.name = self.parsed.get('name', self.parsed['type'])
        self.return_record_name = return_record_name

    def read(self, bytes_: bytes):
        b = BytesIO(bytes_)
        value = fastavro.schemaless_reader(
            b,
            self.parsed,
            return_record_name=self.return_record_name
        )
        b.close()
        return value

    def write(self, data) -> bytes:
        b = BytesIO()
        fastavro.schemaless_writer(b, self.parsed, data)
        value = b.getvalue()
        b.close()
        return value
