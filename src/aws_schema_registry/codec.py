from __future__ import annotations

"""Functions to encode and decode values with schema information.

In order to benefit from using a schema registry with Kafka, key and
value data must be encoded with additional bits of information, which
effectively renders the raw bytes human unreadable.

The encoded data consists of the following components:

    byte | value
    ------------
    0    | magic byte that signifies whether the data was written with
         | a compatible client
    1    | which algorithm was used to compress the data bytes
    2-17 | uuid that indicates the writer schema
    18+  | actual data bytes, possibly compressed according to the
         | compression byte

This encoding is based on the Java Glue Schema Registry client
(https://github.com/awslabs/aws-glue-schema-registry) in an effort to
maintain full compatibility.
"""

from io import BytesIO
from uuid import UUID
import zlib

VERSION_BYTE = b'\x03'
"""Expected value of the magic version byte.

If the leading byte of the encoded data has a different value,
that signifies one of the following:

    1. The data was encoded by a different version of the encoder
    2. The data was encoded by a different library (e.g. the Java library)
       that is no longer compatible with this library
    3. The data was encoded for another schema registry
       (e.g. Confluent Schema Registry)
    4. The data was written by a schema-less producer
"""

COMPRESSION_ENABLED_BYTE = b'\x05'
"""Compression byte when using ZLIB compression."""

COMPRESSION_DISABLED_BYTE = b'\x00'
"""Compression byte when compression is disabled."""

SCHEMA_VERSION_ID_SIZE = 16
"""Number of bytes reserved for the schema version uuid."""


class CodecException(Exception):
    """Raised when encoding or decoding fails."""


class UnknownEncodingException(CodecException):
    """Raised when decoding data with an unknown encoding."""


def encode(data: bytes,
           schema_version_id: UUID,
           compression=None) -> bytes:
    """Encode data and schema information into bytes.

    Arguments:
        data (bytes): the payload itself.
        schema_version_id (UUID): version id of the schema used to
            serialize the data.
        compression (Any): whether to compress the payload data.
            Any truthy value can be passed to enable compression.

            Currently only ZLIB compression is supported. In future
            versions this parameter may take specific values to
            differentiate between different compression algorithms.

    Returns:
        bytes
    """
    b = BytesIO()
    if compression:
        compression_byte = COMPRESSION_ENABLED_BYTE
        data = zlib.compress(data)
    else:
        compression_byte = COMPRESSION_DISABLED_BYTE
    b.write(VERSION_BYTE)
    b.write(compression_byte)
    b.write(schema_version_id.bytes)
    b.write(data)
    value = b.getvalue()
    b.close()
    return value


def decode(bytes_: bytes) -> tuple[bytes, UUID]:
    """Decode bytes into data and schema information.

    Arguments:
        bytes_ (bytes): encoded bytes.

    Returns:
        tuple[bytes, UUID]: a two-item tuple consisting of the decoded
            and decompressed data, and the schema version id

    Raises:
        UnknownEncodingException: if the leading byte of the encoded
            data is not recognized, implying the data was encoded with
            an incompatible client or for a different schema registry
        CodecException: if any other error occurs while decoding
    """
    b = BytesIO(bytes_)
    version = b.read(1)
    if version != VERSION_BYTE:
        raise UnknownEncodingException(
            r"leading byte {version!r} not recognized"
        )
    compression = b.read(1)
    schema_version = UUID(bytes=b.read(SCHEMA_VERSION_ID_SIZE))
    data = b.read()
    if compression == COMPRESSION_ENABLED_BYTE:
        data = zlib.decompress(data)
    elif compression != COMPRESSION_DISABLED_BYTE:
        raise CodecException(
            f'compression byte {compression!r} not recognized'
        )
    return data, schema_version
