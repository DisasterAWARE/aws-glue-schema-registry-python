from uuid import uuid4

import pytest

from aws_schema_registry.codec import (
    CodecException, encode, decode,
    UnknownEncodingException
)


@pytest.mark.parametrize('compression', [None, 'zlib'])
def test_codec(compression):
    data = (1024).to_bytes(2, 'big')
    schema_version_id = uuid4()
    encoded = encode(data, schema_version_id, compression=compression)
    decoded = decode(encoded)
    assert decoded[0] == data
    assert decoded[1] == schema_version_id


def test_unknown_leading_byte():
    # leading byte '0' is what the Confluent Schema Registry client uses
    bytes_ = b'\x00\x05\x00\x00'
    with pytest.raises(UnknownEncodingException):
        decode(bytes_)


def test_unknown_compression_byte():
    bytes_ = b'\x00\x01\x00\x00'
    with pytest.raises(CodecException):
        decode(bytes_)
