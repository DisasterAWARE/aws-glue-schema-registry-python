import pytest

from aws_schema_registry import ValidationError
from aws_schema_registry.avro import AvroSchema


def test_fully_qualified_name():
    s = AvroSchema('{"type": "record", "namespace": "foo", "name": "Bar"}')
    assert s.fqn == "foo.Bar"


def test_primitive_name():
    # fastavro does not fulfill this part of the Avro spec
    s = AvroSchema('{"type": "string"}')
    assert s.fqn == 'string'


def test_readwrite():
    s = AvroSchema('''
{
  "type": "record",
  "name": "JediMaster",
  "fields": [
    {"name": "name", "type": "string" },
    {"name": "age", "type": "int" }
  ]
}''')
    d = {
        'name': 'Yoda',
        'age': 900
    }
    assert s.read(s.write(d)) == d


def test_validation():
    s = AvroSchema('''
{
  "type": "record",
  "name": "JediMaster",
  "fields": [
    {"name": "name", "type": "string" },
    {"name": "age", "type": "int" }
  ]
}''')
    with pytest.raises(ValidationError) as e:
        s.validate({'name': 'Obi-Wan'})
    assert 'name' not in str(e)
    assert 'age' in str(e)
    with pytest.raises(ValidationError) as e:
        s.validate({'name': 1, 'age': 2})
    assert 'name' in str(e)
    assert 'age' not in str(e)
    s.validate({'name': 'Jar Jar', 'age': 42, 'sith': True})
