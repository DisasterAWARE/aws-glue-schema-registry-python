import re
import pytest

from aws_schema_registry import ValidationError
from aws_schema_registry.jsonschema import JsonSchema


def test_readwrite():
    s = JsonSchema("""{
          "$schema": "http://json-schema.org/draft-04/schema#",
          "type": "object",
          "properties": {
            "name": {
              "type": "string"
            },
            "age": {
              "type": "integer"
            }
          },
          "required": [
            "name",
            "age"
          ]
        }""")

    d = {
        'name': 'Yoda',
        'age': 900
    }

    assert s.read(s.write(d)) == d


def test_validation_during_read_write():
    s = JsonSchema("""{
          "$schema": "http://json-schema.org/draft-04/schema#",
          "type": "object",
          "properties": {
            "name": {
              "type": "string"
            },
            "age": {
              "type": "integer"
            }
          },
          "required": [
            "name",
            "age"
          ]
        }""")

    with pytest.raises(ValidationError, match=re.escape(
        "data.name must be string"
    )):
        s.read(b'{"name": 1, "age": 2}')

    with pytest.raises(ValidationError, match=re.escape(
        "data.name must be string"
    )):
        s.write({"name": 1, "age": 2})


def test_validation():
    s = JsonSchema("""{
      "$schema": "http://json-schema.org/draft-04/schema#",
      "type": "object",
      "properties": {
        "name": {
          "type": "string"
        },
        "age": {
          "type": "integer"
        }
      },
      "required": [
        "name",
        "age"
      ]
    }""")

    with pytest.raises(
        ValidationError,
        # fastjsonschema>=2.18.0 reports only missing properties, so it will
        # exclude 'name'
        match=r"data must contain \[('name', )?'age'\] properties"
    ):
        s.validate({'name': 'Obi-Wan'})
    with pytest.raises(ValidationError, match=re.escape(
        "data.name must be string"
    )):
        s.validate({'name': 1, 'age': 2})

    s.validate({'name': 'Jar Jar', 'age': 42, 'sith': True})

    s = JsonSchema("""{
          "$schema": "http://json-schema.org/draft-04/schema#",
          "type": "object",
          "properties": {
            "name": {
              "type": "string"
            },
            "age": {
              "type": "integer"
            }
          },
          "required": [
            "name",
            "age"
          ],
          "additionalProperties": false
        }""")

    with pytest.raises(ValidationError, match=re.escape(
        "data must not contain {'sith'} properties"
    )):
        s.validate({'name': 'Jar Jar', 'age': 42, 'sith': True})
