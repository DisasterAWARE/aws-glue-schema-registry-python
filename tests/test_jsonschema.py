import re
import pytest

import fastjsonschema

from aws_schema_registry import ValidationError
from aws_schema_registry.jsonschema import JsonSchema

FAST_JSON_SCHEMA_REPORTS_ONLY_MISSING = True
try:
    fastjsonschema_major_version = int(fastjsonschema.VERSION.split('.')[0])
    fastjsonschema_minor_version = int(fastjsonschema.VERSION.split('.')[1])
    if fastjsonschema_major_version == 2 and fastjsonschema_minor_version < 18:
        FAST_JSON_SCHEMA_REPORTS_ONLY_MISSING = False
except Exception:
    pass


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

    if FAST_JSON_SCHEMA_REPORTS_ONLY_MISSING:
        error_msg = "data must contain ['age'] properties"
    else:
        error_msg = "data must contain ['name', 'age'] properties"
    with pytest.raises(ValidationError, match=re.escape(error_msg)):
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
