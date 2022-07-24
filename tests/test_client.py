from unittest.mock import Mock
from uuid import UUID

import pytest

from aws_schema_registry.client import (
    SchemaRegistryClient
)

REGISTRY_NAME = 'user-topic'
SCHEMA_NAME = 'User-Topic'
JSON_SCHEMA_NAME = 'User-Topic-json'
SCHEMA_ARN = f'arn:aws:glue:us-west-2:123:schema/{REGISTRY_NAME}/{SCHEMA_NAME}'
SCHEMA_VERSION_ID = UUID('b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63')
JSON_SCHEMA_VERSION_ID = UUID('98718bb6-ca2a-4ac6-b841-748cab68b1b1')
SCHEMA_DEF = '{"name": "Test", "type": "record", "fields": []}'
JSON_SCHEMA_DEF = """{
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
    }"""

METADATA = {
    'event-source-1': 'topic1',
    'event-source-2': 'topic2',
    'event-source-3': 'topic3',
    'event-source-4': 'topic4',
    'event-source-5': 'topic5'
}


@pytest.fixture
def glue_client():
    return Mock()


@pytest.fixture
def client(glue_client):
    return SchemaRegistryClient(
        glue_client,
        registry_name=REGISTRY_NAME,
        wait_interval_seconds=0
    )


def test_get_schema_version(client, glue_client):
    glue_client.get_schema_version = Mock(return_value={
        'SchemaVersionId': str(SCHEMA_VERSION_ID),
        'SchemaDefinition': SCHEMA_DEF,
        'SchemaArn': SCHEMA_ARN,
        'DataFormat': 'AVRO',
        'VersionNumber': 123,
        'Status': 'AVAILABLE'
    })

    version = client.get_schema_version(SCHEMA_VERSION_ID)

    assert version.version_id == SCHEMA_VERSION_ID


def test_get_schema_by_definition(client, glue_client):
    glue_client.get_schema_by_definition = Mock(return_value={
        'SchemaVersionId': str(SCHEMA_VERSION_ID),
        'SchemaArn': SCHEMA_ARN,
        'DataFormat': 'AVRO',
        'Status': 'AVAILABLE'
    })

    version = client.get_schema_by_definition(SCHEMA_DEF, SCHEMA_NAME)

    assert version.version_id == SCHEMA_VERSION_ID
