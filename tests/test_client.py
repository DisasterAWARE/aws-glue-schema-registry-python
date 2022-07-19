from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest

from aws_schema_registry.client import (
    SchemaRegistryClient, SCHEMA_NOT_FOUND_MSG, SCHEMA_VERSION_NOT_FOUND_MSG
)
from aws_schema_registry.exception import SchemaRegistryException

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


def test_get_or_register_schema_version_default_does_not_create_schema(client, glue_client):
    glue_client.get_schema_by_definition = Mock(
        side_effect=SchemaRegistryException(
            Exception(SCHEMA_NOT_FOUND_MSG)
        ))
    mock_create_schema(glue_client)
    mock_get_schema_version(glue_client, 'AVRO')

    with pytest.raises(SchemaRegistryException) as expected_exception:
        get_or_register_schema_version(client, 'AVRO', False)

    assert SCHEMA_NOT_FOUND_MSG in str(expected_exception)

    mock_get_schema_version(glue_client, 'JSON')

    with pytest.raises(SchemaRegistryException) as expected_exception:
        get_or_register_schema_version(client, 'JSON', False)

    assert SCHEMA_NOT_FOUND_MSG in str(expected_exception)


def test_get_or_register_schema_version_creates_schema(client, glue_client):
    glue_client.get_schema_by_definition = Mock(
        side_effect=SchemaRegistryException(
            Exception(SCHEMA_NOT_FOUND_MSG)
        ))
    mock_create_schema(glue_client)
    mock_get_schema_version(glue_client, 'AVRO')

    version = get_or_register_schema_version(client, 'AVRO', True)

    assert version.version_id == SCHEMA_VERSION_ID

    mock_get_schema_version(glue_client, 'JSON')

    version = get_or_register_schema_version(client, 'JSON', True)

    assert version.version_id == SCHEMA_VERSION_ID


def test_get_or_register_schema_version_default_does_not_register_version(
    client, glue_client
):
    glue_client.get_schema_by_definition = Mock(
        side_effect=SchemaRegistryException(
            Exception(SCHEMA_VERSION_NOT_FOUND_MSG)
        ))
    mock_register_schema_version(glue_client)
    mock_get_schema_version(glue_client, 'AVRO')

    with pytest.raises(SchemaRegistryException) as expected_exception:
        get_or_register_schema_version(client, 'AVRO', False)

    assert SCHEMA_VERSION_NOT_FOUND_MSG in str(expected_exception)

    mock_get_schema_version(glue_client, 'JSON')

    with pytest.raises(SchemaRegistryException) as expected_exception:
        get_or_register_schema_version(client, 'JSON', False)

    assert SCHEMA_VERSION_NOT_FOUND_MSG in str(expected_exception)


def test_get_or_register_schema_version_registers_version(
    client, glue_client
):
    glue_client.get_schema_by_definition = Mock(
        side_effect=SchemaRegistryException(
            Exception(SCHEMA_VERSION_NOT_FOUND_MSG)
        ))
    mock_register_schema_version(glue_client)
    mock_get_schema_version(glue_client, 'AVRO')

    version = get_or_register_schema_version(client, 'AVRO', True)

    assert version.version_id == SCHEMA_VERSION_ID

    mock_get_schema_version(glue_client, 'JSON')

    version = get_or_register_schema_version(client, 'JSON', True)

    assert version.version_id == SCHEMA_VERSION_ID


@pytest.mark.parametrize(
    "schema_def,schema_name,schema_ver_id",
    [(SCHEMA_DEF, SCHEMA_NAME, SCHEMA_VERSION_ID),
     (JSON_SCHEMA_DEF, JSON_SCHEMA_NAME, JSON_SCHEMA_VERSION_ID)])
def test_register_schema_version(client, glue_client,
                                 schema_name, schema_def, schema_ver_id):
    print(schema_name, schema_def, schema_ver_id)
    glue_client.register_schema_version = Mock(return_value={
        'SchemaVersionId': str(schema_ver_id),
        'VersionNumber': 1,
        'Status': 'AVAILABLE'
    })

    version_id = client.register_schema_version(schema_def, schema_name)

    assert version_id == schema_ver_id


def test_wait_for_schema_evolution_check_to_complete(client, glue_client):
    responses = [
        {
            'SchemaVersionId': str(SCHEMA_VERSION_ID),
            'Status': 'PENDING'
        }, {
            'SchemaVersionId': str(SCHEMA_VERSION_ID),
            'Status': 'AVAILABLE'
        }
    ]
    glue_client.get_schema_version = Mock(side_effect=responses)

    client._wait_for_schema_evolution_check_to_complete(SCHEMA_VERSION_ID)


def test_schema_evolution_timeout(client, glue_client):
    glue_client.get_schema_version = Mock(return_value={
        'SchemaVersionId': str(SCHEMA_VERSION_ID),
        'Status': 'PENDING'
    })

    with pytest.raises(SchemaRegistryException):
        client._wait_for_schema_evolution_check_to_complete(SCHEMA_VERSION_ID)

    assert glue_client.get_schema_version.call_count == 10


def test_put_schema_version_metadata_succeeds(client, glue_client):
    glue_client.put_schema_version_metadata = Mock(
        side_effect=_make_put_schema_version_metadata_response
    )

    client.put_schema_version_metadata(SCHEMA_VERSION_ID, METADATA)

    assert (
        glue_client.put_schema_version_metadata.call_count
        ==
        len(METADATA)
    )
    for k, v in METADATA.items():
        glue_client.put_schema_version_metadata.assert_any_call(
            SchemaVersionId=str(SCHEMA_VERSION_ID),
            MetadataKeyValue={
                'MetadataKey': k,
                'MetadataValue': v
            }
        )


def _make_put_schema_version_metadata_response(
    SchemaVersionId: str,
    MetadataKeyValue: dict
):
    return {
        'SchemaVersionId': SchemaVersionId,
        'MetadataKey': MetadataKeyValue['MetadataKey'],
        'MetadataValue': MetadataKeyValue['MetadataValue']
    }


@pytest.mark.parametrize("data_format", ["AVRO", "JSON"])
def test_create_schema(client, glue_client, data_format):
    schema_version_id = uuid4()
    glue_client.create_schema = Mock(return_value={
        'SchemaName': SCHEMA_NAME,
        'DataFormat': data_format,
        'SchemaVersionId': str(schema_version_id)
    })

    version_id = client.create_schema(
        SCHEMA_NAME, data_format, SCHEMA_DEF
    )

    assert version_id == schema_version_id


def mock_get_schema_version(glue_client, data_format):
    glue_client.get_schema_version = Mock(return_value={
        'SchemaVersionId': str(SCHEMA_VERSION_ID),
        'SchemaDefinition': SCHEMA_DEF,
        'DataFormat': data_format,
        'SchemaArn': SCHEMA_ARN,
        'VersionNumber': 123,
        'Status': 'AVAILABLE'
    })


def mock_register_schema_version(glue_client):
    glue_client.register_schema_version = Mock(return_value={
        'SchemaVersionId': str(SCHEMA_VERSION_ID),
        'VersionNumber': 123,
        'Status': 'AVAILABLE'
    })


def mock_create_schema(glue_client):
    glue_client.create_schema = Mock(return_value={
        'RegistryName': REGISTRY_NAME,
        'SchemaName': SCHEMA_NAME,
        'Description': '',
        'DataFormat': 'AVRO',
        'Compatibility': 'BACKWARD',
        'SchemaStatus': 'AVAILABLE',
        'SchemaVersionId': str(SCHEMA_VERSION_ID),
        'SchemaVersionStatus': 'AVAILABLE'
    })


def get_or_register_schema_version(client, data_format, auto_register_schema):
    return client.get_or_register_schema_version(
        definition=SCHEMA_DEF,
        schema_name=SCHEMA_NAME,
        data_format=data_format,
        auto_register_schema=auto_register_schema
    )
