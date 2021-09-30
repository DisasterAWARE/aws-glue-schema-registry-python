import logging
import os
import subprocess

from aws_schema_registry import DataAndSchema, SchemaRegistryClient
from aws_schema_registry.avro import AvroSchema
from aws_schema_registry.adapter.kafka import (
    SchemaRegistryDeserializer, SchemaRegistrySerializer
)

LOG = logging.getLogger(__name__)

JAVA_CODE_LOCATION = os.path.dirname(__file__)
JAR_LOCATION = os.path.join(
    JAVA_CODE_LOCATION,
    'target',
    'java-integration-test.jar'
)

with open(os.path.join(os.path.dirname(__file__), 'user.avsc'), 'r') as f:
    SCHEMA = AvroSchema(f.read())


def test_interop_with_java_library(glue_client, registry, boto_session):
    client = SchemaRegistryClient(glue_client, registry_name=registry)
    serializer = SchemaRegistrySerializer(client)
    deserializer = SchemaRegistryDeserializer(client)

    data = {
        'name': 'John Doe',
        'favorite_number': 6,
        'favorite_color': 'red'
    }
    serialized: bytes = serializer.serialize(
        'test', DataAndSchema(data, SCHEMA)
    )

    if not os.path.exists(JAR_LOCATION):
        LOG.info('Test java jar not found at %s, trying to compile...',
                 JAR_LOCATION)
        compile_java()
    credentials = boto_session.get_credentials()
    proc = subprocess.run(
        ['java', '-jar', JAR_LOCATION],
        input=serialized,
        capture_output=True,
        env={
            'AWS_ACCESS_KEY_ID': credentials.access_key,
            'AWS_SECRET_ACCESS_KEY': credentials.secret_key,
            'AWS_SESSION_TOKEN': credentials.token,
            'AWS_REGION': boto_session.region_name,
            'REGISTRY_NAME': registry,
            'SCHEMA_NAME': 'sometestschema'
        }
    )
    proc.check_returncode()
    assert deserializer.deserialize('test', proc.stdout) == data


def compile_java():
    LOG.info('Finding mvn...')
    find_mvn_proc = subprocess.run(['which', 'mvn'], capture_output=True)
    if find_mvn_proc.returncode != 0:
        raise Exception('Cannot find an installation of maven to compile the'
                        ' java test code. Compile manually or install mvn.')
    mvn = find_mvn_proc.stdout.decode('utf-8').strip()
    LOG.info('mvn found at %s', mvn)
    LOG.info('compiling...')
    compile_proc = subprocess.run(
        [mvn, 'clean', 'package'],
        cwd=JAVA_CODE_LOCATION
    )
    compile_proc.check_returncode()
