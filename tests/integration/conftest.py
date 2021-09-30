from datetime import datetime
import logging
import os

import boto3
import pytest

LOG = logging.getLogger(__name__)

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')
AWS_REGION = os.getenv('AWS_REGION')
AWS_PROFILE = os.getenv('AWS_PROFILE')

REGISTRY_PREFIX = 'schema-registry-tests'
DATE = datetime.utcnow().strftime('%y-%m-%d-%H-%M-%s')
CLEANUP_REGISTRY = os.getenv('CLEANUP_REGISTRY') is None


@pytest.fixture(scope='session')
def boto_session():
    return boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
        region_name=AWS_REGION,
        profile_name=AWS_PROFILE
    )


@pytest.fixture(scope='session')
def glue_client(boto_session):
    return boto_session.client('glue')


@pytest.fixture(scope='session')
def registry(glue_client):
    """The AWS Glue registry to use for testing."""
    name = f'{REGISTRY_PREFIX}-{DATE}'
    LOG.info('creating registry %s...' % name)
    glue_client.create_registry(
        RegistryName=name,
        Description='Registry used for the schema registry python integration'
        ' tests. This registry does not hold any valuable data and is safe to'
        ' delete as long as it is not currently in use by a test'
    )
    yield name
    if CLEANUP_REGISTRY:
        LOG.info('deleting registry %s...' % name)
        glue_client.delete_registry(RegistryId={'RegistryName': name})
