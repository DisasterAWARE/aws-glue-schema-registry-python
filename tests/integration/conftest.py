import logging
import os

import boto3
import pytest

from aws_schema_registry.client import TemporaryRegistry

LOG = logging.getLogger(__name__)

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')
AWS_REGION = os.getenv('AWS_REGION')
AWS_PROFILE = os.getenv('AWS_PROFILE')

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
    with TemporaryRegistry(
        glue_client,
        name='schema-registry-tests',
        description='Registry used for the schema registry python integration'
        ' tests. This registry does not hold any valuable data and is safe to'
        ' delete as long as it is not currently in use by a test',
        autoremove=CLEANUP_REGISTRY
    ) as registry:
        yield registry.name
