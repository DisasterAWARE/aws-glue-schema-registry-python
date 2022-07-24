from datetime import datetime
import logging
import time
import random
import string
from typing import ContextManager, Mapping
from uuid import UUID

from aws_schema_registry.schema import (
    CompatibilityMode, DataFormat, SchemaVersion
)
from aws_schema_registry.exception import SchemaRegistryException

LOG = logging.getLogger(__name__)

SCHEMA_VERSION_NOT_FOUND_MSG = 'Schema version is not found.'
SCHEMA_NOT_FOUND_MSG = 'Schema is not found.'

DEFAULT_COMPATIBILITY_MODE: CompatibilityMode = 'BACKWARD'


def schema_name_from_arn(arn: str) -> str:
    return arn.split('/')[-1]


class SchemaRegistryClient:
    """Façade that makes the registry API easier to use.

    Simplifies the large boto glue client to just operations on a
    single registry at a time and hides HTTP communication details.

    Arguments:
        glue_client: glue client created by `botocore`/`boto3`.
        registry_name: the name of the registry this client will work
            against. If not specified, defaults to the default registry
            which is named 'default-registry'.
        max_wait_attempts: maximum number of times to check whether a
            newly created schema has become available before reporting
            an error.
        wait_interval_seconds: delay in seconds between checking
            whether a newly created schema has become available.
    """

    def __init__(
        self,
        glue_client,
        registry_name: str = 'default-registry',
        max_wait_attempts: int = 10,
        wait_interval_seconds: float = 3
    ):
        self.glue_client = glue_client
        self.registry_name = registry_name
        self.max_wait_attempts = max_wait_attempts
        self.wait_interval_seconds = wait_interval_seconds

    def get_schema_version(self, version_id: UUID) -> SchemaVersion:
        """Get a schema version from the registry by id.

        Arguments:
            version_id: the schema version's unique id.

        Returns:
            SchemaVersion
        """
        try:
            res = self.glue_client.get_schema_version(
                SchemaVersionId=str(version_id)
            )
        except Exception as e:
            raise SchemaRegistryException(
                f'Failed to get schema version by id {version_id}'
            ) from e
        if (
            res['SchemaVersionId'] is None or
            res['Status'] != 'AVAILABLE'
        ):
            raise SchemaRegistryException(
                f"Schema Found but status is {res['Status']}"
            )
        return SchemaVersion(
            schema_name=schema_name_from_arn(res['SchemaArn']),
            version_id=UUID(res['SchemaVersionId']),
            definition=res['SchemaDefinition'],
            data_format=res['DataFormat'],
            status=res['Status'],
            version_number=res['VersionNumber']
        )

    def get_schema_by_definition(
        self,
        definition: str,
        schema_name: str
    ) -> SchemaVersion:
        """Get a schema version from the registry by schema definition.

        Arguments:
            definition: the stringified schema definition.
            schema_name: the name of the schema.

        Returns:
            SchemaVersion
        """
        try:
            LOG.debug(
                'Getting schema version id for: name = %s, definition = %s',
                schema_name, definition
            )
            res = self.glue_client.get_schema_by_definition(
                SchemaId={
                    'SchemaName': schema_name,
                    'RegistryName': self.registry_name
                },
                SchemaDefinition=definition
            )
            if (
                res['SchemaVersionId'] is None or
                res['Status'] != 'AVAILABLE'
            ):
                raise SchemaRegistryException(
                    f"Schema Found but status is {res['Status']}"
                )
            return SchemaVersion(
                schema_name=schema_name_from_arn(res['SchemaArn']),
                version_id=UUID(res['SchemaVersionId']),
                definition=definition,
                data_format=res['DataFormat'],
                status=res['Status']
            )
        except Exception as e:
            raise SchemaRegistryException(
                'Failed to get schemaVersionId by schema definition for schema'
                f' name = {schema_name}'
            ) from e


class TemporaryRegistry(ContextManager):
    """A real schema registry for use in tests and experiments.

    This class implements the ContextManager protocol, creating the registry
    on enter and destroying it on exit.

    Usage:

```python
    with TemporaryRegistry(glue_client, 'MyRegistry') as r:
        # registry is created on enter
        print(r.name)  # the "real" (suffixed) registry name
        # registry is destroyed on exit
```

    Arguments:
        glue_client: glue client created by `botocore`/`boto3`.
        name: human-readable name for the created registry. The name will be
            suffixed by a random identifier to reduce the freqency of
            collisions.
        description: description for the created registry.
        autoremove: whether to destroy the created registry. Defaults to True.
    """

    DEFAULT_DESCRIPTION = 'Temporary registry created with the aws-glue-schema-registry Python library.'  # NOQA

    def __init__(self, glue_client,
                 name: str = 'temporary-registry',
                 description: str = DEFAULT_DESCRIPTION,
                 autoremove: bool = True):
        self.glue_client = glue_client
        date = datetime.utcnow().strftime('%y-%m-%d-%H-%M')
        r = ''.join(random.choices(string.digits + string.ascii_letters,
                                   k=16))
        self.name = f'{name}-{date}-{r}'
        self.description = description
        self.autoremove = autoremove

    def __enter__(self):
        LOG.info('creating registry %s...' % self.name)
        self.glue_client.create_registry(
            RegistryName=self.name,
            Description=self.description
        )
        return self

    def __exit__(self, *args):
        if self.autoremove:
            LOG.info('deleting registry %s...' % self.name)
            self.glue_client.delete_registry(
                RegistryId={'RegistryName': self.name}
            )
