import logging
import time
from typing import Mapping
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

    def get_or_register_schema_version(
        self,
        definition: str,
        schema_name: str,
        data_format: DataFormat,
        compatibility_mode: CompatibilityMode = DEFAULT_COMPATIBILITY_MODE,
        metadata: Mapping[str, str] = None
    ) -> SchemaVersion:
        """Get Schema Version ID by following below steps:

        1) If schema version id exists in registry then get it from registry
        2) If schema version id does not exist in registry
             then if schema exists but version doesn't exist
                 then
                 2.1) Register schema version
             else if schema does not exist
                 then
                 2.2) create schema and register schema version

        Arguments:
            definition: the stringified schema definition.
            schema_name: the name of the schema in the registry.
            data_format: which format to use if creating the schema.
                Has no effect if the schema by name already exists.
            compatibility_mode: which compatibility mode to use if
                creating the schema. Has no effect if the schema by
                name already exists.
            metadata: optional metadata to add to the schema version
                if registering a new version. Has no effect if a
                schema version matching the specified definition already
                exists.
        """
        try:
            version = self.get_schema_by_definition(
                definition, schema_name
            )
        except SchemaRegistryException as e:
            cause_msg = str(e.__cause__)
            if SCHEMA_VERSION_NOT_FOUND_MSG in cause_msg:
                LOG.debug(cause_msg)
                version_id = self.register_schema_version(
                    definition, schema_name, metadata
                )
            elif SCHEMA_NOT_FOUND_MSG in cause_msg:
                LOG.debug(cause_msg)
                version_id = self.create_schema(
                    schema_name, data_format, definition, compatibility_mode,
                    metadata
                )
            else:
                raise SchemaRegistryException(
                    'Exception occurred while fetching or registering schema'
                    f' definition = {definition}, schema name = {schema_name}'
                ) from e
            version = self.get_schema_version(version_id)
        return version

    def register_schema_version(
        self,
        definition: str,
        schema_name: str,
        metadata: Mapping[str, str] = None
    ) -> UUID:
        """Register a new version to an existing schema.

        Waits until the new version becomes available before returning.

        Arguments:
            definition: the schema definition.
            schema_name: the name of the schema.
            metadata (optional): version metadata key-value pairs.

        Returns:
            UUID: the id of the new schema version
        """
        try:
            res = self.glue_client.register_schema_version(
                SchemaId={
                    'SchemaName': schema_name,
                    'RegistryName': self.registry_name
                },
                SchemaDefinition=definition
            )
            version_id = UUID(res['SchemaVersionId'])
            LOG.info('Registered the schema version with schema version '
                     'id = %s and with version number = %s and status %s',
                     version_id, res['VersionNumber'], res['Status'])
            if res['Status'] != 'AVAILABLE':
                self._wait_for_schema_evolution_check_to_complete(version_id)
        except Exception as e:
            raise SchemaRegistryException(
                'Register schema :: Call failed when registering the schema'
                f' with the schema registry for schema name = {schema_name}',
            ) from e
        if metadata:
            self.put_schema_version_metadata(version_id, metadata)
        return version_id

    def _wait_for_schema_evolution_check_to_complete(
        self,
        schema_version_id: UUID
    ):
        time.sleep(self.wait_interval_seconds)
        for _ in range(self.max_wait_attempts):
            res = self.glue_client.get_schema_version(
                SchemaVersionId=str(schema_version_id)
            )
            status = res['Status']
            if status == 'AVAILABLE':
                break
            elif status != 'PENDING':
                raise SchemaRegistryException(
                    'Schema evolution check failed.'
                    f' schemaVersionId {schema_version_id} is in'
                    f' {status} status.'
                )
        else:
            raise SchemaRegistryException(
                'Retries exhausted for schema evolution check for '
                f'schemaVersionId = {schema_version_id}'
            )

    def put_schema_version_metadata(
        self,
        version_id: UUID,
        metadata: Mapping[str, str]
    ):
        for k, v in metadata.items():
            try:
                self.glue_client.put_schema_version_metadata(
                    SchemaVersionId=str(version_id),
                    MetadataKeyValue={
                        'MetadataKey': k,
                        'MetadataValue': v
                    }
                )
            except Exception as e:
                raise SchemaRegistryException(
                    'Put schema version metadata :: Call failed when put'
                    f' metadata key = {k} value = {v} to schema for schema'
                    f' versionid = {version_id}'
                ) from e

    def create_schema(
        self,
        name: str,
        data_format: DataFormat,
        definition: str,
        compatibility_mode: CompatibilityMode = DEFAULT_COMPATIBILITY_MODE,
        metadata: Mapping[str, str] = None
    ) -> UUID:
        """Create a new schema and return the version id."""
        try:
            LOG.info('Creating schema with name: %s and definition: %s',
                     name, definition)
            res = self.glue_client.create_schema(
                SchemaName=name,
                RegistryId={
                    'RegistryName': self.registry_name
                },
                DataFormat=data_format,
                Compatibility=compatibility_mode,
                Description='',
                Tags={},
                SchemaDefinition=definition
            )
            version_id = UUID(res['SchemaVersionId'])
            if metadata:
                self.put_schema_version_metadata(version_id, metadata)
        except Exception as e:
            if type(e).__name__ == 'AlreadyExistsException':
                LOG.warn('Schema is already created, this could be caused by '
                         'multiple producers racing to auto-create schema.')
                version_id = self.register_schema_version(
                    definition, name, metadata
                )
            else:
                raise SchemaRegistryException(
                    f'Create schema {name} failed'
                ) from e
        return version_id
