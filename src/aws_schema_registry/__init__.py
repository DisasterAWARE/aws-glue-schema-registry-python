from .client import SchemaRegistryClient
from .exception import SchemaRegistryException
from .schema import CompatibilityMode, DataFormat, Schema, SchemaVersion
from .serde import (
    DataAndSchema, SchemaRegistryDeserializer, SchemaRegistrySerializer
)

__version__ = '1.0.0rc2'

__all__ = [
    'CompatibilityMode',
    'DataAndSchema',
    'DataFormat',
    'Schema',
    'SchemaRegistryClient',
    'SchemaRegistryDeserializer',
    'SchemaRegistryException',
    'SchemaRegistrySerializer',
    'SchemaVersion'
]
