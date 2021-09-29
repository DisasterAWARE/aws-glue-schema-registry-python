from __future__ import annotations

from dataclasses import dataclass
import sys
from typing import Any, Optional
from uuid import UUID

if sys.version_info[1] < 8:  # for py37
    from typing_extensions import Literal, Protocol
else:
    from typing import Literal, Protocol

DataFormat = Literal['AVRO', 'JSON']

CompatibilityMode = Literal['NONE', 'DISABLED', 'BACKWARD', 'BACKWARD_ALL',
                            'FORWARD', 'FORWARD_ALL', 'FULL', 'FULL_ALL']
"""Controls the checks performed on new schema versions.

Values:
    NONE: no compatibility checks performed
    DISABLED: no new versions can be added to the schema
    BACKWARD: consumer can read both current and previous version
    BACKWARD_ALL: consumer can read current and all previous
        versions
    FORWARD: consumer can read both current and subsequent version
    FORWARD_ALL: consumer can read both current and all subsequent
        versions
    FULL: combination of 'BACKWARD' and 'FORWARD'
    FULL_ALL: combination of 'BACKWARD_ALL' and 'FORWARD_ALL'
"""

SchemaStatus = Literal['AVAILABLE', 'PENDING', 'DELETING']
SchemaVersionStatus = Literal['AVAILABLE', 'PENDING', 'FAILURE', 'DELETING']


class Schema(Protocol):
    data_format: DataFormat
    name: str
    string: str

    def read(self, bytes_: bytes) -> Any: ...

    def write(self, data) -> bytes: ...


@dataclass
class SchemaVersion:
    schema_name: str
    version_id: UUID
    definition: str
    data_format: DataFormat
    status: SchemaVersionStatus
    version_number: Optional[int] = None
