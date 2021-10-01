"""Strategies for choosing schema names in the registry.

The following strategies are included in this module:
    * topic_name_strategy (default)
    * record_name_strategy
    * topic_record_name_strategy

Use `help(<strategy>)` to see more information on each strategy.

Implement custom strategies by defining a function that fulfills the
SchemaNameStrategy:

```python
from aws_schema_registry import Schema

def custom_strategy(topic: str, is_key: bool, schema: Schema) -> str:
    ...
```
"""

import sys

if sys.version_info[1] < 8:  # py37
    from typing_extensions import Protocol
else:
    from typing import Protocol

from aws_schema_registry.schema import Schema


class SchemaNamingStrategy(Protocol):
    """Controls how schema names are chosen for a value.

    Arguments:
        topic: the name of the topic the value is being written into.
        value: the _unserialized_ value being written.
        is_key: whether the value is a Kafka key or value.

    Returns:
        str: the schema name to use
    """
    def __call__(self, topic: str, is_key: bool, schema: Schema) -> str: ...


def topic_name_strategy(topic: str, is_key: bool, schema: Schema) -> str:
    """The default naming strategy.

    Message keys are `<topic>-key` and message values are
    `<topic>-value`.

    This is a sensible strategy for topics whose records follow a uniform
    schema, but does not allow mixing different schemas on the same topic.
    """
    return f"{topic}-{'key' if is_key else 'value'}"


def record_name_strategy(topic: str, is_key: bool, schema: Schema) -> str:
    """Uses the fully-qualified record name as the schema name.

    Allows a topic to contain records with multiple incompatible schemas.
    However, this requires that the fully-qualified record names uniquely
    and consistently identify a schema across the entire registry.
    """
    return schema.fqn


def topic_record_name_strategy(topic: str, is_key: bool,
                               schema: Schema) -> str:
    """Combines the topic and record name to form the schema name.

    Allows a topic to contain records with multiple incompatible schemas.
    Additionally allows different topics to use the same record name for
    incompatible schemas.
    """
    return f'{topic}-{schema.fqn}'
