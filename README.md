# AWS Glue Schema Registry for Python


[![PyPI](https://img.shields.io/pypi/v/aws-glue-schema-registry.svg)](https://pypi.org/project/aws-glue-schema-registry)
[![PyPI](https://img.shields.io/pypi/pyversions/aws-glue-schema-registry)](https://pypi.org/project/aws-glue-schema-registry)
[![main](https://github.com/DisasterAWARE/aws-glue-schema-registry-python/actions/workflows/main.yml/badge.svg)](https://github.com/DisasterAWARE/aws-glue-schema-registry-python/actions/workflows/main.yml)

Use the AWS Glue Schema Registry in Python projects.

This library is a partial port of [aws-glue-schema-registry](https://github.com/awslabs/aws-glue-schema-registry) which implements a subset of its features with full compatibility.

## Feature Support

Feature | Java Library | Python Library | Notes
:------ | :----------- | :------------- | :----
Serialization and deserialization using schema registry | ✔️ | ✔️
Avro message format | ✔️ | ✔️
JSON Schema message format | ✔️ | ❌
Kafka Streams support | ✔️ | | N/A for Python, Kafka Streams is Java-only
Compression | ✔️ | ✔️ |
Local schema cache | ✔️ | ✔️
Schema auto-registration | ✔️ | ✔️
Evolution checks | ✔️ | ✔️
Migration from a third party Schema Registry | ✔️ | ✔️
Flink support | ✔️ | ❌
Kafka Connect support | ✔️ | | N/A for Python, Kafka Connect is Java-only

## Installation

Clone this repository and install it:

```
python setup.py install -e .
```

This library includes opt-in extra dependencies that enable support for certain features. For example, to use the schema registry with [kafka-python](https://pypi.org/project/kafka-python/), you should install the `kafka-python` extra:

```
python setup.py install -e .[kafka-python]
```

## Usage

First use `boto3` to create a low-level AWS Glue client:

```python
import boto3

# Pass your AWS credentials or profile information here
session = boto3.Session(access_key_id=xxx, secret_access_key=xxx, region_name='us-west-2')

glue_client = session.client('glue')
```

See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration for more information on configuring boto3.

Send Kafka messages with `SchemaRegistrySerializer`:

```python
from aws_schema_registry import DataAndSchema, SchemaRegistryClient
from aws_schema_registry.avro import AvroSchema

# In this example we will use kafka-python as our Kafka client,
# so we need to have the `kafka-python` extras installed and use
# the kafka adapter.
from aws_schema_registry.adapter.kafka import SchemaRegistrySerializer
from kafka import KafkaConsumer

# Create the schema registry client, which is a façade around the boto3 glue client
client = SchemaRegistryClient(glue_client,
                              registry_name='my-registry')

# Create the serializer
serializer = SchemaRegistrySerializer(client)

# Create the producer
producer = KafkaProducer(value_serializer=serializer)

# Our producer needs a schema to send along with the data.
# In this example we're using Avro, so we'll load an .avsc file.
with open('user.avsc', 'r') as schema_file:
    schema = AvroSchema(schema_file.read())

# Send message data along with schema
data = {
    'name': 'John Doe',
    'favorite_number': 6
}
producer.send('my-topic', value=DataAndSchema(data, schema))
# the value MUST be an instance of DataAndSchema when we're using the SchemaRegistrySerializer
```

Read Kafka messages with `SchemaRegistryDeserializer`:

```python
from aws_schema_registry import SchemaRegistryClient

# In this example we will use kafka-python as our Kafka client,
# so we need to have the `kafka-python` extras installed and use
# the kafka adapter.
from aws_schema_registry.adapter.kafka import SchemaRegistryDeserializer
from kafka import KafkaConsumer

# Create the schema registry client, which is a façade around the boto3 glue client
client = SchemaRegistryClient(glue_client,
                              registry_name='my-registry')

# Create the deserializer
deserializer = SchemaRegistryDeserializer(client)

# Create the consumer
consumer = KafkaConsumer('my-topic', value_deserializer=deserializer)

# Now use the consumer normally
for message in consumer:
    # The deserializer produces DataAndSchema instances
    value: DataAndSchema = message.value
    value.data
    value.schema
```

## Contributing

Clone this repository and install development dependencies:

```
pip install -e .[dev]
```

Run the linter and tests with tox before committing. After committing, check Github Actions to see the result of the automated checks.

### Linting

Lint the code with:

```
flake8
```

Run the type checker with:

```
mypy
```

### Tests

Tests go under the `tests/` directory. All tests outside of `tests/integration` are unit tests with no external dependencies.

Tests under `tests/integration` are integration test that interact with external resources and/or real AWS schema registries. They generally run slower and require some additional configuration.

Run just the unit tests with:

```
pytest --ignore tests/integration
```

All integration tests use the following environment variables:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN`
- `AWS_REGION`
- `AWS_PROFILE`
- `CLEANUP_REGISTRY`: Set to any value to prevent the test from destroying the registry created during the test, allowing you to inspect its contents.

If no `AWS_` environment variables are set, `boto3` will try to load credentials from your default AWS profile.

See individual integration test directories for additional requirements and setup instructions.

### Tox

This project uses [Tox](https://tox.wiki/en/latest/) to run tests across multiple Python versions.

Install Tox with:

```
pip install tox
```

and run it with:

```
tox
```

Note that Tox requires the tested python versions to be installed. One convenient way to manage this is using [pyenv](https://github.com/pyenv/pyenv#installation). See the `.python-versions` file for the Python versions that need to be installed.
