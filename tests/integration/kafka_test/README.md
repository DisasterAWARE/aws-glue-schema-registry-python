# Kafka integration tests

Tests that the serializer and deserializer works correctly with a real Kafka cluster.

Requires [Docker](https://www.docker.com/). Tested with Docker v20.

Run `docker compose -f tests/integration/kafka_test/docker-compose.yml up -d` before running these tests. Destroy the docker stack with `docker compose -f tests/integration/kafka_test/docker-compose.yml down`.
