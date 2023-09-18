# Kafka integration tests

Tests that the serializer and deserializer works correctly with a real Kafka cluster.

Requires [Docker](https://www.docker.com/). Tested with Docker v20.

Run `docker-compose up -d -f tests/integration/kafka/docker-compose.yml` before running these tests. Destroy the docker stack with `docker-compose down -f tests/integration/kafka/docker-compose.yml`.
