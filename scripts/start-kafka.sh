#!/bin/bash

# Script to start Kafka cluster using Docker Compose

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose-kafka.yml"

echo "Starting Kafka cluster with Docker Compose..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Start the services
docker compose -f "$COMPOSE_FILE" up -d

echo "Waiting for Kafka to be ready..."
sleep 10

# Wait for Kafka to be ready
echo "Checking Kafka connectivity..."
for i in {1..30}; do
    if docker exec kafka-broker /usr/bin/kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
        echo "Kafka is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "Error: Kafka failed to start within 5 minutes"
        echo "Check logs with: docker compose -f $COMPOSE_FILE logs"
        exit 1
    fi
    echo "Waiting for Kafka... (attempt $i/30)"
    sleep 10
done

# Create required topics
echo "Creating Kafka topics..."
docker exec kafka-broker /usr/bin/kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic test-input \
    --if-not-exists

docker exec kafka-broker /usr/bin/kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic test-output \
    --if-not-exists

echo ""
echo "✅ Kafka cluster is running!"
echo ""
echo "Kafka broker: localhost:9092"
echo "Zookeeper: localhost:2181"
echo ""
echo "To view logs: docker-compose -f $COMPOSE_FILE logs -f"
echo "To stop: docker-compose -f $COMPOSE_FILE down"
echo ""
echo "Topics created:"
echo "- test-input"
echo "- test-output"
echo ""
echo "To produce test data:"
echo "docker exec -it kafka-broker /usr/bin/kafka-console-producer --bootstrap-server localhost:9092 --topic test-input"
echo ""
echo "To consume output:"
echo "docker exec -it kafka-broker /usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic test-output --from-beginning"