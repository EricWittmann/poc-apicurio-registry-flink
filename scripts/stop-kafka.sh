#!/bin/bash

# Script to stop Kafka cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose-kafka.yml"

echo "Stopping Kafka cluster..."

docker compose -f "$COMPOSE_FILE" down

echo "✅ Kafka cluster stopped"
echo ""
echo "To remove volumes (delete all data): docker compose -f $COMPOSE_FILE down -v"