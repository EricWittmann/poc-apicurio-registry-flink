#!/bin/bash

# Script to send test data to Kafka

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SAMPLE_DATA="$SCRIPT_DIR/sample-data.json"

echo "Sending test data to Kafka..."

if [ ! -f "$SAMPLE_DATA" ]; then
    echo "Error: Sample data file not found at $SAMPLE_DATA"
    exit 1
fi

# Check if Kafka container is running
if ! docker ps | grep -q kafka-broker; then
    echo "Error: Kafka broker container is not running. Start it with ./start-kafka.sh"
    exit 1
fi

# Send data to test-input topic
echo "Sending data to test-input topic..."
docker exec -i kafka-broker /usr/bin/kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic test-input < "$SAMPLE_DATA"

echo "✅ Test data sent successfully!"
echo ""
echo "To view the data, run:"
echo "docker exec -it kafka-broker /usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic test-input --from-beginning"