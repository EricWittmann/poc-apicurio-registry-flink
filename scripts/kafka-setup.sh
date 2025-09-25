#!/bin/bash

# Script to set up Kafka topics for testing

KAFKA_HOME=${KAFKA_HOME:-/opt/kafka}
BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS:-localhost:9092}

echo "Creating Kafka topics for Flink catalog test..."

# Create input topic
$KAFKA_HOME/bin/kafka-topics.sh --create \
  --bootstrap-server $BOOTSTRAP_SERVERS \
  --replication-factor 1 \
  --partitions 3 \
  --topic test-input \
  --if-not-exists

# Create output topic
$KAFKA_HOME/bin/kafka-topics.sh --create \
  --bootstrap-server $BOOTSTRAP_SERVERS \
  --replication-factor 1 \
  --partitions 3 \
  --topic test-output \
  --if-not-exists

echo "Topics created successfully!"

echo "To produce test data to the input topic, run:"
echo "$KAFKA_HOME/bin/kafka-console-producer.sh --bootstrap-server $BOOTSTRAP_SERVERS --topic test-input"
echo ""
echo "To consume from the output topic, run:"
echo "$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP_SERVERS --topic test-output --from-beginning"