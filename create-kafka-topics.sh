#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Kafka broker address
KAFKA_BROKER="localhost:9092"

# Topics to create
TOPICS=(
    "flink.click.stream"
    "flink.sales.records"
    "flink.recommended.products"
)

# Function to check if a topic exists
topic_exists() {
    local topic=$1
    docker exec kafka kafka-topics --bootstrap-server $KAFKA_BROKER --list | grep -q "^$topic$"
    return $?
}

# Function to create a topic
create_topic() {
    local topic=$1
    echo -e "${YELLOW}Creating topic: $topic${NC}"
    
    if docker exec kafka kafka-topics --create \
        --topic "$topic" \
        --bootstrap-server $KAFKA_BROKER \
        --partitions 1 \
        --replication-factor 1; then
        echo -e "${GREEN}Successfully created topic: $topic${NC}"
        return 0
    else
        echo -e "${RED}Failed to create topic: $topic${NC}"
        return 1
    fi
}

# Check if Kafka is running
echo "Checking Kafka connection..."
if ! docker exec kafka kafka-topics --bootstrap-server $KAFKA_BROKER --list > /dev/null 2>&1; then
    echo -e "${RED}Error: Cannot connect to Kafka at $KAFKA_BROKER${NC}"
    echo "Please ensure Kafka is running (docker-compose up -d)"
    exit 1
fi

echo -e "${GREEN}Connected to Kafka successfully${NC}\n"

# Create each topic if it doesn't exist
for topic in "${TOPICS[@]}"; do
    if topic_exists "$topic"; then
        echo -e "${YELLOW}Topic '$topic' already exists${NC}"
    else
        if ! create_topic "$topic"; then
            echo -e "${RED}Error: Failed to create one or more topics${NC}"
            exit 1
        fi
    fi
done

echo -e "\n${GREEN}All topics are ready!${NC}"
echo "You can verify the topics using:"
echo "docker exec kafka kafka-topics --bootstrap-server $KAFKA_BROKER --list"
echo -e "\nOr visit the Kafka UI at http://localhost:8080" 