#!/bin/bash

# Script to run the Flink Catalog Test Application and provide a useful summary

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_FILE="$PROJECT_DIR/test-run.log"
SUMMARY_FILE="$PROJECT_DIR/test-summary.txt"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Starting Flink Catalog Test Application${NC}"
echo "=================================================="

# Check if Kafka is running
echo -e "${YELLOW}📋 Checking prerequisites...${NC}"
if ! docker ps | grep -q kafka-broker; then
    echo -e "${RED}❌ Kafka broker is not running. Please start it with: ./scripts/start-kafka.sh${NC}"
    exit 1
fi

if ! docker ps | grep -q zookeeper; then
    echo -e "${RED}❌ Zookeeper is not running. Please start it with: ./scripts/start-kafka.sh${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Kafka cluster is running${NC}"

# Check if application JAR exists
JAR_FILE="$PROJECT_DIR/target/flink-catalog-test-1.0-SNAPSHOT.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo -e "${YELLOW}📦 Building application...${NC}"
    cd "$PROJECT_DIR"
    mvn clean package -q
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Build failed${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ Application built successfully${NC}"
fi

# Run the application and capture output
echo -e "${YELLOW}🔄 Running Flink Catalog Test Application...${NC}"
echo "Log file: $LOG_FILE"
echo ""

START_TIME=$(date +%s)

# Run application and capture output
java -jar "$JAR_FILE" > "$LOG_FILE" 2>&1 &
APP_PID=$!

# Wait for application to finish
wait $APP_PID
APP_EXIT_CODE=$?

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo -e "${BLUE}📊 Test Execution Summary${NC}"
echo "=================================================="

# Initialize summary
> "$SUMMARY_FILE"

echo "Flink Catalog Test Application - Execution Summary" >> "$SUMMARY_FILE"
echo "Generated: $(date)" >> "$SUMMARY_FILE"
echo "Duration: ${DURATION}s" >> "$SUMMARY_FILE"
echo "Exit Code: $APP_EXIT_CODE" >> "$SUMMARY_FILE"
echo "" >> "$SUMMARY_FILE"

# Analyze the log file
if [ -f "$LOG_FILE" ]; then
    set +e

    # Check for successful operations
    CATALOG_REGISTERED=$(grep -a -c "Registered and using test catalog" "$LOG_FILE")
    TABLES_CREATED=$(grep -a -c "Created.*table" "$LOG_FILE")
    TABLES_LISTED=$(grep -a -c "table name" "$LOG_FILE")
    SCHEMA_DESCRIBED=$(grep -a -c "Describing.*table" "$LOG_FILE")
    JOB_STARTED=$(grep -a -c "Starting execution of job" "$LOG_FILE")

    # Check for errors
    DESERIALIZE_ERRORS=$(grep -a -c "Failed to deserialize" "$LOG_FILE")
    JSON_ERRORS=$(grep -a -c "JsonParseException" "$LOG_FILE")
    KAFKA_ERRORS=$(grep -a -c -E "KafkaException|Failed to connect" "$LOG_FILE")
    GENERAL_ERRORS=$(grep -a -c -E "ERROR|Exception" "$LOG_FILE")

    # Check for warnings
    WARNINGS=$(grep -a -c "WARN" "$LOG_FILE")

    # Performance metrics
    TOTAL_LOG_LINES=$(wc -l < "$LOG_FILE")

    set -e

    # Generate summary
    echo "CATALOG OPERATIONS:" >> "$SUMMARY_FILE"
    echo "  ✓ Catalog Registered: $CATALOG_REGISTERED" >> "$SUMMARY_FILE"
    echo "  ✓ Tables Created: $TABLES_CREATED" >> "$SUMMARY_FILE"
    echo "  ✓ Tables Listed: $TABLES_LISTED" >> "$SUMMARY_FILE"
    echo "  ✓ Schema Described: $SCHEMA_DESCRIBED" >> "$SUMMARY_FILE"
    echo "" >> "$SUMMARY_FILE"

    echo "STREAM PROCESSING:" >> "$SUMMARY_FILE"
    echo "  ✓ Jobs Started: $JOB_STARTED" >> "$SUMMARY_FILE"
    echo "" >> "$SUMMARY_FILE"

    echo "ERROR ANALYSIS:" >> "$SUMMARY_FILE"
    echo "  ❌ Deserialization Errors: $DESERIALIZE_ERRORS" >> "$SUMMARY_FILE"
    echo "  ❌ JSON Parse Errors: $JSON_ERRORS" >> "$SUMMARY_FILE"
    echo "  ❌ Kafka Errors: $KAFKA_ERRORS" >> "$SUMMARY_FILE"
    echo "  ❌ Total Errors: $GENERAL_ERRORS" >> "$SUMMARY_FILE"
    echo "    ️Warnings: $WARNINGS" >> "$SUMMARY_FILE"
    echo "" >> "$SUMMARY_FILE"

    echo "PERFORMANCE:" >> "$SUMMARY_FILE"
    echo "  Total Log Lines: $TOTAL_LOG_LINES" >> "$SUMMARY_FILE"
    echo "  Execution Time: ${DURATION}s" >> "$SUMMARY_FILE"
    echo "" >> "$SUMMARY_FILE"

    # Determine overall status
    if [ $APP_EXIT_CODE -eq 0 ] && [ $DESERIALIZE_ERRORS -eq 0 ] && [ $JSON_ERRORS -eq 0 ]; then
        OVERALL_STATUS="SUCCESS"
        STATUS_COLOR=$GREEN
        STATUS_EMOJI="✅"
    elif [ $DESERIALIZE_ERRORS -gt 0 ] || [ $JSON_ERRORS -gt 0 ]; then
        OVERALL_STATUS="FAILED - Data Processing Issues"
        STATUS_COLOR=$RED
        STATUS_EMOJI="❌"
    elif [ $APP_EXIT_CODE -ne 0 ]; then
        OVERALL_STATUS="FAILED - Application Error"
        STATUS_COLOR=$RED
        STATUS_EMOJI="❌"
    else
        OVERALL_STATUS="PARTIAL SUCCESS"
        STATUS_COLOR=$YELLOW
        STATUS_EMOJI="⚠️"
    fi

    echo "OVERALL STATUS: $OVERALL_STATUS" >> "$SUMMARY_FILE"

    # Display summary to console
    echo -e "${STATUS_COLOR}${STATUS_EMOJI} Overall Status: $OVERALL_STATUS${NC}"
    echo ""
    echo -e "${BLUE}Key Metrics:${NC}"
    echo -e "  Catalog Operations: ${GREEN}$CATALOG_REGISTERED${NC} registered, ${GREEN}$TABLES_CREATED${NC} tables created"
    echo -e "  Stream Processing: ${GREEN}$JOB_STARTED${NC} jobs started"
    echo -e "  Errors: ${RED}$GENERAL_ERRORS${NC} total, ${RED}$DESERIALIZE_ERRORS${NC} deserialization"
    echo -e "  Execution Time: ${YELLOW}${DURATION}s${NC}"

    # Show critical errors if any
    if [ $DESERIALIZE_ERRORS -gt 0 ]; then
        echo ""
        echo -e "${RED}🚨 Critical Issues Found:${NC}"
        grep -A 3 -B 1 "Failed to deserialize" "$LOG_FILE" | head -10
    fi

    if [ $JSON_ERRORS -gt 0 ]; then
        echo ""
        echo -e "${RED}🚨 JSON Parsing Issues:${NC}"
        grep -A 2 "JsonParseException" "$LOG_FILE" | head -5
    fi

else
    echo -e "${RED}❌ Log file not found: $LOG_FILE${NC}"
    echo "ERROR: No log file generated" >> "$SUMMARY_FILE"
fi

echo ""
echo -e "${BLUE}📄 Full Reports:${NC}"
echo -e "  📋 Summary: ${YELLOW}$SUMMARY_FILE${NC}"
echo -e "  📝 Full Log: ${YELLOW}$LOG_FILE${NC}"

echo ""
echo -e "${BLUE}💡 Quick Commands:${NC}"
echo -e "  View summary: ${YELLOW}cat $SUMMARY_FILE${NC}"
echo -e "  View errors: ${YELLOW}grep -i error $LOG_FILE${NC}"
echo -e "  View catalog ops: ${YELLOW}grep -i catalog $LOG_FILE${NC}"
echo -e "  View table ops: ${YELLOW}grep -i 'table\\|CREATE\\|SHOW\\|DESCRIBE' $LOG_FILE${NC}"

echo ""
echo "=================================================="
echo -e "${GREEN}🏁 Test execution completed${NC}"

# Display summary file content
if [ -f "$SUMMARY_FILE" ]; then
    echo ""
    echo -e "${BLUE}📊 Complete Summary:${NC}"
    echo "--------------------------------------------------"
    cat "$SUMMARY_FILE"
fi
