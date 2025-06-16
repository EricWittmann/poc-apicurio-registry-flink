# Apicurio Registry Flink Catalog POC

This is a proof of concept for implementing Apicurio Registry as a Flink Catalog. The project starts with a recommendation application that uses Flink SQL and requires a catalog to operate.

## Prerequisites

- Java 17 or later
- Maven 3.6 or later
- Docker and Docker Compose (for running Kafka)
- Apache Flink (for testing)

## Project Structure

- `src/main/java/io/apicurio/registry/flink/RecommendationApp.java` - Main application class
- `src/main/resources/product_inventory.csv` - Sample product inventory data
- `pom.xml` - Maven project configuration
- `docker-compose.yml` - Docker Compose configuration for Kafka
- `scripts/create-kafka-topics.sh` - Script to create required Kafka topics

## Current Implementation

The current implementation uses Flink's `GenericInMemoryCatalog` as a placeholder for the future Apicurio Registry Catalog implementation. The application:

1. Creates and registers an in-memory catalog
2. Creates a database in the catalog
3. Creates tables for:
   - Product inventory (CSV file)
   - Click stream (Kafka topic)
   - Sales records (Kafka topic)
   - Recommended products (Kafka topic)
4. Executes a recommendation query that:
   - Joins click stream data with product inventory
   - Considers user purchase history
   - Recommends up to 6 products per user based on category, stock, and rating

## Running the Application

1. Start Kafka using Docker Compose:
   ```bash
   docker-compose up -d
   ```
   This will start:
   - Zookeeper (port 2181)
   - Kafka (port 9092)
   - Apicurio Registry (port 8080)

2. Create the required Kafka topics using the provided script:
   ```bash
   ./scripts/create-kafka-topics.sh
   ```
   This script will:
   - Check if Kafka is running
   - Create any missing topics
   - Skip topics that already exist
   - Provide colored output for easy status checking

   Alternatively, you can create topics manually using:
   ```bash
   docker exec kafka kafka-topics --create --topic flink.click.stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   docker exec kafka kafka-topics --create --topic flink.sales.records --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   docker exec kafka kafka-topics --create --topic flink.recommended.products --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

3. Run the application using one of these methods:

   a. Using the packaged JAR:
   ```bash
   mvn clean package
   java -jar target/poc-apicurio-registry-flink-1.0-SNAPSHOT.jar
   ```

4. To stop Kafka when done:
   ```bash
   docker-compose down
   ```

## Next Steps

1. Implement a custom Flink Catalog using Apicurio Registry
2. Modify the application to use the Apicurio Registry Catalog instead of the in-memory catalog
3. Add schema registry integration for Kafka topics
4. Add proper error handling and configuration management

## Notes

- The current implementation uses JSON format for Kafka topics. In the future, we'll integrate with Apicurio Registry for schema management.
- The in-memory catalog is used as a placeholder. The goal is to replace it with an Apicurio Registry-based catalog.
- The application currently requires a catalog to operate, which will be useful for testing the Apicurio Registry Catalog implementation.
- Kafka UI is included for easier topic management and monitoring during development.
- The `create-kafka-topics.sh` script provides a convenient way to ensure all required topics exist. 
