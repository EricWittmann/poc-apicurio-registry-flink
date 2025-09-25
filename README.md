# Flink Catalog Test Application

A standalone Apache Flink application designed for testing and developing custom Flink catalogs. This project provides a foundation for building and testing your own catalog implementations.

## Features

- Complete Flink streaming application with Table API/SQL support
- Sample custom catalog implementation (`TestCatalog`)
- Kafka source and sink integration
- Representative SQL queries and data processing
- Test data generation utilities
- Maven build configuration with all necessary dependencies

## Prerequisites

### Required
- Java 11 or higher
- Maven 3.6+
- Apache Kafka (for external data sources)

### Kafka Setup
This application expects Kafka to be running at `localhost:9092`. Use the provided Docker setup:

```bash
# Start Kafka cluster
./scripts/start-kafka.sh

# Stop Kafka cluster
./scripts/stop-kafka.sh
```

Alternatively, you can run Kafka locally or modify connection settings in `CatalogTestApplication.java`.

## Quick Start

### 1. Build the Application
```bash
mvn clean package
```

### 2. Start Kafka Cluster
```bash
./scripts/start-kafka.sh
```

### 3. (Optional) Load Sample Data
```bash
./scripts/send-test-data.sh
```

### 4. Run the Application
```bash
java -jar target/flink-catalog-test-1.0-SNAPSHOT.jar
```

## Project Structure

```
src/
├── main/java/com/example/flink/
│   ├── CatalogTestApplication.java     # Main application entry point
│   ├── catalog/
│   │   └── TestCatalog.java           # Sample catalog implementation
│   ├── source/
│   │   └── TestDataGenerator.java     # Test data source
│   └── sink/
│       └── LoggingSink.java           # Simple logging sink
├── main/resources/
│   └── log4j2.properties              # Logging configuration
└── test/java/                         # Test classes (empty, ready for your tests)

scripts/
├── start-kafka.sh                     # Start Kafka cluster with Docker
├── stop-kafka.sh                      # Stop Kafka cluster
├── send-test-data.sh                  # Send sample data to Kafka
├── docker-compose-kafka.yml           # Docker Compose configuration
├── kafka-setup.sh                     # Legacy Kafka setup script
└── sample-data.json                   # Sample test data
```

## What the Application Does

1. **Catalog Registration**: Registers a custom `TestCatalog` with Flink
2. **Table Creation**: Creates Kafka source and sink tables using DDL
3. **Data Processing**: Runs a streaming query that:
   - Reads from `kafka_source` table
   - Applies transformations (age categorization)
   - Writes results to `kafka_sink` table
4. **Catalog Operations**: Demonstrates table listing and schema inspection

## Customizing for Your Catalog

### 1. Replace TestCatalog
The `TestCatalog` class in `src/main/java/com/example/flink/catalog/` is a minimal in-memory implementation. Replace it with your custom catalog:

```java
// In CatalogTestApplication.java
YourCustomCatalog customCatalog = new YourCustomCatalog("your_catalog", "your_database");
tableEnv.registerCatalog("your_catalog", customCatalog);
tableEnv.useCatalog("your_catalog");
```

### 2. Add Your Dependencies
Update `pom.xml` to include dependencies for your storage system:

```xml
<dependency>
    <groupId>your.storage.system</groupId>
    <artifactId>client</artifactId>
    <version>x.y.z</version>
</dependency>
```

### 3. Modify Test Queries
Update the SQL queries in `CatalogTestApplication.java` to test your specific use cases.

## Testing Your Catalog

The application provides several ways to test catalog functionality:

1. **Table Operations**: Create, list, describe tables
2. **Metadata Queries**: `SHOW TABLES`, `DESCRIBE table_name`
3. **Data Processing**: End-to-end streaming with your catalog tables
4. **Error Handling**: Test error scenarios (table not found, etc.)

## Monitoring Output

### Console Output
The application logs catalog operations and processing results to the console.

### Kafka Consumer
Monitor processed results:
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-output --from-beginning
```

## Common Issues

### Kafka Connection Errors
- Ensure Kafka is running on `localhost:9092`
- Check that topics exist: `kafka-topics.sh --list --bootstrap-server localhost:9092`

### ClassPath Issues
- Ensure all dependencies are included in the shaded JAR
- Check for version conflicts in `mvn dependency:tree`

### Catalog Errors
- Verify your catalog implementation handles all required methods
- Check database and table existence before operations

## Development Tips

1. **Incremental Testing**: Start with basic catalog operations before adding complex queries
2. **Logging**: Use the provided logging configuration to debug catalog operations
3. **Error Handling**: Implement proper exception handling in your catalog
4. **Performance**: Consider caching and connection pooling for external storage systems

## Next Steps

- Implement your custom catalog by extending or replacing `TestCatalog`
- Add unit tests in `src/test/java/`
- Configure CI/CD for automated testing
- Add more complex SQL scenarios specific to your use case

## License

This project is provided as-is for educational and development purposes.