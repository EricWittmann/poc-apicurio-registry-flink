package com.example.flink;

import com.example.flink.catalog.TestCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogTestApplication {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogTestApplication.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink Catalog Test Application");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        TestCatalog testCatalog = new TestCatalog("test_catalog", "test_database");
        tableEnv.registerCatalog("test_catalog", testCatalog);
        tableEnv.useCatalog("test_catalog");

        LOG.info("Registered and using test catalog");

        createKafkaSourceTable(tableEnv);
        createKafkaSinkTable(tableEnv);
        createInMemoryTable(tableEnv);

        LOG.info("Executing Flink job...");
        runTestQueries(tableEnv);
    }

    private static void createKafkaSourceTable(StreamTableEnvironment tableEnv) {
        String createSourceTableSql =
            "CREATE TABLE kafka_source (" +
            "  id BIGINT," +
            "  name STRING," +
            "  age INT," +
            "  event_time_str STRING," +
            "  event_time AS TO_TIMESTAMP(event_time_str, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''')," +
            "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = 'test-input'," +
            "  'properties.bootstrap.servers' = 'localhost:9092'," +
            "  'properties.group.id' = 'flink-catalog-test'," +
            "  'scan.startup.mode' = 'earliest-offset'," +
            "  'format' = 'json'," +
            "  'json.fail-on-missing-field' = 'false'," +
            "  'json.ignore-parse-errors' = 'true'" +
            ")";

        tableEnv.executeSql(createSourceTableSql);
        LOG.info("Created Kafka source table");
    }

    private static void createKafkaSinkTable(StreamTableEnvironment tableEnv) {
        String createSinkTableSql =
            "CREATE TABLE kafka_sink (" +
            "  id BIGINT," +
            "  name STRING," +
            "  age INT," +
            "  age_category STRING," +
            "  processing_time TIMESTAMP(3)" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = 'test-output'," +
            "  'properties.bootstrap.servers' = 'localhost:9092'," +
            "  'format' = 'json'" +
            ")";

        tableEnv.executeSql(createSinkTableSql);
        LOG.info("Created Kafka sink table");
    }

    private static void createInMemoryTable(StreamTableEnvironment tableEnv) {
        String createMemoryTableSql =
            "CREATE TABLE memory_lookup (" +
            "  age_min INT," +
            "  age_max INT," +
            "  category STRING" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'fields.age_min.kind' = 'sequence'," +
            "  'fields.age_min.start' = '0'," +
            "  'fields.age_min.end' = '100'," +
            "  'fields.age_max.kind' = 'sequence'," +
            "  'fields.age_max.start' = '20'," +
            "  'fields.age_max.end' = '120'," +
            "  'fields.category.length' = '10'," +
            "  'number-of-rows' = '5'" +
            ")";

        tableEnv.executeSql(createMemoryTableSql);
        LOG.info("Created in-memory lookup table");
    }

    private static void runTestQueries(StreamTableEnvironment tableEnv) {
        LOG.info("Listing all tables in catalog:");
        tableEnv.executeSql("SHOW TABLES").print();

        LOG.info("Describing kafka_source table:");
        tableEnv.executeSql("DESCRIBE kafka_source").print();

        String processingQuery =
            "INSERT INTO kafka_sink " +
            "SELECT " +
            "  id, " +
            "  name, " +
            "  age, " +
            "  CASE " +
            "    WHEN age < 18 THEN 'Minor' " +
            "    WHEN age < 65 THEN 'Adult' " +
            "    ELSE 'Senior' " +
            "  END as age_category, " +
            "  CURRENT_TIMESTAMP as processing_time " +
            "FROM kafka_source";

        LOG.info("Starting stream processing query");
        try {
            var result = tableEnv.executeSql(processingQuery);
            LOG.info("Stream processing job submitted successfully");

            // Let the job run for a few seconds to process data, then stop
            Thread.sleep(10000); // 10 seconds

            LOG.info("Stopping stream processing job after 10 seconds");
            result.getJobClient().ifPresent(jobClient -> {
                try {
                    jobClient.cancel();
                    LOG.info("Job cancelled successfully");
                } catch (Exception e) {
                    LOG.warn("Failed to cancel job gracefully", e);
                }
            });

        } catch (Exception e) {
            LOG.error("Error executing stream processing query", e);
            throw new RuntimeException(e);
        }
    }
}