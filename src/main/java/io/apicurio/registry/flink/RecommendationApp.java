package io.apicurio.registry.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecommendationApp {
    private static final Logger LOG = LoggerFactory.getLogger(RecommendationApp.class);

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Set up the table environment with catalog support
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Create and register the in-memory catalog
        String catalogName = "recommendation_catalog";
        Catalog catalog = new GenericInMemoryCatalog(catalogName);
        tableEnv.registerCatalog(catalogName, catalog);
        
        // Set the current catalog
        tableEnv.useCatalog(catalogName);
        
        // Create the default database in the catalog
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS recommendation_db");
        tableEnv.useDatabase("recommendation_db");

        // Create the product inventory table
        tableEnv.executeSql("""
            CREATE TABLE ProductInventoryTable (
                product_id STRING, 
                category STRING, 
                stock STRING, 
                rating STRING 
            ) WITH (
                'connector' = 'filesystem', 
                'path' = './src/main/resources/product_inventory.csv', 
                'format' = 'csv', 
                'csv.ignore-parse-errors' = 'true' 
            ); 
        """);

        // Create the click stream table
        tableEnv.executeSql("""
            CREATE TABLE ClickStreamTable (
                user_id STRING, 
                product_id STRING, 
                `event_time` TIMESTAMP(3) METADATA FROM 'timestamp', 
                WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND 
            ) WITH ( 
                'connector' = 'kafka', 
                'topic' = 'flink.click.streams', 
                'properties.bootstrap.servers' = 'localhost:9092', 
                'properties.group.id' = 'click-stream-group', 
                'value.format' = 'avro-confluent', 
                'value.avro-confluent.url' = 'http://localhost:8080/apis/ccompat/v6', 
                'scan.startup.mode' = 'latest-offset' 
            );
        """);

        // Create the sales records table
        tableEnv.executeSql("""
            CREATE TABLE SalesRecordTable (
                invoice_id STRING, 
                user_id STRING, 
                product_id STRING, 
                quantity STRING, 
                unit_cost STRING, 
                `purchase_time` TIMESTAMP(3) METADATA FROM 'timestamp', 
                WATERMARK FOR purchase_time AS purchase_time - INTERVAL '1' SECOND 
            ) WITH (
                'connector' = 'kafka', 
                'topic' = 'flink.sales.records', 
                'properties.bootstrap.servers' = 'localhost:9092',
                'properties.group.id' = 'sales-record-group', 
                'value.format' = 'avro-confluent', 
                'value.avro-confluent.url' = 'http://localhost:8080/apis/ccompat/v6', 
                'scan.startup.mode' = 'latest-offset' 
            ); 
        """);

        // Create the CSV sink table
        tableEnv.executeSql("""
            CREATE TABLE CsvSinkTable ( 
                user_id STRING, 
                top_product_ids STRING, 
                `event_time` TIMESTAMP(3), 
                PRIMARY KEY(`user_id`) NOT ENFORCED 
            ) WITH ( 
                'connector' = 'upsert-kafka', 
                'topic' = 'flink.recommended.products', 
                'properties.bootstrap.servers' = 'localhost:9092', 
                'properties.client.id' = 'recommended-products-producer-client', 
                'properties.transaction.timeout.ms' = '800000', 
                'key.format' = 'csv', 
                'value.format' = 'csv', 
                'value.fields-include' = 'ALL' 
            );
        """);

        // Create the clicked_products temporary view
        tableEnv.executeSql("""
            CREATE TEMPORARY VIEW clicked_products AS 
                SELECT DISTINCT 
                    c.user_id, c.event_time, 
                    p.product_id, 
                    p.category 
                FROM ClickStreamTable AS c 
                JOIN ProductInventoryTable AS p ON c.product_id = p.product_id;         
        """);

        // Create the category_products temporary view
        tableEnv.executeSql("""
            CREATE TEMPORARY VIEW category_products AS 
                SELECT 
                    cp.user_id, 
                    cp.event_time, 
                    p.product_id, 
                    p.category, 
                    p.stock, 
                    p.rating, 
                    sr.user_id as purchased 
                FROM clicked_products cp 
                JOIN ProductInventoryTable AS p ON cp.category = p.category 
                LEFT JOIN SalesRecordTable sr ON cp.user_id = sr.user_id AND p.product_id = sr.product_id 
                WHERE p.stock > 0 
                GROUP BY 
                    p.product_id, 
                    p.category, 
                    p.stock, 
                    cp.user_id, 
                    cp.event_time, 
                    sr.user_id, 
                    p.rating
                ;
        """);

        // Create the top_products temporary view
        tableEnv.executeSql("""
            CREATE TEMPORARY VIEW top_products AS 
                SELECT cp.user_id, 
                    cp.event_time, 
                    cp.product_id, 
                    cp.category, 
                    cp.stock, 
                    cp.rating, 
                    cp.purchased, 
                    ROW_NUMBER() OVER (PARTITION BY cp.user_id ORDER BY cp.purchased DESC, cp.rating DESC) AS rn 
                FROM category_products cp;
        """);

        // Create the recommendation query  
        String recommendationQuery = """
            SELECT 
                user_id, 
                LISTAGG(product_id, ',') AS top_product_ids, 
                TUMBLE_END(event_time, INTERVAL '5' SECOND) as window_end_timestamp
            FROM top_products 
            WHERE rn <= 6 
            GROUP BY user_id, 
            TUMBLE(event_time, INTERVAL '5' SECOND)
            ;
        """;

        // Use recommendation query to insert into the CSV sink table
        tableEnv.executeSql("INSERT INTO CsvSinkTable " + recommendationQuery);
    }
} 