package com.example.flink.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

import java.time.Instant;
import java.util.Random;

public class TestDataGenerator implements SourceFunction<Row> {

    private volatile boolean isRunning = true;
    private final Random random = new Random();
    private final String[] names = {"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"};

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        long id = 1;

        while (isRunning) {
            Row row = Row.of(
                id++,
                names[random.nextInt(names.length)],
                random.nextInt(80) + 18, // Age between 18 and 97
                Instant.now()
            );

            ctx.collect(row);
            Thread.sleep(1000); // Generate one record per second
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}