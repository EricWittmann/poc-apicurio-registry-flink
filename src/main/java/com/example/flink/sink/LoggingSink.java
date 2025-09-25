package com.example.flink.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingSink implements SinkFunction<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingSink.class);

    @Override
    public void invoke(Row value, Context context) {
        LOG.info("Processed record: {}", value);
    }
}