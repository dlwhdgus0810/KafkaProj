package com.example.util;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyLogger {

    private static Logger log;

    public MyLogger() {
        log = LoggerFactory.getLogger("Logger");
    }

    public <T> MyLogger(Class<T> clazz) {
        log = LoggerFactory.getLogger(clazz.getName());
    }

    public void info(RecordMetadata metadata) {
        log.info("\n====== record metadata received ======\n" +
                        "- partition: {}\n" +
                        "- offset: {}\n" +
                        "- timestamp: {}\n" +
                        "====== record metadata returned ======",
                metadata.partition(), metadata.offset(), metadata.timestamp());
    }

    public void error(String message, Exception e) {
        log.error("{}, message: {}", message, e.getMessage());
    }
}
