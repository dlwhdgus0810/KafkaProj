package com.practice.kafka.event;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class FileEventHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(FileEventHandler.class.getName());

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topicName;
    private final boolean sync;

    public FileEventHandler(KafkaProducer<String, String> kafkaProducer, String topicName, boolean sync) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
        this.sync = sync;
    }

    @Override
    public void onMessage(MessageEvent messageEvent) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, messageEvent.key, messageEvent.value);

        if (sync) {
            RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
            logMetadata(metadata);
        } else {
            kafkaProducer.send(producerRecord, ((metadata, exception) -> {
                if (exception == null) {
                    logMetadata(metadata);
                } else {
                    log.error("exception error from broker: {}", exception.getMessage());
                }
            }));
        }
    }

    private static void logMetadata(RecordMetadata metadata) {
        log.info("[record metadata received] partition: {}, offset: {}, timestamp: {}",
                metadata.partition(), metadata.offset(), metadata.timestamp());
    }

    public static void main(String[] args) throws Exception {
        String topicName = "file-topic";

        KafkaProducer<String, String> kafkaProducer = createProducer();
        boolean sync = true;

        FileEventHandler fileEventHandler = new FileEventHandler(kafkaProducer, topicName, sync);
        MessageEvent messageEvent = new MessageEvent("key001", "first test message");
        fileEventHandler.onMessage(messageEvent);
    }

    private static KafkaProducer<String, String> createProducer() {
        // KafkaProducer config setting
        Properties props = new Properties();

        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer object creation
        return new KafkaProducer<>(props);
    }
}
